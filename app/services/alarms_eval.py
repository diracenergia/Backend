# app/services/alarms_eval.py
from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional, Tuple

import os
import logging
from decimal import Decimal
from psycopg.types.json import Json  # psycopg v3

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="ts=%(asctime)s level=%(levelname)s module=%(name)s msg=%(message)s",
)
log = logging.getLogger("alarms-eval")

# -----------------------------------------------------------------------------
# Repos y publishers
# -----------------------------------------------------------------------------
from app.repos import tanks as tanks_repo
from app.repos import alarms as alarms_repo
from app.repos import audit as audit_repo  # opcional

from app.services.alarm_events import publish_raised, publish_cleared

# -----------------------------------------------------------------------------
# JSON helpers (inlined)
# -----------------------------------------------------------------------------
from datetime import date
def _to_jsonable(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat().replace("+00:00", "Z")
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_to_jsonable(v) for v in obj]
    return obj

# -----------------------------------------------------------------------------
# Config / Mapping
# -----------------------------------------------------------------------------
_THRESHOLD_MAP = {
    "low_low":   ("LOW_LOW",   "critical"),
    "low":       ("LOW",       "warning"),
    "high":      ("HIGH",      "warning"),
    "high_high": ("HIGH_HIGH", "critical"),
}
_THRESH_ALIAS = {
    "LOW_LOW":  "very_low",
    "LOW":      "low",
    "HIGH":     "high",
    "HIGH_HIGH":"very_high",
}

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _coerce_cfg_to_float(cfg: dict) -> dict:
    out = {}
    for k in ("low_low_pct", "low_pct", "high_pct", "high_high_pct"):
        v = cfg[k]
        out[k] = float(v) if isinstance(v, Decimal) else float(v)
    return out

def _decide_state(level_pct: float, cfg: dict) -> Optional[Tuple[str, str, str]]:
    log.debug("decide_state start level_pct=%.3f cfg_keys=%s", level_pct, list(cfg.keys()))
    for k in ("low_low_pct", "low_pct", "high_pct", "high_high_pct"):
        if k not in cfg:
            log.error("cfg_missing key=%s", k)
            raise ValueError(f"cfg incompleto: falta {k}")

    if level_pct <= cfg["low_low_pct"]:
        code, sev_db = _THRESHOLD_MAP["low_low"];  log.debug("state=low_low code=%s sev=%s", code, sev_db);  return (code, sev_db, "low_low")
    if level_pct <= cfg["low_pct"]:
        code, sev_db = _THRESHOLD_MAP["low"];      log.debug("state=low code=%s sev=%s", code, sev_db);      return (code, sev_db, "low")
    if level_pct >= cfg["high_high_pct"]:
        code, sev_db = _THRESHOLD_MAP["high_high"];log.debug("state=high_high code=%s sev=%s", code, sev_db); return (code, sev_db, "high_high")
    if level_pct >= cfg["high_pct"]:
        code, sev_db = _THRESHOLD_MAP["high"];     log.debug("state=high code=%s sev=%s", code, sev_db);     return (code, sev_db, "high")
    log.debug("state=normal"); return None

def _clear_one(alarm_id: int, *, asset_type: str, asset_id: int, code: str,
               severity_db: str, message: str, value: float) -> None:
    log.info("clear_one start alarm_id=%s asset_type=%s asset_id=%s code=%s value=%.3f",
             alarm_id, asset_type, asset_id, code, value)
    try:
        cleared = alarms_repo.clear(alarm_id, ts_cleared=_utcnow())
        log.debug("clear_one repo.clear cleared=%s", bool(cleared))
    except Exception as e:
        log.exception("clear_one repo.clear error err=%s alarm_id=%s", e, alarm_id);  return
    if not cleared:
        log.info("clear_one skip reason=already_cleared alarm_id=%s", alarm_id);  return

    ts = _iso(_utcnow())
    try:
        publish_cleared(asset_type=asset_type, asset_id=asset_id, code=code,
                        message=message or "", severity=severity_db, value=value,
                        threshold=None, ts_cleared=ts)
        log.info("clear_one published op=CLEARED asset_type=%s asset_id=%s code=%s ts=%s",
                 asset_type, asset_id, code, ts)
    except Exception as e:
        log.exception("clear_one publish error err=%s code=%s", e, code)

def _clear_all_for_tank(tank_id: int, *, value: float) -> None:
    from psycopg.rows import dict_row
    from app.core.db import get_conn
    log.info("clear_all_for_tank start tank_id=%s value=%.3f", tank_id, value)
    sql = """
      SELECT id, code, severity, COALESCE(message,'') AS message
        FROM public.alarms
       WHERE asset_type='tank' AND asset_id=%s AND is_active=true
    """
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, (tank_id,))
            rows = cur.fetchall()
        log.debug("clear_all_for_tank fetched active_count=%d", len(rows))
    except Exception as e:
        log.exception("clear_all_for_tank fetch error err=%s tank_id=%s", e, tank_id);  return

    for row in rows:
        _clear_one(alarm_id=row["id"], asset_type="tank", asset_id=tank_id,
                   code=row["code"], severity_db=row["severity"],
                   message=row["message"], value=value)

# -----------------------------------------------------------------------------
# API principal
# -----------------------------------------------------------------------------
def eval_tank_alarm(tank_id: int, level_pct: Optional[float]) -> Optional[int]:
    log.info("eval start tank_id=%s level_pct=%s", tank_id, level_pct)
    if level_pct is None:
        log.warning("eval skip reason=level_none tank_id=%s", tank_id);  return None

    # 1) Config (coerción a float por si vienen Decimal desde DB)
    try:
        raw_cfg = tanks_repo.get_config_by_id(tank_id)
        cfg = _coerce_cfg_to_float(raw_cfg)
        log.debug("cfg loaded tank_id=%s low_low=%.3f low=%.3f high=%.3f high_high=%.3f",
                  tank_id, cfg["low_low_pct"], cfg["low_pct"], cfg["high_pct"], cfg["high_high_pct"])
    except Exception as e:
        log.exception("cfg error err=%s tank_id=%s", e, tank_id);  return None

    # 2) Estado
    try:
        state = _decide_state(float(level_pct), cfg)
    except Exception as e:
        log.exception("decide_state error err=%s tank_id=%s", e, tank_id);  return None

    if state is None:
        log.info("eval normal -> clear_all tank_id=%s level=%.3f", tank_id, level_pct)
        _clear_all_for_tank(tank_id, value=float(level_pct))
        return None

    alarm_code_upper, severity_db_lower, threshold_key = state
    threshold_alias = _THRESH_ALIAS.get(alarm_code_upper, threshold_key)
    log.info("eval out_of_range code=%s severity=%s alias=%s level=%.3f",
             alarm_code_upper, severity_db_lower, threshold_alias, level_pct)

    # 3) Dedupe
    try:
        active = alarms_repo.get_active(asset_type="tank", asset_id=tank_id, code=alarm_code_upper)
        log.debug("active_lookup exists=%s", bool(active))
    except Exception as e:
        log.exception("active_lookup error err=%s tank_id=%s code=%s", e, tank_id, alarm_code_upper)
        active = None
    if active:
        log.info("eval dedupe reason=already_active alarm_id=%s", getattr(active, "id", None))
        return active.id

    # 4) Insert DB
    message = f"Tank {tank_id} {alarm_code_upper}"
    extra_dict = {"value": float(level_pct), "threshold": threshold_alias}
    extra_jsonable = _to_jsonable(extra_dict)
    log.debug("db_create extra_jsonable=%s", extra_jsonable)

    try:
        created = alarms_repo.create(
            asset_type="tank",
            asset_id=tank_id,
            code=alarm_code_upper,
            severity=severity_db_lower,
            message=message,
            ts_raised=_utcnow(),
            is_active=True,
            extra=Json(extra_jsonable),  # <- Json, pero con datos sanitizados
        )
        alarm_id = created.id
        log.info("db_create ok alarm_id=%s tank_id=%s code=%s", alarm_id, tank_id, alarm_code_upper)
    except Exception as e:
        log.exception("db_create error err=%s tank_id=%s code=%s", e, tank_id, alarm_code_upper)
        return None

    # 5) Publicación
    ts = _iso(_utcnow())
    try:
        publish_raised(
            asset_type="tank",
            asset_id=tank_id,
            code=alarm_code_upper,
            message=message,
            severity=severity_db_lower,
            value=float(level_pct),
            threshold=threshold_alias,
            ts_raised=ts,
        )
        log.info("publish ok op=RAISED tank_id=%s code=%s ts=%s", tank_id, alarm_code_upper, ts)
    except Exception as e:
        log.exception("publish error err=%s op=RAISED tank_id=%s code=%s", e, tank_id, alarm_code_upper)

    # 6) tg_notified_at (best effort)
    try:
        from app.core.db import get_conn
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("UPDATE public.alarms SET tg_notified_at = now() WHERE id=%s", (alarm_id,))
            conn.commit()
        log.debug("tg_notified_at updated alarm_id=%s", alarm_id)
    except Exception as e:
        log.warning("tg_notified_at update skipped err=%s alarm_id=%s", e, alarm_id)

    return alarm_id
