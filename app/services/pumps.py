from fastapi import HTTPException
from app.repos import pumps as repo
from app.repos import pump_commands as cmd_repo

def _to_bool_loose(v):
    if isinstance(v, bool) or v is None:
        return v
    s = str(v).strip().lower()
    if s in ("1","true","yes","on","auto","remoto"):
        return True
    if s in ("0","false","no","off","manual","local"):
        return False
    return None

def queue_command(pump_id: int, cmd: str, user: str, speed_pct: int | None):
    # 1) Config normalizada (vista)
    row = repo.get_normalized_pump_config(pump_id)
    if not row:
        raise HTTPException(404, "Bomba no encontrada")
    (_id, drive_type, remote_enabled, vfd_min, vfd_max, vfd_default) = row

    if not remote_enabled:
        raise HTTPException(403, detail={"message": "Comando remoto deshabilitado para esta bomba"})

    # 2) Última lectura para detectar MANUAL/lockout
    last = repo.get_last_pump_reading(pump_id)
    last_ts, mode, lock_col, raw = (last or (None, None, None, None))
    raw = raw or {}

    selector_norm = (str(mode or raw.get("selector") or raw.get("control_mode") or raw.get("modo") or "").strip().lower())
    remote_bool   = _to_bool_loose(raw.get("remote") or raw.get("remote_enabled") or raw.get("remoto"))
    manual_lockout = bool(lock_col) or selector_norm in {"manual","man","local","lockout","lock-out"}
    if remote_bool is False:
        manual_lockout = True

    if manual_lockout:
        raise HTTPException(
            403,
            detail={
                "message": "Selector en MANUAL: no se aceptan comandos remotos",
                "pump_id": pump_id,
                "latest_ts": last_ts,
                "selector_norm": selector_norm or None,
                "remote_bool": remote_bool,
            }
        )

    # 3) Validaciones SPEED
    payload: dict | None = None
    if cmd == "SPEED":
        if drive_type != "vfd":
            raise HTTPException(400, "La bomba no es VFD; no admite SPEED")
        sp = speed_pct if speed_pct is not None else vfd_default
        if sp is None:
            raise HTTPException(400, "speed_pct requerido (o default configurado) para SPEED")
        mn = vfd_min if vfd_min is not None else 0
        mx = vfd_max if vfd_max is not None else 100
        if not (mn <= sp <= mx):
            raise HTTPException(400, f"speed_pct fuera de rango permitido [{mn}..{mx}]")
        payload = {"speed_pct": int(sp)}

    # 4) Encolar
    return cmd_repo.enqueue_pump_command(pump_id, cmd, payload, user)

# Transiciones de estado (igual que tanques)
VALID = {
    "queued": {"sent", "expired", "failed"},
    "sent":   {"acked", "failed", "expired"},
    "acked":  set(),
    "failed": set(),
    "expired": set(),
}

def update_command_status(pump_id: int, cmd_id: int, new_status: str, error: str | None):
    prev = cmd_repo.get_command_status(cmd_id, pump_id)
    if not prev:
        raise HTTPException(404, "Comando no encontrado para esa bomba")
    if new_status not in VALID.get(prev, set()):
        raise HTTPException(409, f"Transición inválida {prev} → {new_status}")

    if new_status == "sent":
        return cmd_repo.mark_sent(cmd_id)
    elif new_status == "acked":
        return cmd_repo.mark_acked(cmd_id)
    else:
        return cmd_repo.mark_other(cmd_id, new_status, error)
