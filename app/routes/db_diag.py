# app/routes/db_diag_full.py
from fastapi import APIRouter
import os, socket, time
import psycopg
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

# Usamos las mismas constantes/helpers que tu db.py
from app.core.db import PRIMARY_DSN, FALLBACK_DSN
try:
    # si existe en tu db.py, usamos la misma lógica de ipv4
    from app.core.db import _hostaddr_for
except Exception:
    _hostaddr_for = None  # fallback local si no está

router = APIRouter()

# ---------------------------
# Helpers
# ---------------------------
def _redact_dsn(dsn: str) -> str:
    """Oculta password en el DSN para log seguro."""
    if not dsn:
        return ""
    try:
        u = urlparse(dsn)
        # userinfo va en netloc: user:pass@host:port
        netloc = u.netloc
        if "@" in netloc and ":" in netloc.split("@")[0]:
            userinfo, rest = netloc.split("@", 1)
            user, _sep, _pwd = userinfo.partition(":")
            userinfo_red = f"{user}:[REDACTED]"
            netloc = f"{userinfo_red}@{rest}"
            u = u._replace(netloc=netloc)
        return urlunparse(u)
    except Exception:
        return "<unparseable>"

def _hostport(dsn: str):
    try:
        u = urlparse(dsn)
        return {"host": u.hostname, "port": u.port, "path": u.path}
    except Exception:
        return {"host": None, "port": None, "path": None}

def _resolve_dns(host: str):
    """Devuelve listas de A/AAAA (IPv4/IPv6) únicas."""
    v4, v6 = set(), set()
    try:
        for fam, _, _, _, sa in socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP):
            ip = sa[0]
            if fam == socket.AF_INET:
                v4.add(ip)
            elif fam == socket.AF_INET6:
                v6.add(ip)
    except Exception as e:
        return {"error": str(e), "ipv4": [], "ipv6": []}
    return {"ipv4": sorted(v4), "ipv6": sorted(v6)}

def _calc_hostaddr(dsn: str):
    """IPv4 preferida, usando la misma lógica que el core si está disponible."""
    if not dsn:
        return None
    if callable(_hostaddr_for):
        try:
            return _hostaddr_for(dsn)
        except Exception:
            pass
    # fallback local
    try:
        host = urlparse(dsn).hostname
        if not host:
            return None
        for fam, _, _, _, sa in socket.getaddrinfo(host, None, family=socket.AF_INET, proto=socket.IPPROTO_TCP):
            return sa[0]  # primera IPv4
    except Exception:
        return None

def _test_conn(dsn: str, hostaddr: str | None = None, timeout: int = 5):
    """Conecta y trae algunos datos; si hostaddr, forcemos IPv4."""
    if not dsn:
        return {"ok": False, "error": "empty DSN"}
    kw = {"connect_timeout": timeout}
    if hostaddr:
        kw["hostaddr"] = hostaddr
    t0 = time.perf_counter()
    try:
        with psycopg.connect(dsn, **kw) as conn:
            with conn.cursor() as cur:
                cur.execute("select inet_server_addr(), inet_server_port(), current_database(), current_user")
                a, p, db, usr = cur.fetchone()
                out = {
                    "ok": True,
                    "server_addr": str(a),
                    "server_port": p,
                    "db": db,
                    "user": usr,
                }
                # SSL (puede no estar permitido; lo envolvemos)
                try:
                    cur.execute("show ssl")
                    out["ssl"] = cur.fetchone()[0]
                except Exception as e2:
                    out["ssl"] = f"(unavailable: {type(e2).__name__})"
            out["duration_ms"] = round((time.perf_counter() - t0) * 1000, 1)
            if hostaddr:
                out["hostaddr_used"] = hostaddr
            return out
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "duration_ms": round((time.perf_counter() - t0) * 1000, 1),
            **({"hostaddr_used": hostaddr} if hostaddr else {}),
        }

def _pack_section(name: str, dsn: str):
    meta = _hostport(dsn)
    host = meta.get("host")
    dns = _resolve_dns(host) if host else {"ipv4": [], "ipv6": []}
    ha = _calc_hostaddr(dsn)
    timeout = int(os.getenv("DB_CONNECT_TIMEOUT", "5") or "5")

    return {
        "dsn_raw_redacted": _redact_dsn(dsn),
        "meta": meta,
        "dns": dns,
        "hostaddr_candidate": ha,
        "tests": {
            "plain": _test_conn(dsn, hostaddr=None, timeout=timeout),
            "ipv4": _test_conn(dsn, hostaddr=ha, timeout=timeout) if ha else {"ok": False, "error": "no ipv4 resolved"},
        },
    }

# ---------------------------
# Endpoint
# ---------------------------
@router.get("/__db_diag_full")
def db_diag_full():
    env_view = {
        "DB_URL": _redact_dsn(os.getenv("DB_URL", "")),
        "DATABASE_URL": _redact_dsn(os.getenv("DATABASE_URL", "")),
        "DB_URL_FALLBACK": _redact_dsn(os.getenv("DB_URL_FALLBACK", "")),
        "EVENTS_DB_URL": _redact_dsn(os.getenv("EVENTS_DB_URL", "")),
        "DB_CONNECT_TIMEOUT": os.getenv("DB_CONNECT_TIMEOUT"),
        "DB_FORCE_IPV4": os.getenv("DB_FORCE_IPV4"),
        "DEBUG_DB_DSN": os.getenv("DEBUG_DB_DSN"),
        "DEBUG_EVENTS_DSN": os.getenv("DEBUG_EVENTS_DSN"),
    }
    out = {
        "lib": {"psycopg": getattr(psycopg, "__version__", "unknown")},
        "env": env_view,
        "primary": _pack_section("primary", PRIMARY_DSN),
        "fallback": _pack_section("fallback", FALLBACK_DSN) if FALLBACK_DSN else {"dsn_raw_redacted": "", "meta": {}, "dns": {}, "hostaddr_candidate": None, "tests": {"plain": {"ok": False, "error": "empty DSN"}, "ipv4": {"ok": False, "error": "empty DSN"}}},
    }
    return out
