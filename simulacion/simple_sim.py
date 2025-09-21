# simple_sim.py
import os, time, random, requests, json
from dotenv import load_dotenv

# ================== ENV / Config ==================
# Elegí .env con DOTENV (default ".env")
dotenv_file = os.getenv("DOTENV", ".env")
load_dotenv(dotenv_file)

API        = os.getenv("API", "http://127.0.0.1:8000")
API_KEY    = os.getenv("API_KEY", "simulador123")
AUTH_TOKEN = os.getenv("AUTH_TOKEN", "")     # opcional: "Bearer <token>"
DEVICE_ID  = os.getenv("DEVICE_ID", "")      # opcional: ID lógico del device

TANK_ID = int(os.getenv("TANK_ID", "2"))
PUMP_ID = int(os.getenv("PUMP_ID", "3"))

# Accionamiento: direct|soft|vfd (acepto DRIVE_TYPE o DRIVE)
DRIVE_TYPE = (os.getenv("DRIVE_TYPE") or os.getenv("DRIVE") or "direct").lower()

# Periodicidades
PERIOD_SECONDS   = float(os.getenv("PERIOD_SECONDS", "5"))
POLL_CMD_SECONDS = float(os.getenv("POLL_CMD_SECONDS", "1.0"))

# Simular cortes (0 = no cortar)
DROP_EVERY_N_TANK = int(os.getenv("DROP_EVERY_N_TANK", "0"))
DROP_EVERY_N_PUMP = int(os.getenv("DROP_EVERY_N_PUMP", "0"))

# Balance del tanque
TANK_CAP_L        = float(os.getenv("TANK_CAP_L", "500"))
INLET_NOMINAL_LPM = float(os.getenv("INLET_NOMINAL_LPM", "50"))

# VFD (si DRIVE_TYPE = vfd)
VFD_MIN     = int(os.getenv("VFD_MIN", "0"))
VFD_MAX     = int(os.getenv("VFD_MAX", "100"))
VFD_DEFAULT = int(os.getenv("VFD_DEFAULT", str(min(max(50, VFD_MIN), VFD_MAX))))

S = requests.Session()
S.headers.update({"Content-Type": "application/json"})

def make_headers(extra: dict | None = None) -> dict:
    """Headers comunes para TODAS las requests según OpenAPI."""
    h = {"Content-Type": "application/json"}
    if API_KEY:
        h["X-Api-Key"] = str(API_KEY)
    if AUTH_TOKEN:
        h["Authorization"] = str(AUTH_TOKEN)  # ej: "Bearer <token>"
    if DEVICE_ID:
        h["X-Device-Id"] = str(DEVICE_ID)
    if extra:
        h.update(extra)
    return h

# ================== Estado simulado ==================
pump_on        = True
speed_pct      = VFD_DEFAULT if DRIVE_TYPE == "vfd" else 50
selector_mode  = "auto"       # auto | man
remote_enabled = True

valve_in_pct   = 100
valve_out_pct  = 100
leak_lpm       = 0.0
noise_amp      = 1.0
fault_mode     = "none"       # none|cavitacion|sensor_trabado|filtro_tapado|power_loss

level_percent  = random.uniform(60, 85)  # nivel inicial %
temperature_c  = 23.0

tick           = 0
_last_step_ts  = time.monotonic()
_last_meas     = {"flow_lpm": 0.0, "pressure_bar": 0.1}

def clamp(v, a, b): return max(a, min(b, v))

def http_post(path: str, payload: dict, headers: dict | None = None):
    try:
        h = make_headers(headers)
        r = S.post(f"{API}{path}", data=json.dumps(payload), headers=h, timeout=15)
        print(f"{path} -> {r.status_code} {r.text[:300]}")
    except Exception as e:
        print(f"{path} ERROR: {e}")

def http_get(path: str, params: dict | None = None):
    r = S.get(f"{API}{path}", params=params or {}, headers=make_headers(), timeout=15)
    r.raise_for_status()
    return r.json()

# ================== Física simplificada ==================
def inlet_flow_lpm():
    base = INLET_NOMINAL_LPM * (valve_in_pct / 100.0)
    return max(0.0, base + random.uniform(-1, 1) * noise_amp)

def pump_base_curve():
    """Curva base caudal/presión según tipo de drive/velocidad."""
    if not pump_on or fault_mode == "power_loss":
        return 0.0, 0.1

    if DRIVE_TYPE == "vfd":
        sp = clamp(speed_pct, VFD_MIN, VFD_MAX)
        base_flow = 100.0 * (sp / 100.0)  # 100 L/min @100%
        base_pres = 4.0   * (sp / 100.0)  # 4 bar @100%
    else:
        base_flow = random.uniform(60, 80)
        base_pres = random.uniform(2.0, 3.0)

    return base_flow, base_pres

def apply_hydraulics(flow, pres):
    # Restricción por válvula de descarga
    flow *= (valve_out_pct / 100.0)

    # Fallas
    if fault_mode == "filtro_tapado":
        flow *= 0.5
        pres *= 1.2
    elif fault_mode == "cavitacion":
        pres *= 0.6
        flow *= 0.8
        flow += random.uniform(-6, 6) * noise_amp
        pres += random.uniform(-0.4, 0.4) * noise_amp
    elif fault_mode == "sensor_trabado":
        pass  # se simula abajo en gen_pump

    # Ruido general
    flow = max(0.0, flow + random.uniform(-3, 3) * noise_amp)
    pres = max(0.1, pres + random.uniform(-0.2, 0.2) * noise_amp)
    return round(flow, 2), round(pres, 2)

def step(dt_s: float):
    """Integra estado del tanque según caudales [L/min]."""
    global level_percent, temperature_c

    inflow  = inlet_flow_lpm()
    base_f, base_p = pump_base_curve()
    pump_flow_lpm, pump_pres_bar = apply_hydraulics(base_f, base_p)

    total_out_lpm = pump_flow_lpm + max(0.0, leak_lpm)

    dV = (inflow - total_out_lpm) * (dt_s / 60.0)  # L
    level_l = (level_percent / 100.0) * TANK_CAP_L
    level_l = clamp(level_l + dV, 0.0, TANK_CAP_L)
    level_percent = (level_l / TANK_CAP_L) * 100.0

    temperature_c = clamp(temperature_c + random.uniform(-0.1, 0.1), 18.0, 32.0)

    _last_meas["flow_lpm"], _last_meas["pressure_bar"] = pump_flow_lpm, pump_pres_bar
    return pump_flow_lpm, pump_pres_bar, inflow

# ================== Lecturas (payloads según OpenAPI) ==================
def gen_tank():
    level = round(level_percent, 1)
    volume_l = round((level_percent / 100.0) * TANK_CAP_L, 1)
    return {
        "tank_id": TANK_ID,
        "level_percent": level,                 # requerido
        "volume_l": volume_l,                  # opcional
        "temperature_c": round(temperature_c, 1),  # opcional
        "raw_json": {                          # opcional; útil para trazas
            "source": "simple-sim",
            "valve_in_pct": round(valve_in_pct, 1),
            "leak_lpm": round(leak_lpm, 2),
            "noise_amp": round(noise_amp, 2),
        },
        # "ts": datetime.now(timezone.utc).isoformat()  # opcional
        # "device_id": DEVICE_ID                        # opcional (también va por header X-Device-Id)
    }

def gen_pump():
    flow, pres = _last_meas["flow_lpm"], _last_meas["pressure_bar"]
    current_a = 1.2 + (flow / 100.0) * 2.0 + random.uniform(-0.1, 0.1) * noise_amp
    return {
        "pump_id": PUMP_ID,                          # requerido
        "is_on": pump_on and fault_mode != "power_loss",
        "flow_lpm": round(flow, 2),                  # opcional
        "pressure_bar": round(pres, 2),              # opcional
        "voltage_v": 220.0 if fault_mode != "power_loss" else 0.0,
        "current_a": round(max(0.0, current_a), 2),
        "control_mode": "auto" if selector_mode == "auto" else "manual",
        "manual_lockout": not remote_enabled,
        "extra": {                                   # opcional
            "source": "simple-sim",
            "drive_type": DRIVE_TYPE,
            "speed_pct": clamp(speed_pct, VFD_MIN, VFD_MAX) if DRIVE_TYPE == "vfd" else speed_pct,
            "selector_mode": selector_mode,
            "remote": remote_enabled,
            "valve_out_pct": valve_out_pct,
            "fault_mode": fault_mode,
        },
        # "ts": datetime.now(timezone.utc).isoformat()  # opcional
    }

# ================== Comandos ==================
def poll_commands_for(kind: str, entity_id: int):
    """kind: 'pumps'|'tanks'"""
    try:
        cmds = http_get(f"/{kind}/{entity_id}/commands", params={"status": "queued"})
    except Exception as e:
        print(f"[cmd] poll {kind}/{entity_id} error: {e}")
        return []
    # ordenar por ts_created si viene
    cmds.sort(key=lambda c: c.get("ts_created", ""))
    return cmds

def set_cmd_status(kind, entity_id, cid, status, error=None):
    try:
        body = {"status": status}
        if error: body["error"] = error
        http_post(f"/{kind}/{entity_id}/commands/{cid}/status", body, headers=None)
    except Exception as e:
        print(f"[cmd] status {cid} -> {status} FAIL: {e}")

def guard_remote(payload):
    """Bloquea si selector está en MAN y remoto deshabilitado (salvo force)."""
    force = bool((payload or {}).get("force", False))
    if selector_mode == "man" and not remote_enabled and not force:
        raise RuntimeError("Selector en MAN y remoto deshabilitado")

def execute_command(cmd: str, payload: dict):
    global pump_on, speed_pct, DRIVE_TYPE, PERIOD_SECONDS, POLL_CMD_SECONDS
    global selector_mode, remote_enabled, valve_in_pct, valve_out_pct
    global leak_lpm, noise_amp, fault_mode, level_percent

    C = cmd.upper()

    if C in ("START", "STOP", "SPEED", "AUTO", "MAN"):
        if C in ("START", "STOP", "SPEED"):
            guard_remote(payload)
        if C == "START":
            pump_on = True
        elif C == "STOP":
            pump_on = False
        elif C == "AUTO":
            selector_mode = "auto"
        elif C == "MAN":
            selector_mode = "man"
        elif C == "SPEED":
            sp = payload.get("speed_pct")
            if sp is None:
                raise ValueError("SPEED requiere payload.speed_pct")
            sp = int(sp)
            if DRIVE_TYPE == "vfd":
                sp = int(clamp(sp, VFD_MIN, VFD_MAX))
            speed_pct = clamp(sp, 0, 100)

    elif C == "SET_SELECTOR_MODE":
        mode = str(payload.get("mode", "")).lower()
        if mode not in ("auto", "man"):
            raise ValueError("mode debe ser 'auto' o 'man'")
        selector_mode = mode

    elif C == "SET_REMOTE":
        remote_enabled = bool(payload.get("enabled", True))

    elif C == "SET_VALVE":
        if "in_pct" in payload:
            valve_in_pct = clamp(float(payload["in_pct"]), 0, 100)
        if "out_pct" in payload:
            valve_out_pct = clamp(float(payload["out_pct"]), 0, 100)

    elif C == "SET_LEAK":
        leak_lpm = max(0.0, float(payload.get("lpm", 0.0)))

    elif C == "SET_NOISE":
        noise_amp = max(0.0, float(payload.get("amp", 1.0)))

    elif C == "SET_DRIVE":
        t = str(payload.get("type", "")).lower()
        if t not in ("direct", "soft", "vfd"):
            raise ValueError("type debe ser direct|soft|vfd")
        DRIVE_TYPE = t
        if DRIVE_TYPE == "vfd":
            speed_pct = int(clamp(speed_pct, VFD_MIN, VFD_MAX))

    elif C == "SET_PERIODS":
        if "period_seconds" in payload:
            PERIOD_SECONDS = max(0.1, float(payload["period_seconds"]))
        if "poll_cmd_seconds" in payload:
            POLL_CMD_SECONDS = max(0.1, float(payload["poll_cmd_seconds"]))

    elif C == "DROP_EVERY":
        n = int(payload.get("tank_every_n", 0))
        globals()["DROP_EVERY_N_TANK"] = max(0, n)

    elif C == "SET_TANK_LEVEL":
        if "volume_l" in payload:
            v = clamp(float(payload["volume_l"]), 0.0, TANK_CAP_L)
            level_percent = (v / TANK_CAP_L) * 100.0
        elif "level_percent" in payload:
            level_percent = clamp(float(payload["level_percent"]), 0.0, 100.0)

    elif C == "SCENARIO":
        name = str(payload.get("name", "normal")).lower()
        if name == "normal":
            fault_mode, noise_amp, leak_lpm = "none", 1.0, 0.0
        elif name == "cavitacion":
            fault_mode, noise_amp = "cavitacion", 1.5
        elif name == "sensor_trabado":
            fault_mode = "sensor_trabado"
        elif name == "filtro_tapado":
            fault_mode = "filtro_tapado"
        elif name == "power_loss":
            fault_mode = "power_loss"
        else:
            raise ValueError("scenario desconocido")

    else:
        raise ValueError(f"Comando desconocido: {cmd}")

def process_cmd_queue(kind: str, entity_id: int):
    cmds = poll_commands_for(kind, entity_id)
    for c in cmds:
        cid     = c.get("id")
        cmd     = str(c.get("cmd"))
        payload = c.get("payload") or {}
        try:
            set_cmd_status(kind, entity_id, cid, "sent")
            execute_command(cmd, payload)
            set_cmd_status(kind, entity_id, cid, "acked")
            print(f"[cmd] {kind}/{entity_id} {cid} {cmd} OK")
        except Exception as e:
            set_cmd_status(kind, entity_id, cid, "failed", error=str(e))
            print(f"[cmd] {kind}/{entity_id} {cid} {cmd} FAIL: {e}")

# ================== Loop principal ==================
if __name__ == "__main__":
    print(f"Simulador → .env={dotenv_file} API={API} tank={TANK_ID} pump={PUMP_ID} drive={DRIVE_TYPE}")
    last_send = 0.0
    last_poll = 0.0
    _last_step_ts = time.monotonic()
    tick = 0

    while True:
        now = time.monotonic()

        # 0) Integración de estado a dt real
        dt_s = max(0.0, now - _last_step_ts)
        if dt_s > 0:
            step(dt_s)
            _last_step_ts = now

        # 1) Poll de comandos (bomba y tanque)
        if now - last_poll >= POLL_CMD_SECONDS:
            last_poll = now
            process_cmd_queue("pumps", PUMP_ID)
            process_cmd_queue("tanks", TANK_ID)

        # 2) Envío de lecturas (respetando “gaps” si están configurados)
        if now - last_send >= PERIOD_SECONDS:
            last_send = now
            tick += 1

            send_tank = not (DROP_EVERY_N_TANK > 0 and tick % DROP_EVERY_N_TANK == 0)
            send_pump = not (DROP_EVERY_N_PUMP > 0 and tick % DROP_EVERY_N_PUMP == 0)

            if not send_tank:
                print("[tank] (saltado a propósito para simular corte)")
            if not send_pump:
                print("[pump] (saltado a propósito para simular corte)")

            if send_tank:
                http_post("/ingest/tank", gen_tank())
            if send_pump:
                http_post("/ingest/pump", gen_pump())

        time.sleep(0.05)
