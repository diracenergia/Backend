from app.repos import tanks as repo

def process_tank_thresholds(insert_result: dict) -> dict | None:
    rlevel = insert_result.get("level_percent")
    if rlevel is None:
        return None

    tank_id = insert_result["tank_id"]
    cfg = repo.get_tank_thresholds(tank_id)
    if not cfg:
        return None

    low, low_low, high, high_high = cfg
    lvl = float(rlevel)

    code = severity = msg = None
    if lvl <= low_low:
        code, severity, msg = "LOW_LOW", "critical", f"Nivel muy bajo ({lvl:.1f}% <= {low_low:.2f}%)"
    elif lvl <= low:
        code, severity, msg = "LOW", "warning", f"Nivel bajo ({lvl:.1f}% <= {low:.2f}%)"
    elif lvl >= high_high:
        code, severity, msg = "HIGH_HIGH", "critical", f"Nivel muy alto ({lvl:.1f}% >= {high_high:.2f}%)"
    elif lvl >= high:
        code, severity, msg = "HIGH", "warning", f"Nivel alto ({lvl:.1f}% >= {high:.2f}%)"

    latest = {
        "id": insert_result["id"],
        "ts": insert_result["ts"].isoformat(),
        "tank_id": tank_id,
        "raw_json": insert_result["raw_json"],
        "volume_l": float(insert_result["volume_l"]) if insert_result["volume_l"] is not None else None,
        "device_id": insert_result["device_id"],
        "level_percent": lvl,
        "temperature_c": float(insert_result["temperature_c"]) if insert_result["temperature_c"] is not None else None,
    }

    if code is None:
        repo.clear_tank_alarms(tank_id, latest)
        return None
    return repo.create_tank_alarm(tank_id, code, severity, msg, latest)
