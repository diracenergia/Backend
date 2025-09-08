from fastapi import HTTPException
from app.repos import tank_commands as repo

VALID = {
    "queued": {"sent", "expired", "failed"},
    "sent":   {"acked", "failed", "expired"},
    "acked":  set(),
    "failed": set(),
    "expired": set(),
}

def update_tank_command_status(tank_id: int, cmd_id: int, new_status: str, error: str | None):
    prev = repo.get_command_status(cmd_id, tank_id)
    if not prev:
        raise HTTPException(404, "Comando de tanque no encontrado")
    allowed = VALID.get(prev, set())
    if new_status not in allowed:
        raise HTTPException(409, f"Transición inválida {prev} → {new_status}")

    if new_status == "sent":
        return repo.mark_sent(cmd_id)
    elif new_status == "acked":
        return repo.mark_acked(cmd_id)
    else:
        return repo.mark_other(cmd_id, new_status, error)
