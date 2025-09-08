from fastapi import APIRouter, Query
from app.schemas.common import StatusLit, CommandStatusIn
from app.schemas.pumps import PumpCommandIn
from app.repos import pump_commands as repo
from app.services import pumps as svc

router = APIRouter(prefix="/pumps", tags=["commands:pumps"])

@router.post("/{pump_id}/command", status_code=201)
def queue_pump_command(pump_id: int, body: PumpCommandIn):
    return svc.queue_command(pump_id, body.cmd, body.user, body.speed_pct)

@router.get("/{pump_id}/commands")
def list_pump_commands(
    pump_id: int,
    status: StatusLit | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
):
    return repo.list_pump_commands(pump_id, status, limit)

@router.post("/{pump_id}/commands/{cmd_id}/status")
def update_command_status(pump_id: int, cmd_id: int, body: CommandStatusIn):
    return svc.update_command_status(pump_id, cmd_id, body.status, body.error)
