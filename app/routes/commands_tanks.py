from fastapi import APIRouter, Query, Path
from app.schemas.tanks import TankCommandIn
from app.schemas.common import StatusLit, CommandStatusIn
from app.repos import tank_commands as repo
from app.services import commands as svc

router = APIRouter(prefix="/tanks", tags=["commands:tanks"])

@router.post("/{tank_id}/command", status_code=201)
def queue_tank_command(tank_id: int, body: TankCommandIn):
    inserted = repo.enqueue_tank_command(tank_id, body.cmd, body.payload, body.user)
    return inserted

@router.get("/{tank_id}/commands")
def list_tank_commands(
    tank_id: int,
    status: StatusLit | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
):
    return repo.list_tank_commands(tank_id, status, limit)

@router.post("/{tank_id}/commands/{cmd_id}/status")
def update_tank_command_status(tank_id: int, cmd_id: int, body: CommandStatusIn):
    res = svc.update_tank_command_status(tank_id, cmd_id, body.status, body.error)
    return res
