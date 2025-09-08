from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from app.schemas.alarms import AckIn
from app.repos import alarms as repo_alarms
from app.repos import audit as repo_audit
from app.services.notify import notify_ack

router = APIRouter(prefix="/alarms", tags=["alarms"])

@router.get("")
def list_alarms(active: bool | None = Query(True)):
    return repo_alarms.list_alarms(active)

@router.post("/{alarm_id}/ack")
def ack_alarm(alarm_id: int, body: AckIn, background_tasks: BackgroundTasks):
    alarm_dict, asset_type, asset_id, code_sev = repo_alarms.ack_alarm(alarm_id, body.user)
    if not alarm_dict:
        raise HTTPException(status_code=404, detail="Alarma no activa o inexistente")

    code, severity = code_sev
    # auditoría
    repo_audit.insert_alarm_ack_event(body.user, asset_type, asset_id, code, severity, body.note)

    # notificar async
    background_tasks.add_task(notify_ack, alarm_dict, body.user)

    return {"ok": True, "alarm": alarm_dict}
