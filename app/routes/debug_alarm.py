# app/routes/debug_alarm.py
from fastapi import APIRouter
from app.services.alarm_listener import get_status

router = APIRouter()

@router.get("/__alarm_listener_status")
def alarm_listener_status():
    return get_status()
