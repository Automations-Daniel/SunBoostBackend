from fastapi import APIRouter, Depends
from config.bot_slack import *
from schemas.bot_slack import *


# Rutas relacionadas con el bot de alarmas
router = APIRouter(tags=["Alarm Bot"], prefix="/bot")

@router.post("/alert/")
def send_alert(channels: ChannelList, message: str):
    return send_slack_notifications(channels, message)
