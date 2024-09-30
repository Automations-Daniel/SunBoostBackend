from fastapi import APIRouter, Depends
from config.bot_slack import *
from schemas.bot_slack import *


# Rutas relacionadas con el bot de alarmas
router = APIRouter(tags=["Alarm Bot"], prefix="/bot")


@router.post("/alert/")
def send_alert(channels: ChannelList, message: str):
    """
    Envía una alerta a los canales de Slack especificados.

    Args:
        channels (ChannelList): Una lista de identificadores de canales de Slack.
        message (str): El mensaje que se enviará a los canales.

    Returns:
        list: Una lista de respuestas de Slack, indicando el estado del envío a cada canal.
    """
    return send_slack_notifications(channels, message)
