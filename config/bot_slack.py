import slack
from os import getenv
from dotenv import load_dotenv
from schemas.bot_slack import ChannelList, SlackResponseAlerts

# Cargar variables de entorno desde el archivo .env
load_dotenv(".env")

client = slack.WebClient(token=getenv("SLACK_TOKEN"))


def send_slack_notifications(channels_data: ChannelList, message: str):
    """
    Envía un mensaje a múltiples canales de Slack.

    Args:
        channels_data (ChannelList): Una lista de identificadores de canales de Slack.
        message (str): El mensaje que se enviará a los canales de Slack.

    Returns:
        list: Una lista de SlackResponseAlerts, cada uno conteniendo el canal, mensaje,
              y si el mensaje fue enviado exitosamente.
    """
    responses = []
    for channel in channels_data:
        try:
            response = client.chat_postMessage(channel=channel, text=message)
            responses.append(
                SlackResponseAlerts(channel=channel, message=message, success=True)
            )
        except Exception as e:
            responses.append(
                SlackResponseAlerts(channel=channel, message=message, success=False)
            )
    return responses
