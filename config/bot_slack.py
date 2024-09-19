import slack
from os import getenv
from dotenv import load_dotenv
from schemas.bot_slack import ChannelList, SlackResponseAlerts

# Cargar variables de entorno desde el archivo .env
load_dotenv(".env")

client = slack.WebClient(token=getenv("SLACK_TOKEN"))


# Función para enviar mensajes a múltiples canales
def send_slack_notifications(channels_data: ChannelList, message: str):
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
