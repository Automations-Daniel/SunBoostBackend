from pydantic import BaseModel

#Entrada y salida para funciones y endpoints
class ChannelList(BaseModel):
    channels: list[str]

class SlackResponseAlerts(BaseModel):
    channel: str
    message: str
    success: bool