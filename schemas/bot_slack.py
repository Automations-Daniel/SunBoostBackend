from pydantic import BaseModel

#Modelos Pydantic para entrada y salida de funciones y endpoints
class ChannelList(BaseModel):
    channels: list[str]

class SlackResponseAlerts(BaseModel):
    channel: str
    message: str
    success: bool