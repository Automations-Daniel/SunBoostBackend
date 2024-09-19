from controllers.bot_slack import *
from fastapi import FastAPI, Request
from dotenv import load_dotenv
from routes import analisis, bot_slack
from fastapi.middleware.cors import CORSMiddleware
import time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# Cargamos las variables de entorno
load_dotenv()


description = """
# ¡Bienvenido a la api para análisis de datos de **SunBoostCRM**!

Somos una agencia premium en generación de leads de Home Improvement & Eficiencia energética. 

---

**Regístrate** si aún no lo has hecho para acceder a nuestros analisis, recuerda hacerlo con tu **correo corporativo SunBoost**. 🌟

**Inicia sesión** y sumérgete en nuestros análisis. ✨

---

**(En construcción)**
"""


# Se crea el objeto con todos los endpoints para lanzarlo despues con uvicorn
app = FastAPI(
    title="SunBoostData",
    description=description,
    version="1.0.0",
    contact={
        "email": "automations@sunboostcrm.com",
    },
)

origins = ["http://18.117.88.146", "http://18.117.88.146:8501"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    # Expose headers para que el cliente del navegador pueda ver los headers personalizados
)

app.include_router(analisis.router)
app.include_router(bot_slack.router)


# Ruta principal
@app.get("/")
async def root():
    return {"message": "API for SunBoostCRM, go to docs"}


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response


# Prueba de job
def my_scheduled_job():
    print(f"Job ejecutado a las {time.strftime('%X')}")


# Configuración del BackgroundScheduler
scheduler = BackgroundScheduler()


# Configurar alerta diaria a las 01:00 AM todos los días
scheduler.add_job(send_daily_alerts, CronTrigger(hour=1, minute=0))


# Configurar alerta semanal a las 01:00 AM todos los lunes
scheduler.add_job(send_weekly_alerts, CronTrigger(day_of_week="mon", hour=1, minute=2))


# Programar la tarea mensual (por ejemplo, el primer día de cada mes a las 01:00 AM)
scheduler.add_job(send_monthly_alerts, CronTrigger(day=1, hour=1, minute=3))
scheduler.start()


@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()
