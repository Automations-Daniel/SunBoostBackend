from fastapi import APIRouter, Depends
from config.data import *
from fastapi.responses import JSONResponse

# Rutas relacionadas con analisis
router = APIRouter(tags=["Data Analysis"], prefix="/data")


@router.get("/clients/")
def all_clients():
    return get_sheet_names()


@router.get("/{client_name}")
def info_clients(client_name: str):
    return get_google_sheets_data(client_name)


@router.get("/closed/{client_name}")
def closed_videos_client(client_name: str):
    df = get_google_sheets_data(client_name)
    analysis_df = analyze_closed_data(df)

    # Convertir el DataFrame en una lista de diccionarios
    result = analysis_df.to_dict(orient="records")

    return JSONResponse(content=result)


@router.get("/appointments/{client_name}")
def appointments_videos_client(client_name: str):
    df = get_google_sheets_data(client_name)
    analysis_df = analyze_appointments_data(df)

    # Convertir el DataFrame en una lista de diccionarios
    result = analysis_df.to_dict(orient="records")

    return JSONResponse(content=result)


@router.get("/quality/{client_name}")
def analyze_quality(client_name: str, nomenclatura: str):
    df = get_google_sheets_data(client_name)
    analysis_df = analyze_quality_distribution(df, nomenclatura)

    # Convertir el DataFrame en una lista de diccionarios
    result = analysis_df.to_dict(orient="records")

    return JSONResponse(content=result)


@router.get("/general/video-performance")
def general_video_performance():
    analysis_df = analyze_general_video_performance()  # Analizar el rendimiento general por vídeo

    # Convertir el DataFrame en una lista de diccionarios
    result = analysis_df.to_dict(orient="records")

    return JSONResponse(content=result)
