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
def closed_videos_client(
    client_name: str, start_date: str = None, end_date: str = None
):
    df = get_google_sheets_data(client_name)

    # Filtrado por fecha si se proporciona start_date o end_date
    df = filter_by_date(df, start_date, end_date)

    analysis_df = analyze_closed_data(df)
    result = analysis_df.to_dict(orient="records")

    return JSONResponse(content=result)


@router.get("/appointments/{client_name}")
def appointments_videos_client(
    client_name: str, start_date: str = None, end_date: str = None
):
    df = get_google_sheets_data(client_name)

    # Filtrado por fecha si se proporciona start_date o end_date
    df = filter_by_date(df, start_date, end_date)

    analysis_df = analyze_appointments_data(df)
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
def general_video_performance(start_date: str = None, end_date: str = None):
    analysis_df = analyze_general_video_performance(
        start_date, end_date
    )  # Pasar las fechas a la función

    # Convertir el DataFrame en una lista de diccionarios
    result = analysis_df.to_dict(orient="records")

    return JSONResponse(content=result)
