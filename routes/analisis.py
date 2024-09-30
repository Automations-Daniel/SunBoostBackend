from fastapi import APIRouter, Depends
from config.data import *
from fastapi.responses import JSONResponse

# Rutas relacionadas con analisis
router = APIRouter(tags=["Data Analysis"], prefix="/data")


@router.get("/clients/")
def all_clients():
    """
    Recupera una lista de todos los nombres de clientes disponibles.

    Returns:
        list: Una lista con los nombres de las hojas (clientes) disponibles en Google Sheets.
    """
    return get_sheet_names()


@router.get("/{client_name}")
def info_clients(client_name: str):
    """
    Recupera los datos de un cliente específico desde Google Sheets.

    Args:
        client_name (str): El nombre del cliente cuya información se desea obtener.

    Returns:
        pd.DataFrame: Un DataFrame de pandas con los datos del cliente especificado.
    """
    return get_google_sheets_data(client_name)


@router.get("/closed/{client_name}")
def closed_videos_client(
    client_name: str, start_date: str = None, end_date: str = None
):
    """
    Recupera y analiza los cierres de un cliente específico en un rango de fechas.

    Args:
        client_name (str): El nombre del cliente cuyas estadísticas de cierre se desean obtener.
        start_date (str, opcional): Fecha de inicio del filtro (formato YYYY-MM-DD).
        end_date (str, opcional): Fecha de fin del filtro (formato YYYY-MM-DD).

    Returns:
        JSONResponse: Un objeto JSON con los resultados del análisis de cierres.
    """
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
    """
    Recupera y analiza las citas de un cliente específico en un rango de fechas.

    Args:
        client_name (str): El nombre del cliente cuyas estadísticas de citas se desean obtener.
        start_date (str, opcional): Fecha de inicio del filtro (formato YYYY-MM-DD).
        end_date (str, opcional): Fecha de fin del filtro (formato YYYY-MM-DD).

    Returns:
        JSONResponse: Un objeto JSON con los resultados del análisis de citas.
    """
    df = get_google_sheets_data(client_name)

    # Filtrado por fecha si se proporciona start_date o end_date
    df = filter_by_date(df, start_date, end_date)

    analysis_df = analyze_appointments_data(df)
    result = analysis_df.to_dict(orient="records")

    return JSONResponse(content=result)


@router.get("/quality/{client_name}")
def analyze_quality(
    client_name: str, nomenclatura: str, start_date: str = None, end_date: str = None
):
    """
    Analiza la calidad de los leads por etapa para un video específico del cliente, con un filtro opcional de fechas.

    Args:
        client_name (str): El nombre del cliente cuyos datos se desean analizar.
        nomenclatura (str): El identificador del video a analizar.
        start_date (str, opcional): Fecha de inicio del filtro (formato YYYY-MM-DD).
        end_date (str, opcional): Fecha de fin del filtro (formato YYYY-MM-DD).

    Returns:
        JSONResponse: Un objeto JSON con la distribución de calidad por etapas para el video especificado.
    """
    df = get_google_sheets_data(client_name)

    # Aplicar el filtro de fechas al DataFrame si se proporcionan
    if start_date or end_date:
        df = filter_by_date(df, start_date, end_date)

    # Realizar el análisis de calidad con el DataFrame filtrado
    analysis_df = analyze_quality_distribution(df, nomenclatura)

    # Convertir el DataFrame en una lista de diccionarios
    result = analysis_df.to_dict(orient="records")

    return JSONResponse(content=result)


@router.get("/general/video-performance")
def general_video_performance(start_date: str = None, end_date: str = None):
    """
    Analiza el rendimiento general de los videos en un rango de fechas.

    Args:
        start_date (str, opcional): Fecha de inicio del filtro (formato YYYY-MM-DD).
        end_date (str, opcional): Fecha de fin del filtro (formato YYYY-MM-DD).

    Returns:
        JSONResponse: Un objeto JSON con el rendimiento de los videos dentro del rango de fechas especificado.
    """
    analysis_df = analyze_general_video_performance(
        start_date, end_date
    )  # Pasar las fechas a la función

    # Convertir el DataFrame en una lista de diccionarios
    result = analysis_df.to_dict(orient="records")

    return JSONResponse(content=result)
