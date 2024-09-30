from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
import pandas as pd
import numpy as np
import re
import os

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
KEY = "key.json"
# Escribe aquí el ID de tu documento:
SPREADSHEET_ID = "1edfM96ge_NasWsH16WpGihBeCj0g-Rj2T6zmxZMycp4"


def get_google_sheets_data(range_name: str):
    """
    Obtiene datos de un rango específico en una hoja de cálculo de Google Sheets.

    Args:
        range_name (str): El rango dentro de Google Sheets del cual se van a recuperar los datos.

    Returns:
        pd.DataFrame: Un DataFrame de pandas que contiene los datos del rango especificado.
                      Retorna un DataFrame vacío si no se encuentran datos.
    """
    creds = service_account.Credentials.from_service_account_file(KEY, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()
    # Llamada a la API para obtener los datos de la hoja especificada
    result = (
        sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=range_name).execute()
    )
    values = result.get("values", [])
    # Verifica si hay datos
    if not values:
        return pd.DataFrame()
    max_columns = len(values[0])
    normalized_values = [row + [""] * (max_columns - len(row)) for row in values]
    df = pd.DataFrame(normalized_values[1:], columns=normalized_values[0])
    return df


def get_sheet_names():
    """
    Recupera los nombres de todas las hojas (pestañas) visibles dentro de un documento de Google Sheets.

    Returns:
        list: Una lista de cadenas, cada una representando el nombre de una hoja visible (típicamente clientes).
    """
    creds = service_account.Credentials.from_service_account_file(KEY, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()
    # Llamada a la API para obtener las propiedades del documento
    result = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
    # Filtrar hojas que no están ocultas
    sheet_names = [
        sheet["properties"]["title"]
        for sheet in result["sheets"]
        if not sheet["properties"].get("hidden", False)
    ]
    return sheet_names


def load_video_links():
    """
    Carga enlaces de video desde archivos CSV ubicados en la carpeta 'dataLinksVideos'.

    Returns:
        dict: Un diccionario donde las claves son los IDs de video y los valores son los enlaces correspondientes.
    """
    folder_path = "dataLinksVideos"  # Asegúrate de que esta ruta es correcta
    video_links = {}

    # Iterar sobre todos los archivos CSV en la carpeta
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".csv"):
            file_path = os.path.join(folder_path, file_name)
            df = pd.read_csv(file_path)

            # Recorrer cada fila del DataFrame y obtener el ID del video y su enlace
            for _, row in df.iterrows():
                # Convertir el valor de ID a string antes de aplicar strip()
                video_id = str(row.get("ID", "")).strip()  # Convertir a string
                # Convertir el valor de Link a string antes de aplicar strip()
                link = str(row.get("Link", "")).strip()  # Convertir a string

                # Guardar el enlace si el ID no es vacío
                if video_id:
                    video_links[video_id] = link
    return video_links


def preprocess_data(df):
    """
    Normaliza el contenido UTM, la etapa, extrae el ID del video y la leyenda del contenido UTM,
    y agrega enlaces de video a partir de los datos cargados.

    Args:
        df (pd.DataFrame): Un DataFrame de pandas con el contenido UTM y los datos de etapa.

    Returns:
        pd.DataFrame: Un DataFrame con el contenido UTM normalizado, el ID del video extraído,
                      la leyenda y los enlaces de video correspondientes.
    """
    df = df.copy()

    # Comprobaciones generales
    df["UTM Content"] = df["UTM Content"].str.upper().str.strip()
    df["Stage"] = df["Stage"].str.upper().str.strip()

    # Normalización del campo Video Id
    df["Video ID"] = df["UTM Content"].apply(
        lambda x: (
            "Sin Matricula"
            if not x or pd.isna(x)
            else (
                re.match(r"^[A-Z0-9.-]+", x).group(0)
                if re.match(r"^[A-Z0-9.-]+", x)
                else "Sin Matricula"
            )
        )
    )

    # Extraer la leyenda después de la matrícula de vídeo, si no comienza con un número
    df["Leyenda"] = df["UTM Content"].apply(
        lambda x: (
            x.split("|", 1)[1].strip()
            if "|" in x
            else (
                re.split(r"^[A-Z0-9.-]+\s*", x)[1].strip()
                if len(re.split(r"^[A-Z0-9.-]+\s*", x)) > 1
                and not re.match(r"^\d", re.split(r"^[A-Z0-9.-]+\s*", x)[1])
                else ""
            )
        )
    )

    # Cargar los links
    video_links = load_video_links()

    # Asignar links basados en el Video ID
    df["Link"] = df["Video ID"].map(video_links)

    # Reemplazar NaN o valores nulos en los links con "Sin enlace"
    df["Link"] = df["Link"].fillna(
        "Sin enlace"
    )  # Usar fillna para reemplazar nulos directamente

    return df


def analyze_closed_data(df, stages_to_analyze=["CLOSED", "INSTALLED"]):
    """
    Analiza los datos de cierres, contando los leads y cierres basados en las etapas especificadas.

    Args:
        df (pd.DataFrame): Un DataFrame que contiene el contenido UTM y datos de etapa.
        stages_to_analyze (list): Una lista de etapas a analizar para los cierres (por defecto ["CLOSED", "INSTALLED"]).

    Returns:
        pd.DataFrame: Un DataFrame que contiene leads, cierres y las tasas de cierre calculadas.
    """
    df = preprocess_data(df)
    df = df.copy()

    # Contar los leads
    leads = df.groupby(["Video ID", "Leyenda", "Link"]).size().reset_index(name="Leads")

    # Filtrar y contar los cierres según los stages especificados
    cierres = (
        df[df["Stage"].isin(stages_to_analyze)]
        .groupby(["Video ID", "Leyenda", "Link"])
        .size()
        .reset_index(name="Cierres")
    )

    # Unir los leads y cierres en un solo DataFrame
    analysis_df = pd.merge(
        leads, cierres, on=["Video ID", "Leyenda", "Link"], how="left"
    )
    analysis_df["Cierres"] = analysis_df["Cierres"].fillna(0)

    # Calcular ratios y tasas de cierre
    analysis_df["Ratio Cierre"] = analysis_df.apply(
        lambda row: row["Leads"] / row["Cierres"] if row["Cierres"] > 0 else 0, axis=1
    )
    analysis_df["Tasa de Cierre"] = (
        analysis_df["Cierres"] / analysis_df["Leads"]
    ) * 100

    # Comprobaciones de error generales
    analysis_df["Ratio Cierre"] = analysis_df["Ratio Cierre"].replace(
        [float("inf"), pd.NA], 0
    )
    analysis_df["Tasa de Cierre"] = analysis_df["Tasa de Cierre"].replace(
        [float("inf"), pd.NA], 0
    )

    analysis_df = analysis_df.sort_values(by="Cierres", ascending=False)

    return analysis_df


def analyze_appointments_data(
    df,
    stages_to_analyze=[
        "CLOSED",
        "INSTALLED",
        "SHOWED (NOT CLOSED)",
        "SHOWED (NOT QUALIFIED)",
        "NO SHOW (RE-SCHEDULE)",
        "APPOINTMENT BOOKED",
        "APPOINTMENT CANCEL",
    ],
):
    """
    Analiza los datos de citas, contando leads y citas basadas en las etapas especificadas.

    Args:
        df (pd.DataFrame): Un DataFrame que contiene el contenido UTM y los datos de etapa.
        stages_to_analyze (list): Una lista de etapas a analizar para las citas (por defecto incluye varias etapas).

    Returns:
        pd.DataFrame: Un DataFrame que contiene leads, citas y las tasas de citas calculadas.
    """
    df = preprocess_data(df)
    df = df.copy()

    # Contar los leads
    leads = df.groupby(["Video ID", "Leyenda", "Link"]).size().reset_index(name="Leads")

    # Filtrar y contar las citas según los stages especificados
    citas = (
        df[df["Stage"].isin(stages_to_analyze)]
        .groupby(["Video ID", "Leyenda", "Link"])
        .size()
        .reset_index(name="Citas")
    )

    # Unir los leads y citas en un solo DataFrame
    analysis_df = pd.merge(leads, citas, on=["Video ID", "Leyenda", "Link"], how="left")
    analysis_df["Citas"] = analysis_df["Citas"].fillna(0)

    # Calcular ratios y tasas de citas
    analysis_df["Ratio Citas"] = analysis_df.apply(
        lambda row: row["Leads"] / row["Citas"] if row["Citas"] > 0 else 0, axis=1
    )
    analysis_df["Tasa de Citas"] = (analysis_df["Citas"] / analysis_df["Leads"]) * 100

    # Comprobaciones de error general
    analysis_df["Ratio Citas"] = analysis_df["Ratio Citas"].replace(
        [float("inf"), pd.NA], 0
    )
    analysis_df["Tasa de Citas"] = analysis_df["Tasa de Citas"].replace(
        [float("inf"), pd.NA], 0
    )

    analysis_df = analysis_df.sort_values(by="Citas", ascending=False)

    return analysis_df


def analyze_quality_distribution(df, video_id):
    """
    Analiza la distribución de calidad por etapa para un video específico.

    Args:
        df (pd.DataFrame): Un DataFrame que contiene el contenido UTM, la etapa y el ID del video.
        video_id (str): El ID del video específico a analizar.

    Returns:
        pd.DataFrame: Un DataFrame que muestra el número de leads por etapa y su porcentaje para el video especificado.
    """
    df = preprocess_data(df)

    # Filtrar filas donde 'Video ID' coincide con la nomenclatura proporcionada
    filtered_df = df[df["Video ID"] == video_id]

    # Contar el número de leads por Stage
    leads_by_stage = (
        filtered_df.groupby("Stage").size().reset_index(name="Numero de Leads")
    )

    # Calcular el porcentaje de leads por Stage
    total_leads = leads_by_stage["Numero de Leads"].sum()
    leads_by_stage["Porcentaje"] = (
        leads_by_stage["Numero de Leads"] / total_leads
    ) * 100

    leads_by_stage = leads_by_stage.sort_values(by="Porcentaje", ascending=False)

    return leads_by_stage


def analyze_general_video_performance(start_date=None, end_date=None):
    """
    Analiza el rendimiento general de los videos de todos los clientes con un filtro opcional por rango de fechas.

    Args:
        start_date (str, opcional): Fecha de inicio en formato YYYY-MM-DD.
        end_date (str, opcional): Fecha de fin en formato YYYY-MM-DD.

    Returns:
        pd.DataFrame: DataFrame con los resultados del análisis.
    """
    clients = get_sheet_names()
    final_df = pd.DataFrame()

    for client in clients:
        df = get_google_sheets_data(client)
        if not df.empty:
            # Aplicar el filtro de fechas
            df = filter_by_date(df, start_date, end_date)

            # Analizar los cierres y citas
            closed_df = analyze_closed_data(df)
            appointments_df = analyze_appointments_data(df)

            # Unir los leads, cierres y citas en un solo DataFrame por cliente
            combined_df = pd.merge(
                appointments_df,
                closed_df[["Video ID", "Leyenda", "Link", "Cierres"]],
                on=["Video ID", "Leyenda", "Link"],
                how="left",
            )
            combined_df["Cierres"] = combined_df["Cierres"].fillna(0)
            combined_df["Citas"] = combined_df["Citas"].fillna(0)
            final_df = pd.concat([final_df, combined_df], ignore_index=True)

    # Agrupar los resultados por Video ID y Leyenda
    grouped_df = (
        final_df.groupby(["Video ID", "Leyenda", "Link"])
        .agg(
            Leads_Totales=pd.NamedAgg(column="Leads", aggfunc="sum"),
            Citas_Totales=pd.NamedAgg(column="Citas", aggfunc="sum"),
            Cierres_Totales=pd.NamedAgg(column="Cierres", aggfunc="sum"),
        )
        .reset_index()
    )

    sorted_df = grouped_df.sort_values(by="Leads_Totales", ascending=False)

    return sorted_df


def filter_by_date(df, start_date=None, end_date=None):
    """
    Filtra el DataFrame por un rango de fechas opcional.

    Args:
        df (pd.DataFrame): DataFrame que contiene una columna de fecha 'Created at (fecha)'.
        start_date (str, opcional): Fecha de inicio del filtro. Formato YYYY-MM-DD.
        end_date (str, opcional): Fecha de fin del filtro. Formato YYYY-MM-DD.

    Returns:
        pd.DataFrame: DataFrame filtrado por el rango de fechas.
    """
    # Convertir la columna 'Created at (fecha)' a formato datetime
    df["Created at (fecha)"] = pd.to_datetime(df["Created at (fecha)"], errors="coerce")

    # Convertir start_date y end_date a datetime si se proporcionan
    start_date = pd.to_datetime(start_date) if start_date else None
    end_date = pd.to_datetime(end_date) if end_date else None

    # Si solo se proporciona start_date, filtrar solo por ese día
    if start_date and not end_date:
        df = df.loc[df["Created at (fecha)"] == start_date]

    # Si solo se proporciona end_date, filtrar solo por ese día
    elif end_date and not start_date:
        df = df.loc[df["Created at (fecha)"] == end_date]

    # Si ambas fechas están, intercambiar si start_date > end_date
    elif start_date and end_date:
        if start_date > end_date:
            start_date, end_date = end_date, start_date
        df = df.loc[
            (df["Created at (fecha)"] >= start_date)
            & (df["Created at (fecha)"] <= end_date)
        ]

    return df
