from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
import pandas as pd
import re

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
KEY = "key.json"
# Escribe aquí el ID de tu documento:
SPREADSHEET_ID = "1edfM96ge_NasWsH16WpGihBeCj0g-Rj2T6zmxZMycp4"


def get_google_sheets_data(range_name: str):
    """
    Obtiene los datos de un rango específico de una hoja de cálculo de Google Sheets
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
    Obtiene los nombres de todas las hojas visibles en una hoja de cálculo de Google Sheets, en este caso cada cliente
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


def preprocess_data(df):
    """
    Realiza la normalización de UTM Content, Stage, extrae el Video ID y la Leyenda.
    Si el UTM Content está vacío, asigna "Sin Matricula" como Video ID.
    La leyenda es el contenido después de la matrícula, siempre y cuando no comience con un número.
    """
    df = df.copy()

    # Comprobaciones generales
    df["UTM Content"] = df["UTM Content"].str.upper().str.strip()
    df["Stage"] = df["Stage"].str.upper().str.strip()

    # Normalizacion del campo Video Id
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

    return df


def analyze_closed_data(df, stages_to_analyze=["CLOSED", "INSTALLED"]):
    """
    Analiza los datos de cierres, permitiendo especificar los stages a analizar.
    Si no hay UTM Content, registra como 'Sin matrícula'.
    """
    df = preprocess_data(df)
    df = df.copy()

    # Contar los leads
    leads = df.groupby(["Video ID", "Leyenda"]).size().reset_index(name="Leads")

    # Filtrar y contar los cierres según los stages especificados
    cierres = (
        df[df["Stage"].isin(stages_to_analyze)]
        .groupby(["Video ID", "Leyenda"])
        .size()
        .reset_index(name="Cierres")
    )

    # Unir los leads y cierres en un solo DataFrame
    analysis_df = pd.merge(leads, cierres, on=["Video ID", "Leyenda"], how="left")
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
    Analiza los datos de citas, permitiendo especificar los stages a analizar.
    Si no hay UTM Content, registra como 'Sin matrícula'.
    """
    df = preprocess_data(df)
    df = df.copy()

    # Contar los leads
    leads = df.groupby(["Video ID", "Leyenda"]).size().reset_index(name="Leads")

    # Filtrar y contar las citas según los stages especificados
    citas = (
        df[df["Stage"].isin(stages_to_analyze)]
        .groupby(["Video ID", "Leyenda"])
        .size()
        .reset_index(name="Citas")
    )

    # Unir los leads y citas en un solo DataFrame
    analysis_df = pd.merge(leads, citas, on=["Video ID", "Leyenda"], how="left")
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
    Analiza la distribución de calidad por Stage para un video específico.
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


def analyze_general_video_performance():
    """
    Analiza los datos generales de todos los clientes y devuelve los leads totales, citas totales y cierres totales de cada vídeo,
    junto con la Leyenda correspondiente.
    Los resultados se organizan de mayor a menor por leads totales.
    """
    clients = get_sheet_names()
    final_df = pd.DataFrame()

    for client in clients:
        df = get_google_sheets_data(client)
        if not df.empty:
            # Analizar los cierres y citas
            closed_df = analyze_closed_data(df)
            appointments_df = analyze_appointments_data(df)
            # Unir los leads, cierres y citas en un solo DataFrame por cliente
            combined_df = pd.merge(
                appointments_df,
                closed_df[["Video ID", "Leyenda", "Cierres"]],
                on=["Video ID", "Leyenda"],
                how="left",
            )
            combined_df["Cierres"] = combined_df["Cierres"].fillna(0)
            combined_df["Citas"] = combined_df["Citas"].fillna(0)
            final_df = pd.concat([final_df, combined_df], ignore_index=True)

    # Agrupar los resultados por Video ID y Leyenda
    grouped_df = (
        final_df.groupby(["Video ID", "Leyenda"])
        .agg(
            Leads_Totales=pd.NamedAgg(column="Leads", aggfunc="sum"),
            Citas_Totales=pd.NamedAgg(column="Citas", aggfunc="sum"),
            Cierres_Totales=pd.NamedAgg(column="Cierres", aggfunc="sum"),
        )
        .reset_index()
    )

    sorted_df = grouped_df.sort_values(by="Leads_Totales", ascending=False)

    return sorted_df
