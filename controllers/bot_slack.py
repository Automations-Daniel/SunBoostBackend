from config.bot_slack import *
from config.data import (
    get_sheet_names,
    analyze_appointments_data,
    analyze_closed_data,
    get_google_sheets_data,
)
from datetime import datetime, timedelta
import re
import pandas as pd


def send_daily_appointments_alert():
    """
    Envía un resumen diario de las citas a Slack, solo si hay citas relevantes.
    Si no hubo leads analizables para ningún cliente, envía una alerta.
    """
    # Usar la fecha del día anterior
    date = (datetime.now() - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    clients = get_sheet_names()

    final_message = f"*Resumen de citas diario {date.strftime('%m/%d/%Y')}:* <@U053520KZ4P> <@U07F0LZGA4F>\n"
    send_message = False

    for client in clients:
        df = get_google_sheets_data(client)
        df = df.copy()

        df["Created at (fecha)"] = pd.to_datetime(
            df["Created at (fecha)"], errors="coerce"
        )

        # Limpieza y conversión de 'Dia de cita'
        df["Dia de cita"] = df["Dia de cita"].str.strip()
        df["Dia de cita"] = df["Dia de cita"].replace(r"[^\w\s,]", "", regex=True)
        df["Dia de cita"] = pd.to_datetime(
            df["Dia de cita"], format="%B %d, %Y", errors="coerce"
        )

        # Que la fecha solo tenga día, mes y año (sin hora)
        df["Created at (fecha)"] = df["Created at (fecha)"].dt.floor("d")
        df["Dia de cita"] = df["Dia de cita"].dt.floor("d")

        # Filtrar los datos para la fecha proporcionada
        df_filtered = df.loc[
            (df["Created at (fecha)"] == date) | (df["Dia de cita"] == date)
        ]

        if not df_filtered.empty:
            appointments = analyze_appointments_data(df_filtered)
            appointments = appointments.loc[appointments["Citas"] != 0]

            if not appointments.empty:
                # Si hay citas relevantes, añadirlas al mensaje
                send_message = True
                message = f"*{client}:*\n"
                for _, row in appointments.iterrows():
                    leyenda = f" ({row['Leyenda']})" if row["Leyenda"] else ""
                    link = row["Link"] if pd.notna(row["Link"]) else "Sin enlace"
                    video_text = (
                        f"<{link}|Ver video>" if link != "Sin enlace" else "Sin enlace"
                    )
                    message += f"  • {row['Video ID']}{leyenda}, Citas: *{row['Citas']}* - {video_text}\n"
                final_message += message + "\n"

    # Verificamos
    if send_message:
        print(send_slack_notifications(["#creativos-citas"], final_message))
    else:
        # Enviar alerta si no hubo leads analizables
        alert_message = f"*No hubo leads analizables el {date.strftime('%m/%d/%Y')} para ningún cliente.*"
        print(send_slack_notifications(["#creativos-citas"], alert_message))


def send_daily_closed_alert():
    """
    Envía un resumen diario de los cierres a Slack, solo si hay cierres relevantes.
    Si no hubo leads analizables para ningún cliente, envía una alerta.
    """
    # Usar la fecha del día anterior
    date = (datetime.now() - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    clients = get_sheet_names()

    final_message = f"*Resumen de cierres diario {date.strftime('%m/%d/%Y')}:* <@U053520KZ4P> <@U07F0LZGA4F>\n"
    send_message = False

    for client in clients:
        df = get_google_sheets_data(client)
        df = df.copy()

        df["Created at (fecha)"] = pd.to_datetime(
            df["Created at (fecha)"], errors="coerce"
        )

        # Limpieza y conversión de 'Dia de cita'
        df["Dia de cita"] = df["Dia de cita"].str.strip()
        df["Dia de cita"] = df["Dia de cita"].replace(r"[^\w\s,]", "", regex=True)
        df["Dia de cita"] = pd.to_datetime(
            df["Dia de cita"], format="%B %d, %Y", errors="coerce"
        )

        df["Created at (fecha)"] = df["Created at (fecha)"].dt.floor("d")
        df["Dia de cita"] = df["Dia de cita"].dt.floor("d")

        df_filtered = df.loc[
            (df["Created at (fecha)"] == date) | (df["Dia de cita"] == date)
        ]

        if not df_filtered.empty:
            closed = analyze_closed_data(df_filtered)
            closed = closed.loc[closed["Cierres"] != 0]

            if not closed.empty:
                # Si hay cierres relevantes, añadirlos al mensaje
                send_message = True
                message = f"*{client}:*\n"
                for _, row in closed.iterrows():
                    leyenda = f" ({row['Leyenda']})" if row["Leyenda"] else ""
                    link = row["Link"] if pd.notna(row["Link"]) else "Sin enlace"
                    video_text = (
                        f"<{link}|Ver video>" if link != "Sin enlace" else "Sin enlace"
                    )
                    message += f"  • {row['Video ID']}{leyenda}, Cierres: *{row['Cierres']}* - {video_text}\n"
                final_message += message + "\n"

    if send_message:
        print(send_slack_notifications(["#creativos-cierres"], final_message))
    else:
        # Enviar alerta si no hubo leads analizables
        alert_message = f"*No hubo leads analizables el {date.strftime('%m/%d/%Y')} para ningún cliente.*"
        print(send_slack_notifications(["#creativos-cierres"], alert_message))


def send_daily_alerts():
    send_daily_appointments_alert()
    send_daily_closed_alert()


def send_weekly_appointments_alert():
    """
    Envía un resumen semanal de las citas a Slack, solo si hay citas relevantes.
    Si no hubo leads analizables para ningún cliente, envía una alerta.
    """
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=6)

    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)

    start_date_str = start_date.strftime("%m/%d/%Y")
    end_date_str = end_date.strftime("%m/%d/%Y")

    clients = get_sheet_names()

    final_message = f"*Resumen semanal de citas ({start_date_str} - {end_date_str}):* <@U053520KZ4P> <@U07F0LZGA4F>\n"
    send_message = False  # Variable para controlar si se envía el mensaje o no

    for client in clients:
        df = get_google_sheets_data(client)
        df = df.copy()

        df["Created at (fecha)"] = pd.to_datetime(
            df["Created at (fecha)"], errors="coerce"
        )
        df["Created at (fecha)"] = df["Created at (fecha)"].dt.floor("d")

        # Filtrar los datos para el rango de fechas
        df_filtered = df.loc[
            (df["Created at (fecha)"] >= start_date)
            & (df["Created at (fecha)"] <= end_date)
        ]

        if not df_filtered.empty:
            appointments = analyze_appointments_data(df_filtered)
            appointments = appointments.loc[appointments["Citas"] != 0]

            if not appointments.empty:
                send_message = True
                message = f"*{client}:*\n"
                for _, row in appointments.iterrows():
                    leyenda = f" ({row['Leyenda']})" if row["Leyenda"] else ""
                    link = row["Link"] if pd.notna(row["Link"]) else "Sin enlace"
                    video_text = (
                        f"<{link}|Ver video>" if link != "Sin enlace" else "Sin enlace"
                    )
                    message += f"  • {row['Video ID']}{leyenda}, Citas: *{row['Citas']}* - {video_text}\n"
                final_message += message + "\n"

    if send_message:
        print(send_slack_notifications(["#creativos-citas"], final_message))
    else:
        alert_message = f"*No hubo leads analizables entre el {start_date_str} y el {end_date_str} para ningún cliente.*"
        print(send_slack_notifications(["#creativos-citas"], alert_message))


def send_weekly_closed_alert():
    """
    Envía un resumen semanal de los cierres a Slack, solo si hay cierres relevantes.
    Si no hubo leads analizables para ningún cliente, envía una alerta.
    """
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=6)

    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)

    start_date_str = start_date.strftime("%m/%d/%Y")
    end_date_str = end_date.strftime("%m/%d/%Y")

    clients = get_sheet_names()

    final_message = f"*Resumen semanal de cierres ({start_date_str} - {end_date_str}):* <@U053520KZ4P> <@U07F0LZGA4F>\n"
    send_message = False

    for client in clients:
        df = get_google_sheets_data(client)
        df = df.copy()

        df["Created at (fecha)"] = pd.to_datetime(
            df["Created at (fecha)"], errors="coerce"
        )
        df["Created at (fecha)"] = df["Created at (fecha)"].dt.floor("d")

        df_filtered = df.loc[
            (df["Created at (fecha)"] >= start_date)
            & (df["Created at (fecha)"] <= end_date)
        ]

        if not df_filtered.empty:
            closed = analyze_closed_data(df_filtered)
            closed = closed.loc[closed["Cierres"] != 0]

            if not closed.empty:
                send_message = True
                message = f"*{client}:*\n"
                for _, row in closed.iterrows():
                    leyenda = f" ({row['Leyenda']})" if row["Leyenda"] else ""
                    link = row["Link"] if pd.notna(row["Link"]) else "Sin enlace"
                    video_text = (
                        f"<{link}|Ver video>" if link != "Sin enlace" else "Sin enlace"
                    )
                    message += f"  • {row['Video ID']}{leyenda}, Cierres: *{row['Cierres']}* - {video_text}\n"
                final_message += message + "\n"

    if send_message:
        print(send_slack_notifications(["#creativos-cierres"], final_message))
    else:
        alert_message = f"*No hubo leads analizables entre el {start_date_str} y el {end_date_str} para ningún cliente.*"
        print(send_slack_notifications(["#creativos-cierres"], alert_message))


def send_weekly_alerts():
    """
    Envía los resúmenes semanales de citas y cierres a sus respectivos canales en Slack.
    """
    send_weekly_appointments_alert()
    send_weekly_closed_alert()


def send_monthly_appointments_alert():
    """
    Envía un resumen mensual de las citas a Slack, solo si hay citas relevantes.
    Si no hubo leads analizables para ningún cliente, envía una alerta.
    """
    now = datetime.now()
    start_date = (now.replace(day=1) - timedelta(days=1)).replace(
        day=1, hour=0, minute=0, second=0, microsecond=0
    )
    end_date = now.replace(
        day=1, hour=23, minute=59, second=59, microsecond=999999
    ) - timedelta(days=1)

    start_date_str = start_date.strftime("%m/%d/%Y")
    end_date_str = end_date.strftime("%m/%d/%Y")

    clients = get_sheet_names()

    final_message = f"*Resumen mensual de citas ({start_date_str} - {end_date_str}):* <@U053520KZ4P> <@U07F0LZGA4F>\n"
    send_message = False

    for client in clients:
        df = get_google_sheets_data(client)
        df = df.copy()

        df["Created at (fecha)"] = pd.to_datetime(
            df["Created at (fecha)"], errors="coerce"
        )
        df["Created at (fecha)"] = df["Created at (fecha)"].dt.floor("d")

        df_filtered = df.loc[
            (df["Created at (fecha)"] >= start_date)
            & (df["Created at (fecha)"] <= end_date)
        ]

        if not df_filtered.empty:
            appointments = analyze_appointments_data(df_filtered)
            appointments = appointments.loc[appointments["Citas"] != 0]

            if not appointments.empty:
                send_message = True
                message = f"*{client}:*\n"
                for _, row in appointments.iterrows():
                    leyenda = f" ({row['Leyenda']})" if row["Leyenda"] else ""
                    link = row["Link"] if pd.notna(row["Link"]) else "Sin enlace"
                    video_text = (
                        f"<{link}|Ver video>" if link != "Sin enlace" else "Sin enlace"
                    )
                    message += f"  • {row['Video ID']}{leyenda}, Citas: *{row['Citas']}* - {video_text}\n"
                final_message += message + "\n"

    if send_message:
        print(send_slack_notifications(["#creativos-citas"], final_message))
    else:
        alert_message = f"*No hubo leads analizables entre el {start_date_str} y el {end_date_str} para ningún cliente.*"
        print(send_slack_notifications(["#creativos-citas"], alert_message))


def send_monthly_closed_alert():
    """
    Envía un resumen mensual de los cierres a Slack, solo si hay cierres relevantes.
    Si no hubo leads analizables para ningún cliente, envía una alerta.
    """
    now = datetime.now()
    start_date = (now.replace(day=1) - timedelta(days=1)).replace(
        day=1, hour=0, minute=0, second=0, microsecond=0
    )
    end_date = now.replace(
        day=1, hour=23, minute=59, second=59, microsecond=999999
    ) - timedelta(days=1)

    start_date_str = start_date.strftime("%m/%d/%Y")
    end_date_str = end_date.strftime("%m/%d/%Y")

    clients = get_sheet_names()

    final_message = f"*Resumen mensual de cierres ({start_date_str} - {end_date_str}):* <@U053520KZ4P> <@U07F0LZGA4F>\n"
    send_message = False

    for client in clients:
        df = get_google_sheets_data(client)
        df = df.copy()

        df["Created at (fecha)"] = pd.to_datetime(
            df["Created at (fecha)"], errors="coerce"
        )
        df["Created at (fecha)"] = df["Created at (fecha)"].dt.floor("d")

        df_filtered = df.loc[
            (df["Created at (fecha)"] >= start_date)
            & (df["Created at (fecha)"] <= end_date)
        ]

        if not df_filtered.empty:
            closed = analyze_closed_data(df_filtered)
            closed = closed.loc[closed["Cierres"] != 0]

            if not closed.empty:
                send_message = True
                message = f"*{client}:*\n"
                for _, row in closed.iterrows():
                    leyenda = f" ({row['Leyenda']})" if row["Leyenda"] else ""
                    link = row["Link"] if pd.notna(row["Link"]) else "Sin enlace"
                    video_text = (
                        f"<{link}|Ver video>" if link != "Sin enlace" else "Sin enlace"
                    )
                    message += f"  • {row['Video ID']}{leyenda}, Cierres: *{row['Cierres']}* - {video_text}\n"
                final_message += message + "\n"

    if send_message:
        print(send_slack_notifications(["#creativos-cierres"], final_message))
    else:
        # Enviar alerta si no hubo leads analizables
        alert_message = f"*No hubo leads analizables entre el {start_date_str} y el {end_date_str} para ningún cliente.*"
        print(send_slack_notifications(["#creativos-cierres"], alert_message))


def send_monthly_alerts():
    """
    Envía los resúmenes mensuales de citas y cierres a sus respectivos canales en Slack,
    solo si hay citas o cierres relevantes.
    """
    send_monthly_appointments_alert()
    send_monthly_closed_alert()
