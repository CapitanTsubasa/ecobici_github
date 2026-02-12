"""
ETL de bicicletas rotas – versión refactorizada para Django management command
"""

import os
from io import BytesIO
from datetime import datetime

import numpy as np
import pandas as pd

from django.core.management.base import BaseCommand

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


# ==============================
# CONFIGURACIÓN
# ==============================
SCOPES_DRIVE = ['https://www.googleapis.com/auth/drive']
SCOPES_SHEETS = ['https://www.googleapis.com/auth/spreadsheets']

FOLDER_ID = '15VrRfhgGQdVeOpD2Q_BD2287WCFWif8d'
SPREADSHEET_ID = '1oCi_ayrv98PfIxXllPmQlCG0TDLuiWIozontbIMBURA'
RANGE_PINCHADAS = 'Dock_Bicis!G:H'
RANGE_OUTPUT = 'Bicicletas_Rotas!A1'

POSSIBLE_KEY_PATHS = [
    r'c:\Users\27384244926\python_bicis\client.json',
    r'F:\python_bicis\python_bicis\client.json',
    r'c:\Users\20349069890\python_bicis\client.json',
]


# ==============================
# UTILIDADES
# ==============================

def get_key_path() -> str:
    """Devuelve la primera ruta válida al service account."""
    for path in POSSIBLE_KEY_PATHS:
        if os.path.exists(path):
            return path
    raise FileNotFoundError("No se encontró client.json en ninguna ruta configurada")


def build_drive_service(key_path: str):
    creds = service_account.Credentials.from_service_account_file(
        key_path, scopes=SCOPES_DRIVE
    )
    return build('drive', 'v3', credentials=creds)


def build_sheets_service(key_path: str):
    creds = service_account.Credentials.from_service_account_file(
        key_path, scopes=SCOPES_SHEETS
    )
    return build('sheets', 'v4', credentials=creds)


def download_csv_from_drive(drive_service, filename: str) -> pd.DataFrame:
    """Busca y descarga un CSV desde Google Drive por nombre."""
    query = f"name = '{filename}' and '{FOLDER_ID}' in parents"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])

    if not files:
        raise FileNotFoundError(f"No se encontró {filename} en Drive")

    file_id = files[0]['id']

    request = drive_service.files().get_media(fileId=file_id)
    file_stream = BytesIO()
    downloader = MediaIoBaseDownload(file_stream, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    file_stream.seek(0)
    return pd.read_csv(file_stream, encoding='UTF-16LE')


def read_pinchadas_sheet(sheets_service) -> pd.DataFrame:
    """Lee bicicletas pinchadas desde Google Sheets."""
    result = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=SPREADSHEET_ID, range=RANGE_PINCHADAS)
        .execute()
    )

    values = result.get('values', [])

    if not values:
        return pd.DataFrame(columns=['Bicicleta', 'Pinchada'])

    return pd.DataFrame(values[1:], columns=values[0])


# ==============================
# TRANSFORMACIONES
# ==============================

def transformar_rotas(rotas: pd.DataFrame, estaciones: pd.DataFrame) -> pd.DataFrame:
    cols = [
        'id', 'model', 'msnbc', 'registeredDate', 'station.id', 'station.name',
        'status', 'states', 'location', 'lastCheckUp', 'lastReportDate'
    ]

    rotas = rotas[cols].copy()
    rotas['fecha_ejecucion'] = pd.Timestamp.now()

    rotas['station.id'] = pd.to_numeric(rotas['station.id'], errors='coerce').astype('Int64')
    rotas = rotas.dropna(subset=['station.id'])
    rotas['station.id'] = rotas['station.id'].astype(int)

    estaciones['ID'] = pd.to_numeric(estaciones['ID'], errors='coerce').astype('Int64')

    rotas_estaciones = rotas.merge(
        estaciones,
        left_on='station.id',
        right_on='ID',
        how='left'
    )

    idx_fecha = rotas_estaciones.columns.get_loc('fecha_ejecucion')

    cols_finales = (
        list(rotas_estaciones.columns[: idx_fecha + 1]) + ['Latitud', 'Longitud']
    )

    return rotas_estaciones[cols_finales].copy()


def enriquecer_con_pinchadas(rotas: pd.DataFrame, pinchadas: pd.DataFrame) -> pd.DataFrame:
    df = rotas.merge(
        pinchadas,
        how='left',
        left_on='msnbc',
        right_on='Bicicleta'
    ).drop(columns=['Bicicleta'], errors='ignore')

    df['Pinchada'] = df['Pinchada'].fillna('NO')

    df['lastReportDate'] = pd.to_datetime(df['lastReportDate'], errors='coerce')

    now = pd.Timestamp.now()

    df['duracion_rota_horas'] = (now - df['lastReportDate']).dt.total_seconds() / 3600

    bins = [0, 6, 24, 72, 168, np.inf]
    labels = ['0–6 h', '6–24 h', '1–3 días', '3–7 días', '+7 días']

    df['Categoria_tiempo'] = pd.cut(df['duracion_rota_horas'], bins=bins, labels=labels, right=False)

    return df.drop(columns=['duracion_rota_horas'])


# ==============================
# CARGA A SHEETS
# ==============================

def guardar_en_sheets(sheets_service, df: pd.DataFrame):
    sheet = sheets_service.spreadsheets()

    values = df.astype(str).values.tolist()
    headers = [df.columns.tolist()]

    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_OUTPUT,
        valueInputOption='RAW',
        body={'values': headers}
    ).execute()

    sheet.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_OUTPUT,
        valueInputOption='RAW',
        insertDataOption='INSERT_ROWS',
        body={'values': values}
    ).execute()


# ==============================
# MANAGEMENT COMMAND
# ==============================

class Command(BaseCommand):
    help = 'Ejecuta ETL de bicicletas rotas y actualiza Google Sheets'

    def handle(self, *args, **kwargs):
        self.stdout.write('Iniciando ETL de bicicletas rotas...')

        key_path = get_key_path()

        drive_service = build_drive_service(key_path)
        sheets_service = build_sheets_service(key_path)

        estaciones = download_csv_from_drive(drive_service, 'estaciones_ecobici.csv')
        rotas = download_csv_from_drive(drive_service, 'bicicletas_rotas.csv')

        rotas_proc = transformar_rotas(rotas, estaciones)

        pinchadas = read_pinchadas_sheet(sheets_service)
        rotas_final = enriquecer_con_pinchadas(rotas_proc, pinchadas)

        guardar_en_sheets(sheets_service, rotas_final)

        self.stdout.write(self.style.SUCCESS('ETL completado correctamente.'))
