from django.core.management.base import BaseCommand
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
from io import BytesIO
import pandas as pd
import numpy as np
import os


class Command(BaseCommand):
    help = "ETL diario de bicicletas rotas"

    def handle(self, *args, **kwargs):

        # =========================
        # 1. CREDENCIALES
        # =========================
        rutas = [
            r'c:\Users\27384244926\python_bicis\client.json',
            r'F:\python_bicis\python_bicis\client.json',
            r'c:\Users\20349069890\python_bicis\client.json'
        ]

        key_path = next((r for r in rutas if os.path.exists(r)), None)

        if not key_path:
            raise FileNotFoundError("No se encontró client.json")

        creds = service_account.Credentials.from_service_account_file(
            key_path,
            scopes=['https://www.googleapis.com/auth/drive',
                    'https://www.googleapis.com/auth/spreadsheets']
        )

        drive_service = build('drive', 'v3', credentials=creds)
        sheets_service = build('sheets', 'v4', credentials=creds)

        folder_id = '15VrRfhgGQdVeOpD2Q_BD2287WCFWif8d'

        # =========================
        # 2. FUNCION DESCARGA CSV DRIVE
        # =========================
        def descargar_csv(nombre_archivo, encoding="UTF-16LE"):

            query = f"name='{nombre_archivo}' and '{folder_id}' in parents"
            results = drive_service.files().list(q=query, fields="files(id, name)").execute()
            files = results.get('files', [])

            if not files:
                raise FileNotFoundError(f"No se encontró {nombre_archivo} en Drive")

            file_id = files[0]['id']

            request = drive_service.files().get_media(fileId=file_id)
            file_stream = BytesIO()
            downloader = MediaIoBaseDownload(file_stream, request)

            done = False
            while not done:
                _, done = downloader.next_chunk()

            file_stream.seek(0)
            return pd.read_csv(file_stream, encoding=encoding)

        # =========================
        # 3. CARGA DATA
        # =========================
        estaciones = descargar_csv("estaciones_ecobici.csv")
        rotas = descargar_csv("bicicletas_rotas.csv")

        cols = [
            'id', 'model', 'msnbc', 'registeredDate',
            'station.id', 'station.name', 'status', 'states',
            'location', 'lastCheckUp', 'lastReportDate'
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
        cols_finales = list(rotas_estaciones.columns[:idx_fecha + 1]) + ['Latitud', 'Longitud']
        rotas_final = rotas_estaciones[cols_finales].copy()

        # =========================
        # 4. LEER SHEET PINCHADAS
        # =========================
        sheet_id = '1oCi_ayrv98PfIxXllPmQlCG0TDLuiWIozontbIMBURA'
        rango = 'Dock_Bicis!G:H'

        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=rango
        ).execute()

        values = result.get('values', [])

        bicicletas_pinchadas = pd.DataFrame(values[1:], columns=values[0]) if values else pd.DataFrame()

        # =========================
        # 5. ENRIQUECER
        # =========================
        rotas_enriquecido = rotas_final.merge(
            bicicletas_pinchadas,
            how='left',
            left_on='msnbc',
            right_on='Bicicleta'
        )

        if 'Bicicleta' in rotas_enriquecido.columns:
            rotas_enriquecido = rotas_enriquecido.drop(columns=['Bicicleta'])

        rotas_enriquecido['Pinchada'] = rotas_enriquecido['Pinchada'].fillna('NO')

        rotas_enriquecido['lastReportDate'] = pd.to_datetime(
            rotas_enriquecido['lastReportDate'],
            errors='coerce'
        )

        now = pd.Timestamp.now()

        rotas_enriquecido['duracion_rota_horas'] = (
            now - rotas_enriquecido['lastReportDate']
        ).dt.total_seconds() / 3600

        bins = [0, 6, 24, 72, 168, np.inf]
        labels = ['0–6 h', '6–24 h', '1–3 días', '3–7 días', '+7 días']

        rotas_enriquecido['Categoria_tiempo'] = pd.cut(
            rotas_enriquecido['duracion_rota_horas'],
            bins=bins,
            labels=labels,
            right=False
        )

        rotas_enriquecido = rotas_enriquecido.drop(columns=['duracion_rota_horas'])

        # =========================
        # 6. GUARDAR EN SHEETS
        # =========================
        values = rotas_enriquecido.astype(str).values.tolist()
        headers = [rotas_enriquecido.columns.tolist()]

        sheet = sheets_service.spreadsheets()

        sheet.values().update(
            spreadsheetId=sheet_id,
            range='Bicicletas_Rotas!A1',
            valueInputOption='RAW',
            body={'values': headers}
        ).execute()

        sheet.values().append(
            spreadsheetId=sheet_id,
            range='Bicicletas_Rotas!A1',
            valueInputOption='RAW',
            insertDataOption='INSERT_ROWS',
            body={'values': values}
        ).execute()

        self.stdout.write(self.style.SUCCESS("ETL de rotas ejecutado correctamente"))
