from wsgiref import headers
import plotly.express as px
import json
import pandas as pd
import os
import unicodedata
import matplotlib.pyplot as plt
import calendar
import csv

from io import BytesIO

from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build

from django.shortcuts import render
from django.conf import settings
from django.http import HttpResponse

from collections import Counter

from urllib3 import request





# Create your views here.
def index(request):
    context = {"mensaje": "Bienvenidos a la pagina de ecobici"}
    return render(request, "inicio/index.html", context)


# PROBANDO MOSTRAR DATOS DE XLSX
def index(request):
    df = pd.read_csv('bicicletas.csv', sep=';', encoding='latin1', on_bad_lines='skip')

    # Normalizar columnas
    df.columns = df.columns.str.strip().str.lower()
    print("Columnas limpias:", list(df.columns))

    conteo_status = df['status'].value_counts().to_dict()
    
    labels = list(conteo_status.keys())
    valores = list(conteo_status.values())

    if 'status' in df.columns:
        print("Valores únicos en 'status':", df['status'].unique())
        print("No nulos en 'status':", df['status'].notna().sum())

        conteo_status = df['status'].value_counts().to_dict()
    else:
        print("⚠️ La columna 'status' no está en el DataFrame después de limpieza.")

    context = {
        'labels': labels,
        'valores': valores
    }

    return render(request, 'inicio/index.html', context)

#PROBANDO SUBIR VIAJES EN OTRO HTML
def viajes(request):
    ruta = r'C:\Users\20349069890\ecobici_github\Viajes.csv'
    df = pd.read_csv(ruta, sep=';', encoding='latin1')

    # Asegurar que 'Mes-Año' esté como string
    df['Mes-Año'] = df['Mes-Año'].astype(str)

    context = {
        'labels': list(df['Mes-Año']),
        'q_viajes': list(df['Q_Viajes']),
        'acumulado': list(df['Acumulado_Viajes']),
        'promedio': list(df['promedio_diario_dia_habil']),
        'usuarios_unicos': list(df['Usuarios_Unicos']),
        'usuarios_registrados': list(df['Usuarios_Registrados'])
    }

    return render(request, 'inicio/viajes.html', context)

print("Columnas reales del CSV:")

"""
####################################################################################################################
############################################   AGREGANDO CACHE   ###################################################
####################################################################################################################    
"""
####################################################################################################################
####################################   PROBANDO EL PROCESADO - MOSTRAR_USUARIOS   ##################################
####################################################################################################################


def mostrar_usuarios(request):
    scopes = ['https://www.googleapis.com/auth/drive']

    rutas = [
        r'C:\Users\27384244926\Documents\Python_GIT\python_bicis\python_bicis\client.json',
        r'F:\python_bicis\python_bicis\client.json',
        r'c:\Users\20349069890\python_bicis\client.json',
        r'c:\Users\20349069890\ecobici_github\client.json'
    ]

    key_path = next((ruta for ruta in rutas if os.path.exists(ruta)), None)
    if not key_path:
        return render(request, "error.html", {"mensaje": "No se encontró el archivo client.json"})

    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=scopes
    )
    drive_service = build('drive', 'v3', credentials=credentials)

    folder_id = '15VrRfhgGQdVeOpD2Q_BD2287WCFWif8d'
    target_file_name = 'Bicicletas_acumulado_procesado_2025.csv'

    query = f"name='{target_file_name}' and '{folder_id}' in parents"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])

    if not files:
        return render(request, "error.html", {"mensaje": "Archivo no encontrado en Drive"})

    file_id = files[0]['id']
    request_drive = drive_service.files().get_media(fileId=file_id)
    file_stream = BytesIO()
    downloader = MediaIoBaseDownload(file_stream, request_drive)

    done = False
    while not done:
        _, done = downloader.next_chunk()
    file_stream.seek(0)

    # ================================
    # 📥 Carga del CSV
    # ================================
    usuarios = pd.read_csv(file_stream, encoding="latin-1", sep="\t")
    usuarios.columns = usuarios.columns.str.strip()

    usuarios['Fecha_Inicio'] = pd.to_datetime(usuarios['Fecha_Inicio'], errors='coerce')
    usuarios = usuarios.dropna(subset=['Fecha_Inicio'])

    # ================================
    # 🎯 Filtro por estación (origen)
    # ================================
    estacion_filtro = request.GET.get('estacion')

    df = usuarios.copy()
    if estacion_filtro:
        df = df[df['Nombre_Inicio_Viaje'] == estacion_filtro]

    # ================================
    # 📊 Viajes por mes
    # ================================
    df['Mes'] = df['Fecha_Inicio'].dt.to_period('M')

    viajes_por_mes = (
        df.groupby('Mes')
        .size()
        .reset_index(name='viajes')
    )
    viajes_por_mes['Mes'] = viajes_por_mes['Mes'].astype(str)

    # ================================
    # 📊 Viajes por origen / destino
    # ================================
    viajes_por_origen = (
        df['Nombre_Inicio_Viaje']
        .value_counts()
        .head(400)
        .to_dict()
    )

    viajes_por_destino = (
        df['Nombre_Final_Viaje']
        .value_counts()
        .head(400)
        .to_dict()
    )

    # ================================
    # 📋 Tabla preview
    # ================================
    tabla_html = df.head(50).to_html(classes="table table-striped", index=False)

    # ================================
    # 📌 Selector de estaciones
    # ================================
    estaciones_origen = sorted(
        usuarios['Nombre_Inicio_Viaje']
        .dropna()
        .unique()
        .tolist()
    )

    # ================================
    # 📦 Contexto
    # ================================
    context = {
        "tabla": tabla_html,
        "viajes_por_origen": viajes_por_origen,
        "viajes_por_destino": viajes_por_destino,
        "meses_labels": viajes_por_mes['Mes'].tolist(),
        "viajes_values": viajes_por_mes['viajes'].tolist(),
        "estaciones_origen": estaciones_origen,
        "estacion_seleccionada": estacion_filtro,
    }

    return render(request, "inicio/usuarios.html", context)

####################################################################################################################
####################################   FIN DE - MOSTRAR_USUARIOS   #################################################
####################################################################################################################





####################################################################################################################
####################################   PROCESADO - DESCARGAS  ######################################################
####################################################################################################################


def descargar_viajes_por_estacion(request):

    # ==============================
    # 🔐 Conexión a Google Drive
    # ==============================
    scopes = ['https://www.googleapis.com/auth/drive']

    rutas = [
        r'C:\Users\27384244926\Documents\Python_GIT\python_bicis\python_bicis\client.json',
        r'F:\python_bicis\python_bicis\client.json',
        r'c:\Users\20349069890\python_bicis\client.json',
        r'c:\Users\20349069890\ecobici_github\client.json'
    ]

    key_path = next((ruta for ruta in rutas if os.path.exists(ruta)), None)
    if not key_path:
        return HttpResponse("No se encontró client.json", status=500)

    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=scopes
    )

    drive_service = build('drive', 'v3', credentials=credentials)

    # ==============================
    # 📂 Buscar archivo en Drive
    # ==============================
    folder_id = '15VrRfhgGQdVeOpD2Q_BD2287WCFWif8d'
    target_file_name = 'Bicicletas_acumulado_procesado_2025.csv'

    query = f"name='{target_file_name}' and '{folder_id}' in parents"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])

    if not files:
        return HttpResponse("Archivo no encontrado en Drive", status=404)

    file_id = files[0]['id']

    # ==============================
    # ⬇️ Descargar archivo en memoria
    # ==============================
    request_drive = drive_service.files().get_media(fileId=file_id)
    file_stream = BytesIO()
    downloader = MediaIoBaseDownload(file_stream, request_drive)

    done = False
    while not done:
        status, done = downloader.next_chunk()

    file_stream.seek(0)

    # ==============================
    # 📊 Leer CSV en pandas
    # ==============================
    #df = pd.read_csv(file_stream, encoding="utf-8", sep="\t")
    df = pd.read_csv(file_stream, sep="\t", encoding="latin-1",   # <- lectura segura (no rompe nunca)
    on_bad_lines="skip")   # <- evita que una fila corrupta rompa todo

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.encode("latin-1", errors="ignore").str.decode("utf-8", errors="ignore")

    # ==============================
    # 🚲 Agrupar viajes por estación
    # ==============================
    viajes_por_estacion = (
        df.groupby("Nombre_Inicio_Viaje")
        .size()
        .reset_index(name="Cantidad_de_viajes")
        .sort_values("Cantidad_de_viajes", ascending=False)
    )

    # ==============================
    # 📥 Generar descarga CSV
    # ==============================
    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = 'attachment; filename="viajes_por_estacion.csv"'

    viajes_por_estacion.to_csv(response, index=False, encoding="utf-8-sig")

    return response


####################################################################################################################
####################################  FIN DE PROCESADO - DESCARGAS  ################################################
####################################################################################################################


# GRAFICO INTERACTIVO DE PRODUCTOS
def grafico_productos_interactivo(request):
    scopes = ['https://www.googleapis.com/auth/drive']

    # Lista de posibles rutas de client.json
    rutas = [
        r'C:\Users\27384244926\Documents\Python_GIT\python_bicis\python_bicis\client.json',
        r'F:\python_bicis\python_bicis\client.json',
        r'c:\Users\20349069890\python_bicis\client.json',
        r'c:\Users\20349069890\ecobici_github\client.json'
    ]

    key_path = next((ruta for ruta in rutas if os.path.exists(ruta)), None)
    if not key_path:
        return render(request, "error.html", {"mensaje": "No se encontró el archivo client.json"})

    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=scopes
    )
    drive_service = build('drive', 'v3', credentials=credentials)

    folder_id = '15VrRfhgGQdVeOpD2Q_BD2287WCFWif8d'
    target_file_name = 'Bicicletas_acumulado_procesado_2025.csv'

    # Buscar el archivo
    query = f"name = '{target_file_name}' and '{folder_id}' in parents"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])

    if not files:
        return render(request, "error.html", {"mensaje": "Archivo no encontrado en Drive"})

    file_id = files[0]['id']

    # Descargar el archivo
    request_drive = drive_service.files().get_media(fileId=file_id)
    file_stream = BytesIO()
    downloader = MediaIoBaseDownload(file_stream, request_drive)

    done = False
    while not done:
        status, done = downloader.next_chunk()
    file_stream.seek(0)

    # Cargar CSV en pandas
    df = pd.read_csv(file_stream, encoding="latin-1", sep="\t")  # o sep="\t" si es tabulado

    # Contar casos por producto
    conteo_productos = df['Nombre_de_producto'].value_counts()

    # Convertir a diccionario para JS / gráfico
    labels = list(conteo_productos.index)
    values = list(conteo_productos.values)

    context = {
        "labels": labels,
        "values": values
    }

    return render(request, "inicio/grafico_productos.html", context)


def grafico_productos(request):
    scopes = ['https://www.googleapis.com/auth/drive']

    rutas = [
        r'C:\Users\27384244926\Documents\Python_GIT\python_bicis\python_bicis\client.json',
        r'F:\python_bicis\python_bicis\client.json',
        r'c:\Users\20349069890\python_bicis\client.json',
        r'c:\Users\20349069890\ecobici_github\client.json'
    ]

    key_path = next((ruta for ruta in rutas if os.path.exists(ruta)), None)
    if not key_path:
        return render(request, "error.html", {"mensaje": "No se encontró el archivo client.json"})

    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=scopes
    )
    drive_service = build('drive', 'v3', credentials=credentials)

    folder_id = '15VrRfhgGQdVeOpD2Q_BD2287WCFWif8d'
    target_file_name = 'Bicicletas_acumulado_procesado_2025.csv'

    # Buscar el archivo
    query = f"name='{target_file_name}' and '{folder_id}' in parents"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])

    if not files:
        return render(request, "error.html", {"mensaje": "Archivo no encontrado en Drive"})

    file_id = files[0]['id']

    # Descargar el archivo
    request_drive = drive_service.files().get_media(fileId=file_id)
    file_stream = BytesIO()
    downloader = MediaIoBaseDownload(file_stream, request_drive)

    done = False
    while not done:
        status, done = downloader.next_chunk()
    file_stream.seek(0)

    # Cargar CSV en pandas
    df = pd.read_csv(file_stream, encoding="latin-1", sep="\t")

    # Normalizar texto
    def normalizar(texto):
        texto = str(texto).lower()
        texto = unicodedata.normalize("NFD", texto)
        texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
        return texto.strip()

    df["Nombre_normalizado"] = df["Nombre_de_producto"].apply(normalizar)

    # Filtrar todos los que tengan la palabra "basico"
    df_filtrado = df[~df["Nombre_normalizado"].str.contains("basico")]

    # Contar viajes por pase
    conteo_pases = df_filtrado["Nombre_de_producto"].value_counts()

    labels = list(conteo_pases.index)
    values = [int(v) for v in conteo_pases.values]

    # 👉 IMPORTANTE: devolver la respuesta
    return render(request, "inicio/grafico_productos.html", {
        "labels": json.dumps(labels),
        "values": json.dumps(values)
    })



####################################################################################################################
####################################   # MOTIVOS.HTML - DESAPARECIDAS.CSV    ##################################
####################################################################################################################


  

def dashboard(request):

    # ======== LEER DATOS DESDE GOOGLE SHEETS =========
    key_path = r'c:\Users\20349069890\ecobici_github\client.json'
    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    credentials = service_account.Credentials.from_service_account_file(key_path, scopes=scopes)
    sheets_service = build('sheets', 'v4', credentials=credentials)

    id_sheet = '1ZgtDX-VWm3jDiGH4NvpaWwJQIlonH7lHGyLFadRqhiU'
    range_name = 'BICICLETAS!A:BU'

    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=id_sheet,
        range=range_name
    ).execute()

    values = result.get('values', [])
    if not values:
        return HttpResponse("No hay datos en el Google Sheet")

    headers = values[0]
    rows = values[1:]

    # Normalizar filas al largo del header
    num_cols = len(headers)

    rows_normalizadas = [
        row[:num_cols] + [''] * (num_cols - len(row))
        for row in rows
    ]
    df = pd.DataFrame(rows_normalizadas, columns=headers)

    # df = pd.DataFrame(values[1:], columns=values[0])

    # ======== PROCESAR FECHAS =========
    df['FECHA DE VIAJE'] = pd.to_datetime(df['FECHA DE VIAJE'], errors='coerce', dayfirst=True)
    df['MES'] = df['FECHA DE VIAJE'].dt.to_period('M').astype(str)
    df['DIA_SEMANA'] = df['FECHA DE VIAJE'].dt.weekday   # 0=Lunes
    df['DIA_SEMANA_NOMBRE'] = df['FECHA DE VIAJE'].dt.day_name()

    # ======== CAPTURAR FILTROS =========
    fecha_filtro = request.GET.get("fecha")   # YYYY-MM-DD
    mes_filtro = request.GET.get("mes")       # YYYY-MM
    dia_filtro = request.GET.get("dia")       # 0–6

    # ======== APLICAR FILTROS =========
    df_filtrado = df.copy()

    # FILTRO POR FECHA EXACTA
    if fecha_filtro:
        fecha_dt = pd.to_datetime(fecha_filtro).date()
        df_filtrado = df_filtrado[df_filtrado['FECHA DE VIAJE'].dt.date == fecha_dt]

    # FILTRO POR MES
    if mes_filtro:
        df_filtrado = df_filtrado[df_filtrado['MES'] == mes_filtro]

    # FILTRO POR DÍA DE SEMANA
    if dia_filtro:
        df_filtrado = df_filtrado[df_filtrado['DIA_SEMANA'] == int(dia_filtro)]

    # ========================================================
    #            REGENERAR TODAS LAS MÉTRICAS FILTRADAS
    # ========================================================

    # MOTIVOS
    conteo_motivos = df_filtrado['MOTIVO'].value_counts().to_dict()

    # CASOS POR MES
    conteo_mes = (
        df_filtrado.groupby(df_filtrado['FECHA DE VIAJE'].dt.to_period('M'))
        .size()
        .reset_index(name='cantidad')
    )
    conteo_mes['mes'] = conteo_mes['FECHA DE VIAJE'].astype(str)

    # DIA DE SEMANA
    conteo_dia = df_filtrado['DIA_SEMANA_NOMBRE'].value_counts().reset_index()
    conteo_dia.columns = ['dia', 'cantidad']

    # VANDALISMO
    # df_filtrado['MOTIVO'] = df_filtrado['MOTIVO'].astype(str).str.upper().str.strip()
    df_filtrado['MOTIVO'] = (
    df_filtrado['MOTIVO']
    .astype(str)
    .str.upper()
    .str.strip()
    )
    #df['FECHA ROBADA'] = pd.to_datetime(df['FECHA ROBADA'], errors='coerce', dayfirst=True)

    df['FECHA DE VIAJE'] = pd.to_datetime(df['FECHA DE VIAJE'], errors='coerce', dayfirst=True)
    df['FECHA ROBADA'] = pd.to_datetime(df['FECHA ROBADA'], errors='coerce', dayfirst=True)
    df['FECHA RECUPERADA'] = pd.to_datetime(df['FECHA RECUPERADA'], errors='coerce', dayfirst=True)

    mes_filtro = request.GET.get("mes")

    df_vandalismo = df_filtrado[
        df_filtrado['MOTIVO'].str.contains(r'\bVANDALISMO', na=False, regex=True)
    ]

    conteo_vandalismo = (
        df_vandalismo.groupby(df_vandalismo['FECHA DE VIAJE'].dt.to_period('M'))
        .size()
        .reset_index(name='cantidad')
    )
    conteo_vandalismo['mes'] = conteo_vandalismo['FECHA DE VIAJE'].astype(str)

    # CONTADORES
    casos_espera = df_filtrado[df_filtrado['ESTADO ACTUALIZADO'] == 'A LA ESPERA'].shape[0]
    casos_robada = df_filtrado[df_filtrado['ESTADO ACTUALIZADO'].isin(['ROBADA', 'ROBADA - RECUPERADA'])].shape[0]
    casos_robada_recuperada = df_filtrado[df_filtrado['ESTADO ACTUALIZADO'] == 'ROBADA - RECUPERADA'].shape[0]
    comisaria = df_filtrado[df_filtrado['ESTADO ACTUALIZADO'] == 'COMISARIA'].shape[0]
    vandalismo_total = df_filtrado[df_filtrado['MOTIVO'].isin(['BICICLETA VANDALIZADA', 'VANDALISMO PINO CORTADO', 'VANDALISMO-DOCK'])].shape[0]

    # ======== LISTAS PARA SELECTORES =========
    meses_unicos = sorted(df['MES'].dropna().unique(), reverse=True)
    dias_unicos = list(range(0, 7))

    # ============================
    # DATASET EXCLUSIVO DE ROBOS
    # ============================

    #df_robos = df_filtrado[
    #    df_filtrado['ESTADO ACTUALIZADO'].isin(['ROBADA', 'ROBADA - RECUPERADA']) &
    #    df_filtrado['FECHA ROBADA'].notna()
    #].copy()

    #conteo_mes = agrupar_por_mes(df_filtrado, 'FECHA DE VIAJE')
    #conteo_robos_mes = agrupar_por_mes(df_robos, 'FECHA ROBADA')

    df_robos = df[
        df['ESTADO ACTUALIZADO'].isin(['ROBADA', 'ROBADA - RECUPERADA']) &
        df['FECHA ROBADA'].notna()
    ].copy()

    if mes_filtro:
        df_robos = df_robos[
            df_robos['FECHA ROBADA'].dt.to_period('M').astype(str) == mes_filtro
        ]

    conteo_robos_mes = agrupar_por_mes(df_robos, 'FECHA ROBADA')

    # ============================
    # DATASET EXCLUSIVO DE RECUPEROS
    # ============================

    #df['FECHA RECUPERADA'] = pd.to_datetime(
    #df['FECHA RECUPERADA'], errors='coerce', dayfirst=True
    #)

    #df_recuperos = df_filtrado[
    #    (df_filtrado['ESTADO ACTUALIZADO'] == 'ROBADA - RECUPERADA') &
    #    (df_filtrado['FECHA RECUPERADA'].notna())
    #].copy()

    #conteo_recuperos_mes = agrupar_por_mes(df_recuperos, 'FECHA RECUPERADA')


    #if mes_filtro:
    #    df = df[
    #        df['FECHA DE VIAJE'].dt.to_period('M').astype(str) == mes_filtro
    #    ]

    df_recuperos = df[
        (df['ESTADO ACTUALIZADO'] == 'ROBADA - RECUPERADA') &
        (df['FECHA RECUPERADA'].notna())
    ].copy()

    if mes_filtro:
        df_recuperos = df_recuperos[
            df_recuperos['FECHA RECUPERADA'].dt.to_period('M').astype(str) == mes_filtro
        ]

    conteo_recuperos_mes = agrupar_por_mes(df_recuperos, 'FECHA RECUPERADA')

    # ============================
    # PARSEAR COORDENADAS GPS (FORMATO REAL DEL SHEET)
    # ============================

    df['Ultima coordenada de GPS'] = df['Ultima coordenada de GPS'].astype(str)

    coords = df['Ultima coordenada de GPS'].str.extract(
        r'(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)'
    )

    df['lat'] = pd.to_numeric(coords[0], errors='coerce')
    df['lng'] = pd.to_numeric(coords[1], errors='coerce')
    
    df_mapa = df[
        df['lat'].notna() &
        df['lng'].notna() &
        df['ESTADO ACTUALIZADO'].isin(['ROBADA', 'ROBADA - RECUPERADA'])
    ].copy()

    puntos_gps = df_mapa[['lat', 'lng']].to_dict(orient='records')

    #df_mapa = df_filtrado[
    #    df_filtrado['lat'].notna() &
    #    df_filtrado['lng'].notna()
    #].copy()
        #df['ESTADO ACTUALIZADO'].isin(['ROBADA', 'ROBADA - RECUPERADA']) &
   

    

    #context['puntos_gps'] = json.dumps(puntos_gps) PENDIENTE PARA ARREGLAR NO SE VE EL MAPA.

    print("Puntos GPS:", len(puntos_gps))
    print(df_mapa[['Ultima coordenada de GPS', 'lat', 'lng']].head())

    # ======== CONTEXTO =========
    context = {
        'conteno_motivos': conteo_motivos,
        'cant_meses': list(conteo_mes['cantidad']),
        'meses': list(conteo_mes['mes']),

    # Robos
        'meses_robos': list(conteo_robos_mes['mes']),
        'cant_robos': list(conteo_robos_mes['cantidad']),

        'dias': list(conteo_dia['dia']),
        'cant_dias': list(conteo_dia['cantidad']),

        'casos_espera': casos_espera,
        'casos_robada': casos_robada,
        'casos_robada_recuperada': casos_robada_recuperada,
        'comisaria': comisaria,
        'vandalismo': vandalismo_total,

        'meses_unicos': meses_unicos,
        'mes_actual': mes_filtro,
        'fecha_actual': fecha_filtro,
        'dia_actual': dia_filtro,

    # Motivos
        'motivos': list(df_filtrado['MOTIVO'].value_counts().index),
        'cant_motivos': list(df_filtrado['MOTIVO'].value_counts().values),

    # Vandalismo
        'meses_vandalismo': list(conteo_vandalismo['mes']),
        'cant_vandalismo': list(conteo_vandalismo['cantidad']),
    
    # Recuperos
        'meses_recuperos': list(conteo_recuperos_mes['mes']),
        'cant_recuperos': list(conteo_recuperos_mes['cantidad']),

    # ✅ MAPA
        'puntos_gps': json.dumps(puntos_gps),
    }

    return render(request, 'inicio/motivos.html', context)





# ================================================== AGRUPAR POR MES ==================================================


def agrupar_por_mes(df, columna_fecha):
    tmp = df.copy()

    tmp[columna_fecha] = pd.to_datetime(
        tmp[columna_fecha],
        errors='coerce',
        dayfirst=True
    )

    conteo = (
        tmp
        .dropna(subset=[columna_fecha])
        .groupby(tmp[columna_fecha].dt.to_period('M'))
        .size()
        .reset_index(name='cantidad')
    )

    conteo['mes'] = conteo[columna_fecha].astype(str)
    return conteo
    






# FIN MOTIVOS.HTML - DESAPARECIDAS.CSV


def contar_viajes(df):
    """
    Cuenta la cantidad de viajes en el DataFrame y devuelve
    un string formateado (ej: '2,2 M' o '125.430').
    """
    try:
        total_viajes = len(df)

        if total_viajes >= 1_000_000:
            # 1.2 M -> usar coma decimal como en tus ejemplos: "2,2 M"
            display_value = f"{total_viajes / 1_000_000:.1f} M".replace(".", ",")
        elif total_viajes >= 1_000:
            # 125430 -> "125.430"
            display_value = f"{total_viajes:,}".replace(",", ".")
        else:
            display_value = str(total_viajes)

        return display_value
    except Exception as e:
        # Opcional: loguear o imprimir para debug
        print("Error en contar_viajes:", e)
        return "Error"


def descargar_ultimo_uso(request):
    from googleapiclient.discovery import build
    from google.oauth2 import service_account
    from googleapiclient.http import MediaIoBaseDownload
    from io import BytesIO
    import pandas as pd
    import os
    from django.http import HttpResponse

    # --- MISMO CÓDIGO QUE EN mostrar_usuarios PARA CONECTAR A DRIVE ---
    scopes = ['https://www.googleapis.com/auth/drive']

    rutas = [
        r'C:\Users\27384244926\Documents\Python_GIT\python_bicis\python_bicis\client.json',
        r'F:\python_bicis\python_bicis\client.json',
        r'c:\Users\20349069890\python_bicis\client.json',
        r'c:\Users\20349069890\ecobici_github\client.json'
    ]

    key_path = next((ruta for ruta in rutas if os.path.exists(ruta)), None)
    if not key_path:
        return HttpResponse("No se encontró el archivo client.json.", content_type="text/plain")

    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=scopes
    )
    drive_service = build('drive', 'v3', credentials=credentials)

    folder_id = '15VrRfhgGQdVeOpD2Q_BD2287WCFWif8d'
    target_file_name = 'Bicicletas_acumulado_procesado_2025.csv'

    query = f"name='{target_file_name}' and '{folder_id}' in parents"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])

    if not files:
        return HttpResponse("Archivo no encontrado en Drive.", content_type="text/plain")

    file_id = files[0]['id']
    request_drive = drive_service.files().get_media(fileId=file_id)
    file_stream = BytesIO()
    downloader = MediaIoBaseDownload(file_stream, request_drive)

    done = False
    while not done:
        status, done = downloader.next_chunk()
    file_stream.seek(0)

    # --- Cargar CSV a pandas ---
    usuarios = pd.read_csv(file_stream, encoding="latin-1", sep="\t")

    print("COLUMNAS DEL CSV:")
    print(usuarios.columns.tolist())

    # normalizar nombres
    usuarios.columns = usuarios.columns.str.strip()

    # buscar cualquier columna relacionada a bicicleta
    col_bici = next(
        (c for c in usuarios.columns if 'bici' in c.lower() or 'bike' in c.lower() or 'msnbc' in c.lower()),
        None
    )

    print("Columna de bicicleta detectada:", col_bici)

    # --- Asegurar tipo de fecha ---
    usuarios['Fecha_Inicio'] = pd.to_datetime(usuarios['Fecha_Inicio'], errors='coerce')
    usuarios = usuarios.dropna(subset=['Fecha_Inicio'])

    # --- Calcular último uso por bicicleta ---
    if 'Msnbc_de_bicicleta' not in usuarios.columns or 'Fecha_Inicio' not in usuarios.columns:
        return HttpResponse("No se encontraron las columnas necesarias.", content_type="text/plain")

    ultimo_uso = (
        usuarios.groupby('Msnbc_de_bicicleta')['Fecha_Inicio']
        .max()
        .reset_index()
        .rename(columns={'Fecha_Inicio': 'Ultimo_Uso'})
        .sort_values('Ultimo_Uso', ascending=False)
    )

    # --- Generar respuesta CSV ---
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename=\"ultimo_uso_bicicletas.csv\"'
    ultimo_uso.to_csv(path_or_buf=response, index=False)

    print(usuarios.columns)


    return response
