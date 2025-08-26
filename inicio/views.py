import os
from django.shortcuts import render
import pandas as pd
from collections import Counter

# Nuevo
from django.shortcuts import render
from io import BytesIO
import plotly.express as px
import json

from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload

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


def motivos_desaparecidas(request):
    ruta = r'C:\Users\20349069890\ecobici_github\PP_DESAPARECIDAS.csv'
    df = pd.read_csv(ruta, sep=';', encoding='latin1')

    # Agrupar y contar por motivo
    conteo = df['MOTIVO'].value_counts().reset_index()
    conteo.columns = ['motivo', 'cantidad']

    # Pasar los datos al template
    context = {
        'motivos': list(conteo['motivo']),
        'cantidades': list(conteo['cantidad']),
    }
    return render(request, 'inicio/motivos.html', context)

#PROBANDO EL PROCESADO
def mostrar_usuarios(request):
    scopes = ['https://www.googleapis.com/auth/drive']

    rutas = [
        r'C:\Users\27384244926\Documents\Python_GIT\python_bicis\python_bicis\client.json',
        r'F:\python_bicis\python_bicis\client.json',
        r'c:\Users\20349069890\python_bicis\client.json'
    ]

    key_path = next((ruta for ruta in rutas if os.path.exists(ruta)), None)
    if not key_path:
        return render(request, "error.html", {"mensaje": "No se encontró el archivo client.json"})

    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=scopes
    )
    drive_service = build('drive', 'v3', credentials=credentials)

    folder_id = '145SHi-USegx0Ch2RIPzNXm96bHfN_rQu'
    target_file_name = 'Bicicletas_acumulado_procesado_2025.csv'

    # Buscar el archivo
    query = f"name= '{target_file_name}' and '{folder_id}' in parents"
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

    # Cargar en pandas
    usuarios = pd.read_csv(file_stream, encoding="latin-1", sep="\t")

    # Convertir a tabla HTML
    tabla_html = usuarios.head(50).to_html(classes="table table-striped", index=False)

    return render(request, "inicio/usuarios.html", {"tabla": tabla_html})

# GRAFICO INTERACTIVO DE PRODUCTOS
def grafico_productos_interactivo(request):
    scopes = ['https://www.googleapis.com/auth/drive']

    # Lista de posibles rutas de client.json
    rutas = [
        r'C:\Users\27384244926\Documents\Python_GIT\python_bicis\python_bicis\client.json',
        r'F:\python_bicis\python_bicis\client.json',
        r'c:\Users\20349069890\python_bicis\client.json'
        r'c:\Users\20349069890\ecobici_github\client.json'
    ]

    key_path = next((ruta for ruta in rutas if os.path.exists(ruta)), None)
    if not key_path:
        return render(request, "error.html", {"mensaje": "No se encontró el archivo client.json"})

    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=scopes
    )
    drive_service = build('drive', 'v3', credentials=credentials)

    folder_id = '145SHi-USegx0Ch2RIPzNXm96bHfN_rQu'
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
        r'c:\Users\20349069890\python_bicis\client.json'
    ]

    key_path = next((ruta for ruta in rutas if os.path.exists(ruta)), None)
    if not key_path:
        return render(request, "error.html", {"mensaje": "No se encontró el archivo client.json"})

    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=scopes
    )
    drive_service = build('drive', 'v3', credentials=credentials)

    folder_id = '145SHi-USegx0Ch2RIPzNXm96bHfN_rQu'
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

    # Contar viajes por pase
    conteo_pases = df["Nombre_de_producto"].value_counts()

    labels = list(conteo_pases.index)
    # convertimos los valores a int nativo
    values = [int(v) for v in conteo_pases.values]

    return render(request, "inicio/grafico_productos.html", {
        "labels": json.dumps(labels),
        "values": json.dumps(values)
    })