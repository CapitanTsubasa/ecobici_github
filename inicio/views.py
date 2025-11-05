import plotly.express as px
import json
import pandas as pd
import pickle, time
import os
import unicodedata
import matplotlib.pyplot as plt

from io import BytesIO
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
from django.shortcuts import render
from django.conf import settings
from collections import Counter

import pandas as pd
import pickle
import os

CACHE_PATH = "cache_usuarios.pkl"


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

####################################################################################################################
############################################   AGREGANDO CACHE   #############################################
####################################################################################################################

def get_usuarios(file_stream, target_file_name):
    
    """
    Devuelve el DataFrame de usuarios.
    Si existe un archivo cache (pkl) reciente, lo usa.
    Si no, lee el CSV descargado, guarda el cache y lo devuelve.
    """
    # Si el cache existe y fue creado hace menos de 1 hora → usarlo
    if os.path.exists(CACHE_PATH) and (time.time() - os.path.getmtime(CACHE_PATH) < 3600):
        with open(CACHE_PATH, "rb") as f:
            return pickle.load(f)
    else:
        # Guardar temporalmente el archivo descargado
        temp_path = os.path.join(os.getcwd(), target_file_name)
        with open(temp_path, "wb") as f:
            f.write(file_stream.read())

        # Cargar CSV y guardar en cache
        df = pd.read_csv(temp_path, encoding="latin-1", sep="\t")
        with open(CACHE_PATH, "wb") as f:
            pickle.dump(df, f)

        return df
    

####################################################################################################################
############################################   PROBANDO EL PROCESADO   #############################################
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

    query = f"name= '{target_file_name}' and '{folder_id}' in parents"
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
        status, done = downloader.next_chunk()
    file_stream.seek(0)

    # Cargar en pandas
    ############################usuarios = pd.read_csv(file_stream, encoding="latin-1", sep="\t")#############################
    usuarios = get_usuarios(file_stream, target_file_name)


    # ================================
    # 📊 Conteo por sexo
    # ================================
    sexo_counts = usuarios['Sexo'].value_counts().to_dict()

    # ================================
    # 📊 Conteo por mes
    # ================================
    usuarios['Fecha_Inicio'] = pd.to_datetime(usuarios['Fecha_Inicio'], errors='coerce')
    usuarios['Mes'] = usuarios['Fecha_Inicio'].dt.to_period('M')
    viajes_por_mes = usuarios['Mes'].value_counts().sort_index().to_dict()

    # Fecha actual AGREGANDO CONTEO MENSUAL Y GRAFICO
    fecha_actual = pd.Timestamp.now()
    mes_actual = fecha_actual.to_period('M')

    # Cantidad de viajes del mes actual
    viajes_mes_actual = usuarios[usuarios['Mes'] == mes_actual].shape[0]

    # Porcentaje respecto al total anual
    total_anual = usuarios.shape[0]
    porcentaje_mes = (viajes_mes_actual / total_anual) * 100 if total_anual > 0 else 0

    # ================================
    # Tabla de preview
    # ================================
    tabla_html = usuarios.head(50).to_html(classes="table table-striped", index=False)

    # ================================
    # 📊 Gráfico 3: Viajes por estación de origen
    # ================================
    viajes_por_origen = usuarios['Nombre_Inicio_Viaje'].value_counts().head(10)  # Top 10

    # Pasar a dict para usar en Chart.js
    viajes_por_origen_dict = viajes_por_origen.to_dict()

    # ================================
    # 📊 Gráfico 4: Viajes por destino
    # ================================
    viajes_por_destino = usuarios['Nombre_Final_Viaje'].value_counts().head(10)

    # ================================
    # 📊 Gráfico 45: Viajes por destino
    # ================================

    # Pasar a dict para usar en Chart.js
    viajes_por_destino_dict = viajes_por_destino.to_dict()

    # Asegurar orden cronológico
    viajes_por_mes_ordenado = dict(sorted(viajes_por_mes.items()))

    meses_labels = [str(mes) for mes in viajes_por_mes_ordenado.keys()]
    viajes_values = list(viajes_por_mes_ordenado.values())

    # Guardar temporalmente el archivo descargado
    #temp_path = os.path.join(os.getcwd(), target_file_name)
    #with open(temp_path, "wb") as f:
    #    f.write(file_stream.read())

    temp_path = os.path.join(os.getcwd(), target_file_name)
    with open(temp_path, "wb") as f:
        f.write(file_stream.read())

    return render(request, "inicio/usuarios.html", {
        "tabla": tabla_html,
        "sexo_counts": sexo_counts,
        "viajes_mes": viajes_por_mes,
        "viajes_por_origen": viajes_por_origen_dict,
        "viajes_por_destino": viajes_por_destino_dict,
        "viajes_acumulados": contar_viajes(usuarios),
        "viajes_mes_actual": viajes_mes_actual,
        "porcentaje_mes": porcentaje_mes,
        "meses_labels": meses_labels,
        "viajes_values": viajes_values
    })





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






# MOTIVOS.HTML - DESAPARECIDAS.CSV   

def dashboard(request):
    ruta = r'C:\Users\20349069890\ecobici_github\PP_DESAPARECIDAS.csv'
    df = pd.read_csv(ruta, sep=';', encoding='latin1')

    # --- Gráfico 1: Motivos ---
    conteo_motivos = df['MOTIVO'].value_counts().reset_index()
    conteo_motivos.columns = ['motivo', 'cantidad']

    # --- Gráfico 2: Casos por Mes ---
    df['FECHA DE VIAJE'] = pd.to_datetime(df['FECHA DE VIAJE'], errors='coerce', dayfirst=True)
    conteo_mes = df.groupby(df['FECHA DE VIAJE'].dt.to_period('M')).size().reset_index(name='cantidad')
    conteo_mes['mes'] = conteo_mes['FECHA DE VIAJE'].astype(str)

    # --- Gráfico 3: Casos por Día de Semana ---
    df['DIA_SEMANA'] = df['FECHA DE VIAJE'].dt.day_name()
    conteo_dia = df['DIA_SEMANA'].value_counts().reset_index()
    conteo_dia.columns = ['dia', 'cantidad']

    # --- Grafico Aseguramos mayúsculas y limpiamos espacios ---
    df['MOTIVO'] = df['MOTIVO'].astype(str).str.upper().str.strip()
    df['FECHA DE VIAJE'] = pd.to_datetime(df['FECHA DE VIAJE'], errors='coerce')

    # Filtramos todos los que contengan VANDALISMO, sin importar lo que siga
    df_vandalismo = df[df['MOTIVO'].str.contains(r'\bVANDALISMO', na=False, regex=True)]

    # Agrupamos por mes
    conteo_vandalismo = (
        df_vandalismo.groupby(df_vandalismo['FECHA DE VIAJE'].dt.to_period('M'))
        .size()
        .reset_index(name='cantidad')
    )
    conteo_vandalismo['mes'] = conteo_vandalismo['FECHA DE VIAJE'].astype(str)

    context = {
        # otros datos...
        'meses_vandalismo': list(conteo_vandalismo['mes']),
        'cant_vandalismo': list(conteo_vandalismo['cantidad']),
    }
    print(df_vandalismo['MOTIVO'].unique())
    print(len(df_vandalismo))

    # --- Contador de casos "A LA ESPERA" ---
    casos_espera = df[df['ESTADO ACTUALIZADO'] == 'A LA ESPERA'].shape[0]
    casos_robada = df[df['ESTADO ACTUALIZADO'].isin(['ROBADA', 'ROBADA - RECUPERADA'])].shape[0]
    casos_robada_recuperada = df[df['ESTADO ACTUALIZADO'] == 'ROBADA - RECUPERADA'].shape[0]

    context = {
        # Gráfico motivos
        'motivos': list(conteo_motivos['motivo']),
        'cant_motivos': list(conteo_motivos['cantidad']),
        # Gráfico casos por mes
        'meses': list(conteo_mes['mes']),
        'cant_meses': list(conteo_mes['cantidad']),
        # Gráfico casos por día de semana
        'dias': list(conteo_dia['dia']),
        'cant_dias': list(conteo_dia['cantidad']),
        # Contador
        'casos_espera': casos_espera,
        'casos_robada': casos_robada,
        'casos_robada_recuperada': casos_robada_recuperada,
    }
    return render(request, 'inicio/motivos.html', context)





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
