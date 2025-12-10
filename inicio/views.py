import plotly.express as px
import json
import pandas as pd
import os
import unicodedata
import matplotlib.pyplot as plt
import calendar

from io import BytesIO

from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload

from django.shortcuts import render
from django.conf import settings
from django.http import HttpResponse

from collections import Counter





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
    usuarios = pd.read_csv(file_stream, encoding="latin-1", sep="\t")

    # ================================
    # 📅 Manejo de fechas
    # ================================
    usuarios['Fecha_Inicio'] = pd.to_datetime(usuarios['Fecha_Inicio'], errors='coerce')
    usuarios = usuarios.dropna(subset=['Fecha_Inicio'])
    usuarios['Mes'] = usuarios['Fecha_Inicio'].dt.to_period('M')

    fecha_actual = pd.Timestamp.now()
    mes_actual = fecha_actual.to_period('M')
    anio_actual = fecha_actual.year

    # ================================
    # 📊 Totales y acumulaciones
    # ================================
    total_anual = usuarios[usuarios['Fecha_Inicio'].dt.year == anio_actual].shape[0]
    viajes_mes_actual = usuarios[usuarios['Mes'] == mes_actual].shape[0]
    porcentaje_mes = (viajes_mes_actual / total_anual * 100) if total_anual > 0 else 0

    # ================================
    # 📊 Viajes por mes (para gráfico)
    # ================================
    viajes_por_mes = (
        usuarios[usuarios['Fecha_Inicio'].dt.year == anio_actual]
        .groupby(usuarios['Fecha_Inicio'].dt.month)
        .size()
        .reset_index(name='viajes')
    )
    viajes_por_mes['mes_nombre'] = viajes_por_mes['Fecha_Inicio'].apply(
        lambda x: pd.Timestamp(2025, x, 1).strftime('%b')
    )

    meses_labels = viajes_por_mes['mes_nombre'].tolist()
    viajes_values = viajes_por_mes['viajes'].tolist()

    # ================================
    # 📊 Conteo por sexo
    # ================================
    sexo_counts = usuarios['Sexo'].value_counts().to_dict()

    # ================================
    # 📊 Viajes por estación de origen (Top 10)
    # ================================
    viajes_por_origen = usuarios['Nombre_Inicio_Viaje'].value_counts().head(10)
    viajes_por_origen_dict = viajes_por_origen.to_dict()

    # ================================
    # 📊 Viajes por destino (Top 10)
    # ================================
    viajes_por_destino = usuarios['Nombre_Final_Viaje'].value_counts().head(10)
    viajes_por_destino_dict = viajes_por_destino.to_dict()

    # ================================
    # Tabla preview
    # ================================
    tabla_html = usuarios.head(50).to_html(classes="table table-striped", index=False)

    # ================================
    # 📊 Promedio de viajes por tipo de día (mes actual)
    # ================================

    # --- Asegurarse que Fecha_Inicio sea datetime ---
    usuarios['Fecha_Inicio'] = pd.to_datetime(usuarios['Fecha_Inicio'], errors='coerce')

    # --- Filtrar mes actual ---
    mes_actual = pd.Timestamp.now().month
    anio_actual = pd.Timestamp.now().year

    usuarios_mes_actual = usuarios[
        (usuarios['Fecha_Inicio'].dt.month == mes_actual) &
        (usuarios['Fecha_Inicio'].dt.year == anio_actual)
    ]

    # --- Día de la semana (0=Lunes, 6=Domingo) ---
    usuarios_mes_actual['dia_semana'] = usuarios_mes_actual['Fecha_Inicio'].dt.dayofweek
    usuarios_mes_actual['tipo_dia'] = usuarios_mes_actual['dia_semana'].apply(lambda x: 'Fin de semana' if x >= 5 else 'Lunes a Viernes')

    # --- Contar viajes por día ---
    viajes_por_dia = usuarios_mes_actual.groupby(usuarios_mes_actual['Fecha_Inicio'].dt.date).size().reset_index(name='viajes')

    # --- Renombrar y preparar ---
    viajes_por_dia.rename(columns={'Fecha_Inicio': 'fecha'}, inplace=True)
    viajes_por_dia['fecha'] = pd.to_datetime(viajes_por_dia['fecha'], errors='coerce')
    viajes_por_dia['dia_semana'] = viajes_por_dia['fecha'].dt.dayofweek
    viajes_por_dia['tipo_dia'] = viajes_por_dia['dia_semana'].apply(lambda x: 'Fin de semana' if x >= 5 else 'Lunes a Viernes')

    # --- Promedios ---
    if not viajes_por_dia.empty:
        promedio_lunes_viernes = viajes_por_dia[viajes_por_dia['tipo_dia'] == 'Lunes a Viernes']['viajes'].mean()
        promedio_fin_semana = viajes_por_dia[viajes_por_dia['tipo_dia'] == 'Fin de semana']['viajes'].mean()
    else:
        promedio_lunes_viernes = promedio_fin_semana = 0

    promedio_lunes_viernes = 0 if pd.isna(promedio_lunes_viernes) else promedio_lunes_viernes
    promedio_fin_semana = 0 if pd.isna(promedio_fin_semana) else promedio_fin_semana

    # --- Variaciones (de ejemplo) ---
    variacion_lv = 0.037
    variacion_fs = -0.015

    # ================================
    # 🚲 Análisis de uso de bicicletas (mes actual)
    # ================================
    # Usamos usuarios_mes_actual (ya filtrado)
    if not usuarios_mes_actual.empty:
        # 1️⃣ Bicicletas únicas usadas
        bicicletas_usadas = usuarios_mes_actual['Msnbc_de_bicicleta'].nunique()

        # 2️⃣ Total de viajes del mes
        viajes_mes = len(usuarios_mes_actual)

        # 3️⃣ Promedio de viajes por bicicleta
        promedio_viajes_por_bici = viajes_mes / bicicletas_usadas if bicicletas_usadas > 0 else 0

        # 4️⃣ Promedios de lunes a viernes y fin de semana (ya calculados antes)
    else:
        bicicletas_usadas = 0
        viajes_mes = 0
        promedio_viajes_por_bici = 0

    # ============================================================
    # 🚲 Gráfico de bicicletas únicas utilizadas por mes
    # ============================================================
    usuarios['Mes'] = usuarios['Fecha_Inicio'].dt.month
    bicicletas_por_mes = usuarios.groupby('Mes')['Msnbc_de_bicicleta'].nunique().reset_index()
    bicicletas_por_mes['Mes'] = bicicletas_por_mes['Mes'].apply(lambda x: calendar.month_abbr[x])

    fig = px.bar(
        bicicletas_por_mes,
        x='Mes',
        y='Msnbc_de_bicicleta',
        title="🚲 Bicicletas únicas utilizadas por mes",
        color_discrete_sequence=['#1f77b4']
    )
    grafico_bicis_html = fig.to_html(full_html=False)

    # --- Otras secciones que ya tengas ---
    # grafico_viajes_html = ...
    # grafico_dias_html = ...

    # --- Renderizado final ---
    return render(request, "usuarios.html", {
        "grafico_bicis_html": grafico_bicis_html,
        # otros gráficos que ya pasás:
        # "grafico_viajes_html": grafico_viajes_html,
    })

    # ================================
    # 📦 Render Context
    # ================================
    render_context = {
        "tabla": tabla_html,
        "sexo_counts": sexo_counts,
        "viajes_por_origen": viajes_por_origen_dict,
        "viajes_por_destino": viajes_por_destino_dict,
        "viajes_acumulados": total_anual,
        "viajes_mes_actual": viajes_mes_actual,
        "porcentaje_mes": porcentaje_mes,
        "meses_labels": meses_labels,
        "viajes_values": viajes_values,
        "promedio_lunes_viernes": round(promedio_lunes_viernes, 0),
        "promedio_fin_semana": round(promedio_fin_semana, 0),
        "variacion_lv": variacion_lv,
        "variacion_fs": variacion_fs,
        # 🚲 Nuevos datos de análisis
        "bicicletas_usadas": bicicletas_usadas,
        "viajes_mes": viajes_mes,
        "promedio_viajes_por_bici": round(promedio_viajes_por_bici, 1),
        "tabla_ultimo_uso": tabla_ultimo_uso,
    }

    return render(request, "inicio/usuarios.html", render_context)


####################################################################################################################
####################################   FIN DE - MOSTRAR_USUARIOS   #################################################
####################################################################################################################

####################################################################################################################
####################################   VIAJES MENSUALES - GRAFICOS  ################################################
####################################################################################################################



####################################################################################################################
####################################  FIN DE VIAJES MENSUALES - GRAFICOS  ##########################################
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
    range_name = 'BICICLETAS!A:AP'  # Nombre real de la hoja

    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=id_sheet,
        range=range_name
    ).execute()

    values = result.get('values', [])
    if not values:
        return HttpResponse("No hay datos en el Google Sheet")

    df = pd.DataFrame(values[1:], columns=values[0])

    # ================= GRÁFICO 1: MOTIVOS =================
    conteo_motivos = df['MOTIVO'].value_counts().reset_index()
    conteo_motivos.columns = ['motivo', 'cantidad']
    conteo_motivos = df['MOTIVO'].value_counts().to_dict()

    # ================= GRÁFICO 2: CASOS POR MES =================
    df['FECHA DE VIAJE'] = pd.to_datetime(df['FECHA DE VIAJE'], errors='coerce', dayfirst=True)
    conteo_mes = df.groupby(df['FECHA DE VIAJE'].dt.to_period('M')).size().reset_index(name='cantidad')
    conteo_mes['mes'] = conteo_mes['FECHA DE VIAJE'].astype(str)

    # ================= GRÁFICO 3: DÍA DE SEMANA =================
    df['DIA_SEMANA'] = df['FECHA DE VIAJE'].dt.day_name()
    conteo_dia = df['DIA_SEMANA'].value_counts().reset_index()
    conteo_dia.columns = ['dia', 'cantidad']

    # ======== FILTRAR MOTIVO VANDALISMO =========
    df['MOTIVO'] = df['MOTIVO'].astype(str).str.upper().str.strip()

    df_vandalismo = df[df['MOTIVO'].str.contains(r'\bVANDALISMO', na=False, regex=True)]

    conteo_vandalismo = (
        df_vandalismo.groupby(df_vandalismo['FECHA DE VIAJE'].dt.to_period('M'))
        .size()
        .reset_index(name='cantidad')
    )
    conteo_vandalismo['mes'] = conteo_vandalismo['FECHA DE VIAJE'].astype(str)

    # ======== CONTADORES =========
    casos_espera = df[df['ESTADO ACTUALIZADO'] == 'A LA ESPERA'].shape[0]
    casos_robada = df[df['ESTADO ACTUALIZADO'].isin(['ROBADA', 'ROBADA - RECUPERADA'])].shape[0]
    casos_robada_recuperada = df[df['ESTADO ACTUALIZADO'] == 'ROBADA - RECUPERADA'].shape[0]

    # ======== AGREGAR CALENDARIO =========
    df['FECHA DE VIAJE'] = pd.to_datetime(df['FECHA DE VIAJE'], errors='coerce', dayfirst=True)
    df['MES'] = df['FECHA DE VIAJE'].dt.to_period('M').astype(str)

    # ---- LISTA DE MESES ÚNICOS PARA EL DROPDOWN ----
    meses_unicos = sorted(df['MES'].dropna().unique(), reverse=True)

    mes_filtro = request.GET.get('mes')

    if mes_filtro:
        df = df[df['MES'] == mes_filtro]

    context = {
        'cant_meses': list(conteo_mes['cantidad']),
        'cant_dias': list(conteo_dia['cantidad']),
        'casos_espera': casos_espera,
        'casos_robada': casos_robada,
        'casos_robada_recuperada': casos_robada_recuperada,
        'conteo_motivos': conteo_motivos,
        'dias': list(conteo_dia['dia']),
        'meses_unicos': meses_unicos,
        'mes_actual': mes_filtro,
        'meses': list(conteo_mes['mes']),
        'motivos': list(df['MOTIVO'].value_counts().index),
        'cant_motivos': list(df['MOTIVO'].value_counts().values),
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

    return response
