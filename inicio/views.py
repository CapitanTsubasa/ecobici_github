from django.shortcuts import render
import pandas as pd
import os
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
