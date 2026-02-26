import os
import sys
import pandas as pd
from datetime import datetime

CARPETA = os.path.dirname(os.path.abspath(__file__))
ARCHIVO_BLOQUES = "ce_bloques.csv"


# ==============================
# UTILIDADES
# ==============================

def limpiar_columnas(df):
    df.columns = df.columns.str.strip()
    return df


def cargar_archivo_datos(nombre_archivo):
    ruta = os.path.join(CARPETA, nombre_archivo)

    if not os.path.exists(ruta):
        print("❌ El archivo no existe en la carpeta actual.")
        return None

    try:
        if nombre_archivo.lower().endswith(".xlsx"):
            df = pd.read_excel(ruta)
        elif nombre_archivo.lower().endswith(".csv"):
            df = pd.read_csv(ruta)
        else:
            print("❌ Formato no soportado. Solo .xlsx o .csv")
            return None
    except Exception as e:
        print(f"❌ Error al leer el archivo: {e}")
        return None

    df = limpiar_columnas(df)

    # Buscar columna Code sin importar mayúsculas
    columnas_lower = {c.lower(): c for c in df.columns}

    if "code" not in columnas_lower:
        print("❌ No se encontró la columna 'Code' en el archivo.")
        return None

    df.rename(columns={columnas_lower["code"]: "Code"}, inplace=True)
    df["Code"] = df["Code"].astype(str).str.strip()

    return df


def cargar_bloques():
    ruta = os.path.join(CARPETA, ARCHIVO_BLOQUES)

    if not os.path.exists(ruta):
        print("❌ No se encontró ce_bloques.csv en la carpeta actual.")
        return None

    try:
        df = pd.read_csv(ruta)
    except Exception as e:
        print(f"❌ Error al leer ce_bloques.csv: {e}")
        return None

    df = limpiar_columnas(df)

    columnas_necesarias = ["CODIGO", "GRUPO (BLOQUE)"]

    for col in columnas_necesarias:
        if col not in df.columns:
            print("❌ ce_bloques.csv no tiene la estructura esperada.")
            return None

    df["CODIGO"] = df["CODIGO"].astype(str).str.strip()
    df["GRUPO (BLOQUE)"] = df["GRUPO (BLOQUE)"].astype(str).str.strip().str.upper()

    return df


def obtener_codigos_por_bloques(df_bloques, bloques_input):
    bloques_limpios = [f"B{b.strip()}" for b in bloques_input.split(",")]

    bloques_existentes = df_bloques["GRUPO (BLOQUE)"].unique()

    bloques_validos = [b for b in bloques_limpios if b in bloques_existentes]

    if not bloques_validos:
        print("❌ Ninguno de los bloques ingresados existe.")
        return []

    codigos = df_bloques[
        df_bloques["GRUPO (BLOQUE)"].isin(bloques_validos)
    ]["CODIGO"].unique().tolist()

    print(f"✔ Bloques válidos detectados: {', '.join(bloques_validos)}")
    print(f"✔ Centros encontrados: {len(codigos)}")

    return codigos


def guardar_resultado(df_filtrado, nombre_base):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre_salida = f"filtrado_{timestamp}_{nombre_base}"
    ruta_salida = os.path.join(CARPETA, nombre_salida)

    if nombre_base.lower().endswith(".xlsx"):
        df_filtrado.to_excel(ruta_salida, index=False)
    else:
        df_filtrado.to_csv(ruta_salida, index=False)

    return nombre_salida


# ==============================
# MAIN
# ==============================

def main():

    print("\n=== SISTEMA DE FILTRADO DE CENTROS ===\n")

    archivo = input("📂 Ingrese el nombre del archivo a procesar: ").strip()

    df = cargar_archivo_datos(archivo)
    if df is None:
        return

    print("\n¿Qué desea hacer?")
    print("1) Filtrar por escuela (código)")
    print("2) Filtrar por bloque")

    opcion = input("Seleccione opción (1 o 2): ").strip()

    codigos_a_filtrar = []

    if opcion == "1":
        codigos_input = input("Ingrese uno o más códigos separados por coma: ")
        codigos_a_filtrar = [c.strip() for c in codigos_input.split(",")]

    elif opcion == "2":
        df_bloques = cargar_bloques()
        if df_bloques is None:
            return

        bloques_input = input("Ingrese uno o más bloques (ej: 1 o 1,2): ")
        codigos_a_filtrar = obtener_codigos_por_bloques(df_bloques, bloques_input)

        if not codigos_a_filtrar:
            return
    else:
        print("❌ Opción inválida.")
        return

    # Filtrar
    df_filtrado = df[df["Code"].isin(codigos_a_filtrar)]

    if df_filtrado.empty:
        print("⚠ No se encontraron registros con los criterios indicados.")
        return

    nombre_salida = guardar_resultado(df_filtrado, archivo)

    print("\n✅ PROCESO COMPLETADO")
    print(f"Registros encontrados: {len(df_filtrado)}")
    print(f"Archivo generado: {nombre_salida}")
    print("\n=====================================\n")


if __name__ == "__main__":
    main()