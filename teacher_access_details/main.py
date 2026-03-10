import pandas as pd
from pathlib import Path


def detectar_delimitador(file_path: str) -> str:
    """
    Intenta detectar si el CSV usa coma o tabulación.
    """
    with open(file_path, "r", encoding="utf-8-sig") as f:
        primera_linea = f.readline()

    if "\t" in primera_linea:
        return "\t"
    return ","


def leer_csv(file_path: str) -> pd.DataFrame:
    """
    Lee un CSV detectando delimitador común.
    """
    sep = detectar_delimitador(file_path)
    return pd.read_csv(file_path, sep=sep, encoding="utf-8-sig", dtype=str)


def normalizar_access_status(df: pd.DataFrame, col: str = "Access_status") -> pd.DataFrame:
    """
    Convierte Access_status a texto limpio ('0' o '1').
    """
    df = df.copy()
    df[col] = df[col].astype(str).str.strip()
    return df


def procesar_teachers(teachers_path: str):
    df_teachers = leer_csv(teachers_path)
    df_teachers = normalizar_access_status(df_teachers, "Access_status")
    df_teachers = df_teachers[df_teachers["Name"] != "Docente"]

    con_acceso = df_teachers[df_teachers["Access_status"] == "1"].copy()
    sin_acceso = df_teachers[df_teachers["Access_status"] == "0"].copy()

    return con_acceso, sin_acceso


def procesar_students(students_path: str) -> pd.DataFrame:
    df_students = leer_csv(students_path)
    df_students = normalizar_access_status(df_students, "Access_status")

    # Contar acceso/no acceso por Code y School_name
    consolidado = (
        df_students.groupby(["Code", "School_name", "Access_status"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )

    # Asegurar que existan ambas columnas aunque en el archivo solo venga una categoría
    if "1" not in consolidado.columns:
        consolidado["1"] = 0
    if "0" not in consolidado.columns:
        consolidado["0"] = 0

    consolidado = consolidado.rename(
        columns={
            "School_name": "School",
            "1": "Con acceso",
            "0": "Sin acceso",
        }
    )

    consolidado = consolidado[["Code", "School", "Con acceso", "Sin acceso"]]
    consolidado = consolidado.sort_values(by=["Code", "School"]).reset_index(drop=True)

    return consolidado


def exportar_excel(
    teachers_path: str = "teachers.csv",
    students_path: str = "students.csv",
    output_path: str = "reporte_accesos.xlsx",
):
    con_acceso, sin_acceso = procesar_teachers(teachers_path)
    consolidado_students = procesar_students(students_path)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        con_acceso.to_excel(writer, sheet_name="Con acceso", index=False)
        sin_acceso.to_excel(writer, sheet_name="Sin acceso", index=False)
        consolidado_students.to_excel(writer, sheet_name="Consolidado estudiantes", index=False)

    print(f"Archivo generado correctamente: {Path(output_path).resolve()}")


if __name__ == "__main__":
    exportar_excel()
