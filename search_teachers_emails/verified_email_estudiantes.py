import pandas as pd
import unicodedata
import re
from pathlib import Path

from rapidfuzz import process, fuzz


# =========================
# CONFIG
# =========================
ARCH_DOCENTES = "correos_docentes.csv"
ARCH_ESTUDIANTES = "correos_estudiantes.csv"
ARCH_REVISION = "revision.csv"
ARCH_REVISION_ESTUD = "revision_estudiantes.csv"
ARCH_SALIDA_DOCENTES = "revision_resultado_docentes.csv"
ARCH_SALIDA_ESTUDIANTES = "revision_resultado_estudiantes.csv"

# Umbral de similitud (0-100). Sube a 92 si quieres más estricto.
UMBRAL_MATCH = 88


# =========================
# HELPERS
# =========================
def fix_mojibake(s: str) -> str:
    """Intenta corregir textos tipo MARÃA -> MARÍA."""
    if not isinstance(s, str):
        return ""
    if "Ã" in s or "â" in s:
        try:
            return s.encode("latin1").decode("utf-8")
        except Exception:
            return s
    return s


def normalize_name(s: str) -> str:
    """Normaliza nombre: mayúsculas, sin tildes, sin símbolos raros, espacios limpios."""
    s = fix_mojibake(s or "")
    s = s.strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))  # quita tildes
    s = s.upper()
    s = re.sub(r"[^A-Z0-9\s]", " ", s)  # deja letras/números/espacios
    s = re.sub(r"\s+", " ", s).strip()
    return s


def read_csv_flexible(path: Path) -> pd.DataFrame:
    """Lee CSV con fallback de encoding común."""
    for enc in ("utf-8-sig", "utf-8", "latin1"):
        try:
            return pd.read_csv(path, dtype=str, encoding=enc).fillna("")
        except Exception:
            continue
    raise FileNotFoundError(f"No pude leer el archivo: {path}")


# =========================
# MAIN
# =========================
def main():
    base_dir = Path(__file__).parent
    p_doc = base_dir / ARCH_DOCENTES
    p_est = base_dir / ARCH_ESTUDIANTES
    p_rev_docentes = base_dir / ARCH_REVISION
    p_rev_estudiantes = base_dir / ARCH_REVISION_ESTUD
    p_out_doc = base_dir / ARCH_SALIDA_DOCENTES
    p_out_est = base_dir / ARCH_SALIDA_ESTUDIANTES

    if not p_doc.exists():
        raise FileNotFoundError(f"No existe {p_doc}. Ponlo en la misma carpeta del script.")
    if not p_rev_docentes.exists():
        raise FileNotFoundError(f"No existe {p_rev_docentes}. Ponlo en la misma carpeta del script.")
    
    if not p_doc.exists():
        raise FileNotFoundError(f"No existe {p_est}. Ponlo en la misma carpeta del script.")
    if not p_rev_docentes.exists():
        raise FileNotFoundError(f"No existe {p_rev_estudiantes}. Ponlo en la misma carpeta del script.")

    # --- Cargar docentes ---
    df_doc = read_csv_flexible(p_doc)
    df_doc.columns = [c.strip() for c in df_doc.columns]

    # --- Cargar estudiantes ---
    df_est = read_csv_flexible(p_est)
    df_est.columns = [c.strip() for c in df_est.columns]

    # --- Columnas requeridas para procesar ---
    required_doc_cols = {"first_name", "last_name", "email"}
    if not required_doc_cols.issubset(set(df_doc.columns)):
        raise ValueError(f"correos_docentes.csv debe tener columnas: {required_doc_cols}")
    
    required_est_cols = {"first_name", "last_name", "email"}
    if not required_est_cols.issubset(set(df_est.columns)):
        raise ValueError(f"correos_estudiantes.csv debe tener columnas: {required_est_cols}")

    # --- Formateando columnas docentes---
    df_doc["email"] = df_doc["email"].astype(str).str.strip().str.lower()
    df_doc["first_name"] = df_doc["first_name"].astype(str)
    df_doc["last_name"] = df_doc["last_name"].astype(str)

    df_doc["full_name_raw"] = (df_doc["first_name"].str.strip() + " " + df_doc["last_name"].str.strip()).str.strip()
    df_doc["full_name_norm"] = df_doc["full_name_raw"].apply(normalize_name)

    # --- Formateando columnas estudiantes

    df_est["email"] = df_est["email"].astype(str).str.strip().str.lower()
    df_est["first_name"] = df_est["first_name"].astype(str)
    df_est["last_name"] = df_est["last_name"].astype(str)

    df_est["full_name_raw"] = (df_est["first_name"].str.strip() + " " + df_est["last_name"].str.strip()).str.strip()
    df_est["full_name_norm"] = df_est["full_name_raw"].apply(normalize_name)

    # Index por email (existencia)
    emails_set_docentes = set(df_doc["email"].dropna().tolist())
    emails_set_estudiantes = set(df_est["email"].dropna().tolist())

    # Diccionario: nombre_norm -> (email, "FIRST LAST")
    # Si hay duplicados por nombre_norm, nos quedamos con el primero.

    # --- DOCENTES ---
    name_to_best = {}
    for _, r in df_doc.iterrows():
        key = r["full_name_norm"]
        if key and key not in name_to_best and r["email"]:
            apellido = normalize_name(r["last_name"])
            nombre = normalize_name(r["first_name"])
            name_formateado = f"{apellido}, {nombre}"

            name_to_best[key] = (r["email"], name_formateado)

    # Lista de nombres para fuzzy
    maestros_names = list(name_to_best.keys())

    # --- ESTUDIANTES ---
    name_to_best_est = {}
    for _, r in df_est.iterrows():
        key = r["full_name_norm"]
        if key and key not in name_to_best_est and r["email"]:
            apellido = normalize_name(r["last_name"])
            nombre = normalize_name(r["first_name"])
            name_formateado = f"{apellido}, {nombre}"

            name_to_best_est[key] = (r["email"], name_formateado)

    # Lista de nombres para fuzzy
    estudiantes_names = list(name_to_best_est.keys())

    # --- Cargar revisión ---
    # --- DOCENTES ---
    df_rev = read_csv_flexible(p_rev_docentes)
    df_rev.columns = [c.strip() for c in df_rev.columns]
    
    df_rev = read_csv_flexible(p_rev_docentes)
    df_rev.columns = [c.strip() for c in df_rev.columns]

    # --- ESTUDIANTES ---
    df_rev_estudiantes = read_csv_flexible(p_rev_estudiantes)
    df_rev_estudiantes.columns = [c.strip() for c in df_rev_estudiantes.columns]
    
    df_rev_estudiantes = read_csv_flexible(p_rev_estudiantes)
    df_rev_estudiantes.columns = [c.strip() for c in df_rev_estudiantes.columns]

    # --- Columnas requeridas en archivos de revisión ---

    required_rev_cols = {"Email", "Name"}
    if not required_rev_cols.issubset(set(df_rev.columns)):
        raise ValueError("revision.csv debe tener encabezados: Email,Name")
    
    required_rev_estudiantes_cols = {"Email", "Name"}
    if not required_rev_estudiantes_cols.issubset(set(df_rev_estudiantes.columns)):
        raise ValueError("revision.csv debe tener encabezados: Email,Name")

    # --- Formatear columnas ---
    df_rev["Email"] = df_rev["Email"].astype(str).str.strip().str.lower()
    df_rev["Name"] = df_rev["Name"].astype(str).apply(fix_mojibake).str.strip()

    df_rev_estudiantes["Email"] = df_rev_estudiantes["Email"].astype(str).str.strip().str.lower()
    df_rev_estudiantes["Name"] = df_rev_estudiantes["Name"].astype(str).apply(fix_mojibake).str.strip()

    # --- Procesar ---
    # DOCENTES
    out = []
    for _, row in df_rev.iterrows():
        email = row["Email"]
        name = row["Name"]

        existe_email = 1 if email in emails_set_docentes else 0
        email_correcto = ""
        name_verified = ""

        if existe_email == 0:
            q = normalize_name(name)

            # Match exacto por normalización
            if q in name_to_best:
                email_correcto, name_verified = name_to_best[q]
            else:
                # Fuzzy match contra maestro
                if q and maestros_names:
                    match = process.extractOne(
                        q,
                        maestros_names,
                        scorer=fuzz.token_sort_ratio
                    )
                    if match:
                        best_key, score, _ = match
                        if score >= UMBRAL_MATCH:
                            email_correcto, name_verified = name_to_best[best_key]

        else:
            # Si existe el email, opcionalmente podemos traer su nombre "verificado"
            # Buscamos en df_doc por email (primer match)
            rec = df_doc[df_doc["email"] == email].head(1)
            if not rec.empty:
                apellido = normalize_name(rec.iloc[0]["last_name"])
                nombre = normalize_name(rec.iloc[0]["first_name"])
                name_verified = f"{apellido}, {nombre}"

        out.append({
            "email": email,
            "name": name,
            "existe_email": existe_email,
            "email_correcto": email_correcto,
            "name_verified": name_verified
        })

    # ESTUDIANTES
    out_est = []
    for _, row in df_rev_estudiantes.iterrows():
        email = row["Email"]
        name = row["Name"]

        existe_email = 1 if email in emails_set_estudiantes else 0
        email_correcto = ""
        name_verified = ""

        if existe_email == 0:
            q = normalize_name(name)

            # Match exacto por normalización
            if q in name_to_best_est:
                email_correcto, name_verified = name_to_best_est[q]
            else:
                # Fuzzy match contra maestro
                if q and estudiantes_names:
                    match = process.extractOne(
                        q,
                        estudiantes_names,
                        scorer=fuzz.token_sort_ratio
                    )
                    if match:
                        best_key, score, _ = match
                        if score >= UMBRAL_MATCH:
                            email_correcto, name_verified = name_to_best_est[best_key]

        else:
            # Si existe el email, opcionalmente podemos traer su nombre "verificado"
            # Buscamos en df_doc por email (primer match)
            rec = df_est[df_est["email"] == email].head(1)
            if not rec.empty:
                apellido = normalize_name(rec.iloc[0]["last_name"])
                nombre = normalize_name(rec.iloc[0]["first_name"])
                name_verified = f"{apellido}, {nombre}"

        out_est.append({
            "email": email,
            "name": name,
            "existe_email": existe_email,
            "email_correcto": email_correcto,
            "name_verified": name_verified
        })

    # --- SALIDAS ---
    df_out_doc = pd.DataFrame(out)
    df_out_doc.to_csv(p_out_doc, index=False, encoding="utf-8")
    print(f"✅ Listo. Archivo generado: {p_out_doc}")

    df_out_est = pd.DataFrame(out_est)
    df_out_est.to_csv(p_out_est, index=False, encoding="utf-8")
    print(f"✅ Listo. Archivo generado: {p_out_est}")

    # Info rápida
    print(f"   - Total docentes revisados: {len(df_out_doc)}")
    print(f"   - Emails docentes existentes: {int((df_out_doc['existe_email'] == 1).sum())}")
    print(f"   - Emails docentes sugeridos: {int((df_out_doc['email_correcto'] != '').sum())}")

    print(f"   - Total estudiantes revisados: {len(df_out_est)}")
    print(f"   - Emails estudiantes existentes: {int((df_out_est['existe_email'] == 1).sum())}")
    print(f"   - Emails estudiantes sugeridos: {int((df_out_est['email_correcto'] != '').sum())}")


if __name__ == "__main__":
    main()