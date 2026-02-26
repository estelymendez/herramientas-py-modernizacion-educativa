import pandas as pd
import unicodedata
import re
from pathlib import Path

from rapidfuzz import process, fuzz


# =========================
# CONFIG
# =========================
ARCH_DOCENTES = "correos_docentes.csv"
ARCH_REVISION = "revision.csv"
ARCH_SALIDA = "revision_resultado.csv"

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
    p_rev = base_dir / ARCH_REVISION
    p_out = base_dir / ARCH_SALIDA

    if not p_doc.exists():
        raise FileNotFoundError(f"No existe {p_doc}. Ponlo en la misma carpeta del script.")
    if not p_rev.exists():
        raise FileNotFoundError(f"No existe {p_rev}. Ponlo en la misma carpeta del script.")

    # --- Cargar docentes ---
    df_doc = read_csv_flexible(p_doc)
    df_doc.columns = [c.strip() for c in df_doc.columns]

    required_doc_cols = {"first_name", "last_name", "email"}
    if not required_doc_cols.issubset(set(df_doc.columns)):
        raise ValueError(f"correos_docentes.csv debe tener columnas: {required_doc_cols}")

    df_doc["email"] = df_doc["email"].astype(str).str.strip().str.lower()
    df_doc["first_name"] = df_doc["first_name"].astype(str)
    df_doc["last_name"] = df_doc["last_name"].astype(str)

    df_doc["full_name_raw"] = (df_doc["first_name"].str.strip() + " " + df_doc["last_name"].str.strip()).str.strip()
    df_doc["full_name_norm"] = df_doc["full_name_raw"].apply(normalize_name)

    # Index por email (existencia)
    emails_set = set(df_doc["email"].dropna().tolist())

    # Diccionario: nombre_norm -> (email, "FIRST LAST")
    # Si hay duplicados por nombre_norm, nos quedamos con el primero.
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

    # --- Cargar revisión ---
    df_rev = read_csv_flexible(p_rev)
    df_rev.columns = [c.strip() for c in df_rev.columns]

    required_rev_cols = {"Email", "Name"}
    if not required_rev_cols.issubset(set(df_rev.columns)):
        raise ValueError("revision.csv debe tener encabezados: Email,Name")

    df_rev["Email"] = df_rev["Email"].astype(str).str.strip().str.lower()
    df_rev["Name"] = df_rev["Name"].astype(str).apply(fix_mojibake).str.strip()

    # --- Procesar ---
    out = []
    for _, row in df_rev.iterrows():
        email = row["Email"]
        name = row["Name"]

        existe_email = 1 if email in emails_set else 0
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

    df_out = pd.DataFrame(out)
    df_out.to_csv(p_out, index=False, encoding="utf-8")
    print(f"✅ Listo. Archivo generado: {p_out}")

    # Info rápida
    print(f"   - Total revisados: {len(df_out)}")
    print(f"   - Emails existentes: {int((df_out['existe_email'] == 1).sum())}")
    print(f"   - Emails sugeridos: {int((df_out['email_correcto'] != '').sum())}")


if __name__ == "__main__":
    main()