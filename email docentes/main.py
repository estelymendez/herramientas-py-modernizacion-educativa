import csv
import re
import unicodedata
from pathlib import Path

# import Express from Express


def fix_mojibake(s: str) -> str:
    """
    Corrige casos comunes donde un texto UTF-8 fue leído como Latin-1/Windows-1252:
    'MARÃ\x8dA' -> 'MARÍA'
    Si no aplica, devuelve el texto original.
    """
    if not isinstance(s, str):
        return s
    # Heurística simple: si aparecen caracteres típicos del mojibake, intenta corregir
    if "Ã" in s or "â" in s or "\x8f" in s or "\x9d" in s:
        try:
            return s.encode("latin1").decode("utf-8")
        except Exception:
            return s
    return s


def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def normalize_text(s: str) -> str:
    s = fix_mojibake(s or "")
    s = s.strip()
    s = strip_accents(s)
    s = s.upper()
    # deja letras/números/espacios
    s = re.sub(r"[^A-Z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def tokens(s: str) -> set[str]:
    s = normalize_text(s)
    return set(s.split()) if s else set()


def read_csv_with_fallback(path: Path, encodings=("utf-8-sig", "utf-8", "latin1")):
    last_err = None
    for enc in encodings:
        try:
            with path.open("r", encoding=enc, newline="") as f:
                sample = f.read(4096)
            # si pudo leer, ya regresamos ese encoding
            with path.open("r", encoding=enc, newline="") as f:
                return list(csv.reader(f)), enc
        except Exception as e:
            last_err = e
    raise RuntimeError(f"No pude leer {path}. Último error: {last_err}")


def main():
    correo_path = Path("correos_docentes.csv")
    revision_path = Path("revision.csv")
    out_path = Path("revision_resultado.csv")

    # --- Leer correo_docentes.csv con DictReader ---
    # Detectar encoding
    raw_rows, enc_used = read_csv_with_fallback(correo_path)
    if not raw_rows:
        raise RuntimeError("correo_docentes.csv está vacío.")

    headers = raw_rows[0]
    data_rows = raw_rows[1:]

    # Construir lista de dicts
    docentes = []
    for r in data_rows:
        row = {headers[i]: (r[i] if i < len(r) else "") for i in range(len(headers))}
        docentes.append(row)

    # Index por email
    email_to_doc = {}
    for d in docentes:
        em = (d.get("email") or "").strip().lower()
        if em:
            # si se repite, dejamos el primero
            email_to_doc.setdefault(em, d)

    # Index por nombre completo (normalizado)
    # key: "FIRST LAST"
    namekey_to_email = {}
    namekey_to_tokens = {}
    for d in docentes:
        fn = d.get("first_name", "")
        ln = d.get("last_name", "")
        key = normalize_text(f"{fn} {ln}")
        em = (d.get("email") or "").strip().lower()
        if key and em:
            namekey_to_email.setdefault(key, em)
            namekey_to_tokens.setdefault(key, tokens(f"{fn} {ln}"))

    # --- Leer revision.csv (sin encabezados): email, full_name ---
    rev_rows, rev_enc = read_csv_with_fallback(revision_path)
    # revision.csv no tiene encabezados, asumimos 2 columnas
    revision = []
    for r in rev_rows:
        if not r or all(not c.strip() for c in r):
            continue
        email = (r[0] if len(r) > 0 else "").strip()
        full_name = (r[1] if len(r) > 1 else "").strip()
        revision.append((email, full_name))

    # --- Procesar ---
    out_rows = []
    for email, full_name in revision:
        email_l = email.lower()
        existe = 1 if email_l in email_to_doc else 0

        email_sugerido = ""
        metodo = ""

        if existe == 0:
            # 1) match exacto por key normalizada (full_name vs "first last")
            full_norm = normalize_text(full_name)
            if full_norm in namekey_to_email:
                email_sugerido = namekey_to_email[full_norm]
                metodo = "match_nombre_exacto"
            else:
                # 2) match por tokens: si todos los tokens del docente están contenidos en el full_name
                full_tokens = tokens(full_name)
                best_key = None

                for key, dtoks in namekey_to_tokens.items():
                    if dtoks and dtoks.issubset(full_tokens):
                        best_key = key
                        break

                if best_key:
                    email_sugerido = namekey_to_email.get(best_key, "")
                    metodo = "match_tokens_subset"

        out_rows.append({
            "email_revision": email,
            "nombre_revision": fix_mojibake(full_name),
            "existe_email": existe,
            "email_sugerido": email_sugerido,
            "metodo_sugerencia": metodo
        })

    # --- Escribir salida ---
    fieldnames = ["email_revision", "nombre_revision", "existe_email", "email_sugerido", "metodo_sugerencia"]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(out_rows)

    print(f"OK ✅ Generado: {out_path.resolve()}")
    print(f"Leí correo_docentes.csv con encoding: {enc_used} | revision.csv con encoding: {rev_enc}")


if __name__ == "__main__":
    main()