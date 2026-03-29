#!/usr/bin/env python3
"""
generar_html.py
---------------
Lee remates_limpio.csv y actualiza el DATA_EMBEDDED en index.html.

Uso:
    python generar_html.py

Requiere que existan en la misma carpeta:
    - remates_limpio.csv   (generado por scraper.py)
    - index.html           (tu web actual)

Genera:
    - index.html actualizado con los datos frescos embebidos
"""

import csv
import json
import re
import sys
from pathlib import Path

CSV_FILE  = "remates_limpio.csv"
HTML_FILE = "index.html"

def leer_csv(path):
    remates = []
    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            remates.append({k: v.strip() for k, v in row.items()})
    return remates

def actualizar_html(remates, html_path):
    html = Path(html_path).read_text(encoding="utf-8")

    # Convertir lista a JSON compacto (sin espacios extra)
    json_data = json.dumps(remates, ensure_ascii=False, separators=(",", ":"))

    # Reemplazar el bloque DATA_EMBEDDED en el HTML
    # Busca desde "const DATA_EMBEDDED = [" hasta el cierre "];"
    patron = r"const DATA_EMBEDDED\s*=\s*\[.*?\];"
    nuevo  = f"const DATA_EMBEDDED = {json_data};"

    if not re.search(patron, html, flags=re.DOTALL):
        print("ERROR: No se encontró 'const DATA_EMBEDDED' en index.html")
        print("  Asegúrate de que el HTML tenga esa variable definida.")
        sys.exit(1)

    html_nuevo = re.sub(patron, nuevo, html, flags=re.DOTALL)

    Path(html_path).write_text(html_nuevo, encoding="utf-8")
    print(f"  index.html actualizado con {len(remates)} remates.")

def main():
    print()
    print("=" * 55)
    print("  GENERADOR HTML — mipropiedadchile.cl")
    print("=" * 55)

    if not Path(CSV_FILE).exists():
        print(f"ERROR: No se encontró '{CSV_FILE}'")
        print("  Ejecuta primero: python scraper.py")
        sys.exit(1)

    if not Path(HTML_FILE).exists():
        print(f"ERROR: No se encontró '{HTML_FILE}'")
        sys.exit(1)

    print(f"  Leyendo {CSV_FILE}...")
    remates = leer_csv(CSV_FILE)
    print(f"  {len(remates)} remates cargados")

    print(f"  Actualizando {HTML_FILE}...")
    actualizar_html(remates, HTML_FILE)

    print("=" * 55)
    print(f"  Listo. Sube index.html a GitHub para deployar.")
    print("=" * 55)
    print()

if __name__ == "__main__":
    main()
