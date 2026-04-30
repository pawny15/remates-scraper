#!/usr/bin/env python3
"""
build_html.py — mipropiedadchile.cl
====================================
Lee remates_limpio.csv + template HTML base y genera index.html
con los datos embebidos directamente en el JS.

Uso:
    python build_html.py

Salida:
    index.html  (listo para subir a Cloudflare Pages)
"""

import csv
import json
import os
import sys
import requests
from datetime import datetime

# ─── RUTAS ─────────────────────────────────────────────────────────────────
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
CSV_FILE     = os.path.join(SCRIPT_DIR, "remates_limpio.csv")
TEMPLATE_FILE = os.path.join(SCRIPT_DIR, "template.html")
OUTPUT_FILE  = os.path.join(SCRIPT_DIR, "index.html")

# ─── OBTENER UF ACTUAL ────────────────────────────────────────────────────
def obtener_uf() -> float:
    try:
        r = requests.get("https://mindicador.cl/api/uf", timeout=10)
        data = r.json()
        uf = data["serie"][0]["valor"]
        print(f"  UF del día: ${uf:,.2f}")
        return float(uf)
    except Exception as e:
        print(f"  AVISO: No se pudo obtener UF ({e}). Usando valor de referencia.")
        return 39900.0


# ─── LEER CSV ─────────────────────────────────────────────────────────────
def leer_csv(path: str) -> list[dict]:
    remates = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Normalizar región: convertir slugs sucios a nombre limpio
            region = row.get("region", "").strip()
            if region and "-" in region and " " not in region:
                # Es un slug: capitalizar palabras separadas por guion
                region = " ".join(w.capitalize() for w in region.split("-"))
                row["region"] = region

            remates.append({
                "id":           row.get("id", "").strip(),
                "tipo":         row.get("tipo", "Inmueble").strip(),
                "region":       row.get("region", "").strip(),
                "comuna":       row.get("comuna", "").strip(),
                "direccion":    row.get("direccion", "").strip(),
                "fecha_remate": row.get("fecha_remate", "").strip(),
                "precio_clp":   row.get("precio_clp", "").strip(),
                "precio_uf":    row.get("precio_uf", "").strip(),
                "metros2":      row.get("metros2", "").strip(),
                "url_ficha":    row.get("url_ficha", "").strip(),
                "url_maps":     row.get("url_maps", "").strip(),
                "actualizado":  row.get("actualizado", datetime.now().strftime("%Y-%m-%d")).strip(),
            })
    return remates


# ─── GENERAR index.html ────────────────────────────────────────────────────
def generar_html(remates: list[dict], uf: float, template_path: str, output_path: str):
    with open(template_path, "r", encoding="utf-8") as f:
        template = f.read()

    # Serializar datos como JSON compacto
    data_json = json.dumps(remates, ensure_ascii=False, separators=(",", ":"))

    hoy = datetime.now().strftime("%Y-%m-%d %H:%M")
    total = len(remates)

    # Inyectar en el placeholder del template
    resultado = template.replace(
        "// __DATA_PLACEHOLDER__",
        f"// Datos embebidos — {total} remates — actualizado {hoy}\n"
        f"// UF del día: ${uf:,.2f}\n"
        f"const DATA_EMBEDDED = {data_json};"
    )

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(resultado)

    print(f"  ✓ index.html generado con {total} remates")
    print(f"  ✓ Tamaño: {os.path.getsize(output_path):,} bytes")


# ─── MAIN ─────────────────────────────────────────────────────────────────
def main():
    print()
    print("=" * 60)
    print("  BUILD HTML — mipropiedadchile.cl")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    if not os.path.exists(CSV_FILE):
        print(f"  ERROR: No se encontró {CSV_FILE}")
        sys.exit(1)
    if not os.path.exists(TEMPLATE_FILE):
        print(f"  ERROR: No se encontró {TEMPLATE_FILE}")
        sys.exit(1)

    print()
    print("  Leyendo CSV…")
    remates = leer_csv(CSV_FILE)
    print(f"  {len(remates)} remates cargados")

    print()
    print("  Obteniendo UF…")
    uf = obtener_uf()

    print()
    print("  Generando index.html…")
    generar_html(remates, uf, TEMPLATE_FILE, OUTPUT_FILE)

    print()
    print("=" * 60)
    print("  ¡LISTO!")
    print("=" * 60)
    print()


if __name__ == "__main__":
    main()
