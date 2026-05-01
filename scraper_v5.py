#!/usr/bin/env python3
"""
scraper_v5.py — mipropiedadchile.cl
=====================================
Raspa TODOS los remates de rematesinmobiliarios.cl
recorriendo cada región página por página.

Salida: remates_limpio.csv
"""

import csv
import time
import re
import os
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ─── CONFIG ───────────────────────────────────────────────────────────────────
BASE_URL   = "https://www.rematesinmobiliarios.cl"
OUTPUT_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "remates_limpio.csv")
DELAY      = 1.2   # segundos entre requests (respetuoso con el servidor)
HEADERS    = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-CL,es;q=0.9",
}

# URLs de cada región (extraídas del sitio)
REGIONES = [
    ("Tarapacá",        "/remates/region-tarapaca/"),
    ("Antofagasta",     "/remates/region-antofagasta/"),
    ("Atacama",         "/remates/region-atacama/"),
    ("Coquimbo",        "/remates/region-coquimbo/"),
    ("Valparaíso",      "/remates/region-valparaiso/"),
    ("Metropolitana",   "/remates/region-metropolitana/"),
    ("O'Higgins",       "/remates/region-ohiggins/"),
    ("Maule",           "/remates/region-maule/"),
    ("Ñuble",           "/remates/region-nuble/"),
    ("Biobío",          "/remates/region-biobio/"),
    ("Araucanía",       "/remates/region-araucania/"),
    ("Los Ríos",        "/remates/region-los-rios/"),
    ("Los Lagos",       "/remates/region-los-lagos/"),
    ("Aysén",           "/remates/region-aysen/"),
    ("Magallanes",      "/remates/region-magallanes/"),
    ("Arica",           "/remates/region-arica/"),
    ("Tarapacá",        "/remates/region-tarapaca/"),
]

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def get_soup(url: str, session: requests.Session) -> BeautifulSoup | None:
    try:
        r = session.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"    ERROR fetching {url}: {e}")
        return None


def limpiar_precio(texto: str) -> str:
    """Extrae solo números del precio."""
    if not texto:
        return ""
    return re.sub(r"[^\d]", "", texto.strip())


def parsear_fila(tr, region_nombre: str, hoy_str: str) -> dict | None:
    """Parsea una fila <tr> de la tabla de remates."""
    celdas = tr.find_all("td")
    if len(celdas) < 8:
        return None

    # Columnas típicas: N° | Tipo(img) | Fecha Remate | Publicado | Región | Comuna | Tipo texto | Maps | M2 | Mínimo | Ver
    try:
        fecha_remate = celdas[2].get_text(strip=True)   # ej: "02-06-2026"
        comuna       = celdas[5].get_text(strip=True)
        tipo         = celdas[6].get_text(strip=True)
        metros2      = celdas[8].get_text(strip=True) if len(celdas) > 8 else ""
        precio_texto = celdas[9].get_text(strip=True) if len(celdas) > 9 else ""

        # URL ficha
        url_ficha = ""
        link = tr.find("a", href=re.compile(r"ficha-remate"))
        if link:
            url_ficha = urljoin(BASE_URL, link["href"])

        # ID desde URL
        remate_id = ""
        m = re.search(r"id=(\d+)", url_ficha)
        if m:
            remate_id = m.group(1)

        # URL Google Maps
        url_maps = ""
        maps_link = tr.find("a", href=re.compile(r"google.*maps", re.I))
        if maps_link:
            url_maps = maps_link["href"]

        # Convertir fecha DD-MM-YYYY → YYYY-MM-DD para ordenar
        fecha_iso = ""
        if re.match(r"\d{2}-\d{2}-\d{4}", fecha_remate):
            d, mo, y = fecha_remate.split("-")
            fecha_iso = f"{y}-{mo}-{d}"

        precio_clp = limpiar_precio(precio_texto)

        if not fecha_iso or not remate_id:
            return None

        return {
            "id":           remate_id,
            "tipo":         tipo.capitalize() if tipo else "Inmueble",
            "region":       region_nombre,
            "comuna":       comuna,
            "direccion":    "",   # se puede enriquecer desde ficha si se quiere
            "fecha_remate": fecha_iso,
            "precio_clp":   precio_clp,
            "precio_uf":    "",
            "metros2":      metros2,
            "url_ficha":    url_ficha,
            "url_maps":     url_maps,
            "actualizado":  hoy_str,
        }
    except Exception:
        return None


def scrapear_region(region_nombre: str, region_path: str, session: requests.Session, hoy_str: str) -> list[dict]:
    """Raspa todas las páginas de una región."""
    remates = []
    pagina  = 1
    ids_vistos = set()

    while True:
        if pagina == 1:
            url = f"{BASE_URL}{region_path}"
        else:
            url = f"{BASE_URL}{region_path}?p={pagina}"

        print(f"    Página {pagina}: {url}")
        soup = get_soup(url, session)
        if not soup:
            break

        tabla = soup.find("table")
        if not tabla:
            break

        filas = tabla.find_all("tr")[1:]  # saltar header
        if not filas:
            break

        nuevos = 0
        for tr in filas:
            r = parsear_fila(tr, region_nombre, hoy_str)
            if r and r["id"] not in ids_vistos:
                ids_vistos.add(r["id"])
                remates.append(r)
                nuevos += 1

        print(f"      → {nuevos} remates nuevos (total región: {len(remates)})")

        if nuevos == 0:
            break

        # Verificar si hay página siguiente
        paginador = soup.find("div", class_=re.compile(r"paginad|pagination", re.I))
        if not paginador:
            # Buscar link de siguiente página genéricamente
            next_link = soup.find("a", string=re.compile(r"siguiente|next|\b" + str(pagina + 1) + r"\b", re.I))
            if not next_link:
                break

        pagina += 1
        time.sleep(DELAY)

    return remates


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    hoy_str = datetime.now().strftime("%Y-%m-%d")
    print()
    print("=" * 60)
    print("  SCRAPER — mipropiedadchile.cl")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    session = requests.Session()
    todos   = []
    ids_globales = set()

    # Eliminar duplicados de REGIONES (por si acaso)
    regiones_unicas = []
    paths_vistos = set()
    for nombre, path in REGIONES:
        if path not in paths_vistos:
            regiones_unicas.append((nombre, path))
            paths_vistos.add(path)

    for nombre, path in regiones_unicas:
        print(f"\n  → Región: {nombre}")
        remates_region = scrapear_region(nombre, path, session, hoy_str)
        # Deduplicar globalmente
        for r in remates_region:
            if r["id"] not in ids_globales:
                ids_globales.add(r["id"])
                todos.append(r)
        print(f"  ✓ {len(remates_region)} remates en {nombre}")
        time.sleep(DELAY)

    # También raspar la página principal para no perder nada sin región
    print(f"\n  → Página principal (sin región)")
    remates_main = scrapear_region("Sin región", "/", session, hoy_str)
    for r in remates_main:
        if r["id"] not in ids_globales:
            ids_globales.add(r["id"])
            todos.append(r)

    # Ordenar por fecha de remate
    todos.sort(key=lambda x: x["fecha_remate"])

    print(f"\n  TOTAL REMATES: {len(todos)}")

    # Escribir CSV
    campos = ["id","tipo","region","comuna","direccion","fecha_remate",
              "precio_clp","precio_uf","metros2","url_ficha","url_maps","actualizado"]

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()
        writer.writerows(todos)

    print(f"  ✓ CSV guardado: {OUTPUT_CSV}")
    print(f"  ✓ {len(todos)} remates escritos")
    print()
    print("=" * 60)
    print("  ¡SCRAPER COMPLETADO!")
    print("=" * 60)
    print()


if __name__ == "__main__":
    main()
