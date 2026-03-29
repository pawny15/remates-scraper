#!/usr/bin/env python3
"""
Scraper de Remates Judiciales — mipropiedadchile.cl
Fuente: rematesinmobiliarios.cl
Ejecutar: python scraper.py
Genera: remates_limpio.csv

Captura TODOS los remates con paginación completa.
"""

import re
import csv
import time
import hashlib
import logging
from datetime import datetime, date
from dataclasses import dataclass, field
from typing import Optional
import requests
from bs4 import BeautifulSoup

# ─── CONFIGURACION ─────────────────────────────────────
OUTPUT_CSV = "remates_limpio.csv"
LOG_FILE   = "scraper.log"
DELAY      = 1.2
UF         = 39841.72

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-CL,es;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.rematesinmobiliarios.cl/",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ─── MODELO ────────────────────────────────────────────
@dataclass
class Remate:
    id:                str = ""
    tipo:              str = ""
    region:            str = ""
    comuna:            str = ""
    direccion:         str = ""
    fecha_remate:      str = ""   # DD/MM/YYYY
    fecha_sort:        str = ""   # YYYY-MM-DD
    precio_clp:        str = ""
    precio_uf:         str = ""
    metros2:           str = ""
    url_ficha:         str = ""
    url_maps:          str = ""
    fecha_publicacion: str = ""   # DD/MM/YYYY — dato real de la fuente
    actualizado:       str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d"))

    @staticmethod
    def headers():
        return [
            "id", "tipo", "region", "comuna", "direccion",
            "fecha_remate", "precio_clp", "precio_uf",
            "metros2", "url_ficha", "url_maps",
            "fecha_publicacion", "actualizado"
        ]

    def to_row(self):
        return [
            self.id, self.tipo, self.region, self.comuna, self.direccion,
            self.fecha_remate, self.precio_clp, self.precio_uf,
            self.metros2, self.url_ficha, self.url_maps,
            self.fecha_publicacion, self.actualizado
        ]

# ─── UTILIDADES ────────────────────────────────────────
def get_soup(url, session):
    try:
        r = session.get(url, headers=HEADERS, timeout=25)
        r.raise_for_status()
        time.sleep(DELAY)
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        log.warning(f"  ERROR {url}: {e}")
        return None

def parsear_fecha(texto):
    """Acepta DD-MM-YYYY o DD/MM/YYYY. Retorna (DD/MM/YYYY, YYYY-MM-DD)."""
    if not texto:
        return "", ""
    texto = texto.strip().replace("-", "/")
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", texto)
    if m:
        d, mo, y = m.group(1).zfill(2), m.group(2).zfill(2), m.group(3)
        return f"{d}/{mo}/{y}", f"{y}-{mo}-{d}"
    return texto, texto

def es_vigente(fecha_sort):
    if not fecha_sort:
        return True
    try:
        return date.fromisoformat(fecha_sort) >= date.today()
    except ValueError:
        return True

def precio_a_clp_uf(texto):
    if not texto:
        return "", ""
    num_str = re.sub(r"[^\d]", "", texto)
    try:
        num    = int(num_str)
        uf     = round(num / UF, 1)
        clp    = "$" + f"{num:,}".replace(",", ".")
        uf_fmt = f"{uf:,.1f}".replace(",", ".")
        return clp, uf_fmt
    except Exception:
        return texto, ""

def normalizar_tipo(raw):
    mapa = {
        "departamento": "Departamento", "depto": "Departamento",
        "casa": "Casa", "vivienda": "Casa",
        "sitio": "Sitio/Terreno", "terreno": "Sitio/Terreno", "lote": "Sitio/Terreno",
        "parcela": "Parcela", "predio": "Parcela",
        "predio agrícola": "Parcela", "predio agricola": "Parcela",
        "parcela con casa": "Parcela con Casa",
        "local": "Local Comercial", "local comercial": "Local Comercial",
        "oficina": "Oficina", "bodega": "Bodega",
        "estacionamiento": "Estacionamiento",
        "industrial": "Industrial", "nave": "Industrial",
        "galpon": "Industrial", "galpón": "Industrial",
        "edificio": "Edificio",
        "propiedad": "Inmueble", "inmueble": "Inmueble",
        "derechos de propiedad": "Derechos de Propiedad",
        "derechos": "Derechos de Propiedad",
        "lancha": "Embarcación",
    }
    return mapa.get(raw.lower().strip(), raw.title())

def extraer_direccion(url_maps, comuna):
    if not url_maps:
        return ""
    m = re.search(r"/maps/search/(.+?)(?:\?|$)", url_maps)
    if not m:
        return ""
    dir_raw = m.group(1)
    reemplazos = [
        ("%C3%B1","ñ"),("%C3%A9","é"),("%C3%B3","ó"),("%C3%A1","á"),
        ("%C3%AD","í"),("%C3%BA","ú"),("%C3%91","Ñ"),("%C3%89","É"),
        ("%C3%81","Á"),("%C3%9A","Ú"),("%27","'"),("%C2%A0"," "),
        ("%C2%B0","°"),("+"," "),("%2C",","),("%2F","/"),
    ]
    for enc, char in reemplazos:
        dir_raw = dir_raw.replace(enc, char)
    patrones_fin = [
        rf"\s*,?\s*{re.escape(comuna.lower())}.*$",
        r"\s*,?\s*región\s+metropolitana.*$",
        r"\s*,?\s*región\s+de.*$",
        r"\s*,?\s*chile\s*$",
        r"\s*,?\s*santiago\s*$",
    ]
    for pat in patrones_fin:
        dir_raw = re.sub(pat, "", dir_raw, flags=re.IGNORECASE)
    return re.sub(r"\s{2,}", " ", dir_raw).strip().title()

def generar_id(url_ficha, fecha_sort, comuna):
    raw = f"{url_ficha}|{fecha_sort}|{comuna}"
    return hashlib.md5(raw.encode()).hexdigest()[:10].upper()

def detectar_total_paginas(soup):
    """
    Lee el paginador del HTML y devuelve el número máximo de página.
    El sitio usa ?pagina=N como parámetro.
    """
    numeros = re.findall(r"pagina=(\d+)", soup.decode())
    if not numeros:
        return 1
    return max(int(n) for n in numeros)

# ══════════════════════════════════════════════════════
# CORE: scrapear una URL (una sola página de la tabla)
# ══════════════════════════════════════════════════════
def scrape_pagina(url, session, fallback_region, ids_vistos):
    soup = get_soup(url, session)
    if not soup:
        return [], 1

    tabla = soup.select_one("table")
    if not tabla:
        return [], 1

    total_pags = detectar_total_paginas(soup)
    filas      = tabla.select("tbody tr") or tabla.select("tr")
    result     = []

    for fila in filas:
        celdas = fila.select("td")
        if len(celdas) < 5:
            continue
        try:
            # Col 2: Fecha Remate
            fecha_display, fecha_sort = parsear_fecha(celdas[2].get_text(strip=True))
            if not es_vigente(fecha_sort):
                continue

            # Col 3: Publicado
            pub_display, _ = parsear_fecha(celdas[3].get_text(strip=True) if len(celdas) > 3 else "")

            # Col 4: Región
            region = celdas[4].get_text(strip=True) if len(celdas) > 4 else fallback_region

            # Col 5: Comuna
            comuna = celdas[5].get_text(strip=True) if len(celdas) > 5 else ""

            # Col 6: Tipo
            tipo = normalizar_tipo(celdas[6].get_text(strip=True) if len(celdas) > 6 else "")

            # Col 7: Maps / Dirección
            url_maps  = ""
            direccion = ""
            if len(celdas) > 7:
                a = celdas[7].select_one("a[href]")
                if a:
                    url_maps  = a.get("href", "")
                    direccion = extraer_direccion(url_maps, comuna)

            # Col 8: M2
            metros2 = celdas[8].get_text(strip=True) if len(celdas) > 8 else ""

            # Col 9: Precio
            precio_clp, precio_uf = precio_a_clp_uf(
                celdas[9].get_text(strip=True) if len(celdas) > 9 else ""
            )

            # Col 10: URL ficha
            url_ficha = ""
            if len(celdas) > 10:
                a = celdas[10].select_one("a[href]")
                if a:
                    href = a.get("href", "")
                    url_ficha = href if href.startswith("http") else \
                                "https://www.rematesinmobiliarios.cl" + href

            rid = generar_id(url_ficha, fecha_sort, comuna)
            if rid in ids_vistos:
                continue
            ids_vistos.add(rid)

            result.append(Remate(
                id=rid, tipo=tipo, region=region, comuna=comuna,
                direccion=direccion, fecha_remate=fecha_display,
                fecha_sort=fecha_sort, precio_clp=precio_clp,
                precio_uf=precio_uf, metros2=metros2,
                url_ficha=url_ficha, url_maps=url_maps,
                fecha_publicacion=pub_display,
            ))
        except Exception as e:
            log.warning(f"    Fila error: {e}")

    return result, total_pags


def scrape_region(slug, nombre, session, ids_vistos):
    BASE = "https://www.rematesinmobiliarios.cl"
    base_url = f"{BASE}/remates/{slug}/"
    log.info(f"  [{nombre}] {base_url}")

    remates_region = []

    # Página 1 — también detecta total de páginas
    lote, total_pags = scrape_pagina(base_url, session, nombre, ids_vistos)
    remates_region.extend(lote)
    log.info(f"    Pág 1/{total_pags}: {len(lote)} remates")

    # Páginas 2..N
    for pag in range(2, total_pags + 1):
        url_pag = f"{base_url}?pagina={pag}"
        lote, _ = scrape_pagina(url_pag, session, nombre, ids_vistos)
        log.info(f"    Pág {pag}/{total_pags}: {len(lote)} remates")
        remates_region.extend(lote)
        if not lote:
            log.info(f"    Pág {pag} vacía — parando")
            break

    log.info(f"  [{nombre}] subtotal: {len(remates_region)}")
    return remates_region


# ══════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════
def scrape():
    REGIONES = [
        ("region-arica-y-parinacota", "Arica y Parinacota"),
        ("region-internacional",       "Internacional"),
        ("region-tarapaca",            "Tarapacá"),
        ("region-antofagasta",         "Antofagasta"),
        ("region-atacama",             "Atacama"),
        ("region-coquimbo",            "Coquimbo"),
        ("region-valparaiso",          "Valparaíso"),
        ("region-metropolitana",       "Metropolitana"),
        ("region-ohiggins",            "O'Higgins"),
        ("region-maule",               "Maule"),
        ("region-nuble",               "Ñuble"),
        ("region-biobio",              "Biobío"),
        ("region-araucania",           "Araucanía"),
        ("region-los-rios",            "Los Ríos"),
        ("region-los-lagos",           "Los Lagos"),
        ("region-aysen",               "Aysén"),
        ("region-magallanes",          "Magallanes"),
    ]

    session    = requests.Session()
    ids_vistos = set()
    todos      = []

    for slug, nombre in REGIONES:
        todos.extend(scrape_region(slug, nombre, session, ids_vistos))
        log.info(f"  Acumulado total: {len(todos)}")

    return todos


def main():
    hoy = date.today().strftime("%Y-%m-%d")
    log.info("")
    log.info("=" * 60)
    log.info("  SCRAPER — mipropiedadchile.cl")
    log.info(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"  Remates vigentes a partir de: {hoy}")
    log.info("=" * 60)

    remates = scrape()
    remates.sort(key=lambda r: r.fecha_sort or "9999")

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(Remate.headers())
        for r in remates:
            w.writerow(r.to_row())

    log.info("")
    log.info("=" * 60)
    log.info(f"  COMPLETADO: {len(remates)} remates vigentes")
    log.info(f"  Archivo: {OUTPUT_CSV}")
    log.info("=" * 60)
    print(f"\n  Listo. {len(remates)} remates guardados en '{OUTPUT_CSV}'\n")


if __name__ == "__main__":
    main()
