#!/usr/bin/env python3
"""
Scraper de Remates Judiciales — mipropiedadchile.cl
Fuente: rematesinmobiliarios.cl
Ejecutar: python scraper.py
Genera: remates_limpio.csv (listo para subir a Google Sheets o la web)
"""

import re
import csv
import time
import hashlib
import logging
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ─── CONFIGURACION ─────────────────────────────────────
OUTPUT_CSV = "remates_limpio.csv"
LOG_FILE   = "scraper.log"
DELAY      = 1.5
UF         = 39841.72  # actualizar semanalmente

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
    id:            str = ""
    tipo:          str = ""
    region:        str = ""
    comuna:        str = ""
    direccion:     str = ""
    fecha_remate:  str = ""   # DD/MM/YYYY para mostrar
    fecha_sort:    str = ""   # YYYY-MM-DD para ordenar
    precio_clp:    str = ""
    precio_uf:     str = ""
    metros2:       str = ""
    url_ficha:     str = ""
    url_maps:      str = ""
    actualizado:   str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d"))

    @staticmethod
    def headers():
        return [
            "id", "tipo", "region", "comuna", "direccion",
            "fecha_remate", "precio_clp", "precio_uf",
            "metros2", "url_ficha", "url_maps", "actualizado"
        ]

    def to_row(self):
        return [
            self.id, self.tipo, self.region, self.comuna, self.direccion,
            self.fecha_remate, self.precio_clp, self.precio_uf,
            self.metros2, self.url_ficha, self.url_maps, self.actualizado
        ]


# ─── UTILIDADES ────────────────────────────────────────
def get_soup(url: str, session: requests.Session) -> Optional[BeautifulSoup]:
    try:
        r = session.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        time.sleep(DELAY)
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        log.warning(f"  ERROR {url}: {e}")
        return None


def parsear_fecha(texto: str) -> tuple:
    """Retorna (fecha_display DD/MM/YYYY, fecha_sort YYYY-MM-DD)"""
    if not texto:
        return "", ""
    texto = texto.strip()
    m = re.search(r"(\d{1,2})[\-/](\d{1,2})[\-/](\d{4})", texto)
    if m:
        d, mo, y = m.group(1).zfill(2), m.group(2).zfill(2), m.group(3)
        return f"{d}/{mo}/{y}", f"{y}-{mo}-{d}"
    return texto, texto


def precio_a_clp_uf(texto: str) -> tuple:
    """Convierte '$49.372.295' a ('$49.372.295', '1.239,0')"""
    if not texto:
        return "", ""
    num_str = texto.replace("$", "").replace(".", "").replace(",", "").strip()
    try:
        num = int(num_str)
        uf = round(num / UF, 1)
        clp = "$" + f"{num:,}".replace(",", ".")
        uf_fmt = f"{uf:,.1f}".replace(",", ".")
        return clp, uf_fmt
    except Exception:
        return texto, ""


def limpiar_direccion(texto: str) -> str:
    if not texto:
        return ""
    texto = re.sub(r"%27", "'", texto)
    texto = re.sub(r"%[0-9A-Fa-f]{2}", " ", texto)
    texto = re.sub(r"\s+(Region|Región)\s+Metropolitana.*$", "", texto, flags=re.IGNORECASE)
    texto = re.sub(r"\s+Chile\s*$", "", texto, flags=re.IGNORECASE)
    texto = re.sub(r"\s{2,}", " ", texto)
    return texto.strip()


def normalizar_tipo(raw: str) -> str:
    mapa = {
        "departamento": "Departamento",
        "depto": "Departamento",
        "casa": "Casa",
        "sitio": "Sitio/Terreno",
        "terreno": "Sitio/Terreno",
        "lote": "Sitio/Terreno",
        "parcela": "Parcela",
        "predio": "Parcela",
        "predio agrícola": "Parcela",
        "predio agricola": "Parcela",
        "parcela con casa": "Parcela con Casa",
        "local": "Local Comercial",
        "local comercial": "Local Comercial",
        "oficina": "Oficina",
        "bodega": "Bodega",
        "estacionamiento": "Estacionamiento",
        "industrial": "Industrial",
        "propiedad": "Inmueble",
        "inmueble": "Inmueble",
        "derechos de propiedad": "Derechos de Propiedad",
    }
    return mapa.get(raw.lower().strip(), raw.title())


def generar_id(url_ficha: str, fecha: str, comuna: str) -> str:
    raw = f"{url_ficha}|{fecha}|{comuna}"
    return hashlib.md5(raw.encode()).hexdigest()[:10].upper()


# ══════════════════════════════════════════════════════
#  SCRAPER — rematesinmobiliarios.cl
# ══════════════════════════════════════════════════════
def scrape() -> list:
    BASE = "https://www.rematesinmobiliarios.cl"

    REGIONES = [
        ("", ""),
        ("region-arica-y-parinacota", "Arica y Parinacota"),
        ("region-tarapaca", "Tarapacá"),
        ("region-antofagasta", "Antofagasta"),
        ("region-atacama", "Atacama"),
        ("region-coquimbo", "Coquimbo"),
        ("region-valparaiso", "Valparaíso"),
        ("region-metropolitana", "Metropolitana"),
        ("region-ohiggins", "O'Higgins"),
        ("region-maule", "Maule"),
        ("region-nuble", "Ñuble"),
        ("region-biobio", "Biobío"),
        ("region-araucania", "Araucanía"),
        ("region-los-rios", "Los Ríos"),
        ("region-los-lagos", "Los Lagos"),
        ("region-aysen", "Aysén"),
        ("region-magallanes", "Magallanes"),
    ]

    session = requests.Session()
    remates = []
    ids_vistos = set()

    for slug, nombre_region in REGIONES:
        url = f"{BASE}/remates/{slug}/" if slug else f"{BASE}/"
        log.info(f"  -> {url}")
        soup = get_soup(url, session)
        if not soup:
            continue

        tabla = soup.select_one("table")
        if not tabla:
            log.info(f"     Sin tabla")
            continue

        filas = tabla.select("tbody tr") or tabla.select("tr")
        nuevos = 0

        for fila in filas:
            celdas = fila.select("td")
            if len(celdas) < 5:
                continue
            try:
                # Fecha remate (col 2)
                fecha_display, fecha_sort = parsear_fecha(celdas[2].get_text(strip=True))

                # Región (col 4)
                region = celdas[4].get_text(strip=True) if len(celdas) > 4 else nombre_region

                # Comuna (col 5)
                comuna = celdas[5].get_text(strip=True) if len(celdas) > 5 else ""

                # Tipo (col 6)
                tipo_raw = celdas[6].get_text(strip=True) if len(celdas) > 6 else ""
                tipo = normalizar_tipo(tipo_raw)

                # Maps y dirección (col 7)
                url_maps = ""
                direccion = ""
                if len(celdas) > 7:
                    maps_a = celdas[7].select_one("a[href]")
                    if maps_a:
                        url_maps = maps_a.get("href", "")
                        # Extraer dirección de la URL de maps
                        m = re.search(r"/maps/search/(.+)", url_maps)
                        if m:
                            dir_raw = m.group(1)
                            # Decodificar caracteres comunes
                            for enc, char in [
                                ("%C3%B1","ñ"),("%C3%A9","é"),("%C3%B3","ó"),
                                ("%C3%A1","á"),("%C3%AD","í"),("%C3%BA","ú"),
                                ("%C3%91","Ñ"),("%C3%89","É"),("%27","'"),
                                ("+"," "),
                            ]:
                                dir_raw = dir_raw.replace(enc, char)
                            # Quitar ciudad/región/chile del final
                            dir_raw = re.sub(
                                rf"\s*{re.escape(comuna.lower())}.*$", "",
                                dir_raw, flags=re.IGNORECASE
                            )
                            dir_raw = re.sub(r"\s*chile\s*$", "", dir_raw, flags=re.IGNORECASE)
                            dir_raw = re.sub(r"\s{2,}", " ", dir_raw)
                            direccion = dir_raw.strip().title()

                # M2 (col 8)
                metros2 = celdas[8].get_text(strip=True) if len(celdas) > 8 else ""

                # Precio (col 9)
                precio_raw = celdas[9].get_text(strip=True) if len(celdas) > 9 else ""
                precio_clp, precio_uf = precio_a_clp_uf(precio_raw)

                # URL ficha (col 10)
                url_ficha = ""
                if len(celdas) > 10:
                    a = celdas[10].select_one("a[href]")
                    if a:
                        href = a.get("href", "")
                        url_ficha = href if href.startswith("http") else BASE + href

                # ID único y deduplicar
                rid = generar_id(url_ficha, fecha_sort, comuna)
                if rid in ids_vistos:
                    continue
                ids_vistos.add(rid)

                r = Remate(
                    id=rid,
                    tipo=tipo,
                    region=region,
                    comuna=comuna,
                    direccion=direccion,
                    fecha_remate=fecha_display,
                    fecha_sort=fecha_sort,
                    precio_clp=precio_clp,
                    precio_uf=precio_uf,
                    metros2=metros2,
                    url_ficha=url_ficha,
                    url_maps=url_maps,
                )
                remates.append(r)
                nuevos += 1

            except Exception as e:
                log.warning(f"     Error en fila: {e}")

        log.info(f"     {nuevos} nuevos en {nombre_region or 'todas las regiones'}")

    return remates


# ══════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════
def main():
    log.info("")
    log.info("=" * 60)
    log.info("  SCRAPER — mipropiedadchile.cl")
    log.info(f"  {datetime.now().strftime('%Y-%m-%d  %H:%M:%S')}")
    log.info("=" * 60)

    remates = scrape()

    # Ordenar por fecha más próxima
    remates.sort(key=lambda r: r.fecha_sort or "9999")

    # Guardar CSV limpio
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(Remate.headers())
        for r in remates:
            w.writerow(r.to_row())

    log.info("")
    log.info("=" * 60)
    log.info(f"  COMPLETADO")
    log.info(f"  {len(remates)} remates encontrados y ordenados por fecha")
    log.info(f"  Archivo: {OUTPUT_CSV}")
    log.info(f"  Abrir en Excel: start {OUTPUT_CSV}")
    log.info("=" * 60)
    log.info("")
    print("")
    print(f"  Listo. {len(remates)} remates guardados en '{OUTPUT_CSV}'")
    print(f"  Ejecuta 'start {OUTPUT_CSV}' para abrirlo en Excel.")
    print("")


if __name__ == "__main__":
    main()
