#!/usr/bin/env python3
"""
Scraper de Remates Judiciales — rematesinmobiliarios.cl
Genera: docs/remates.json (para la web) + remates_limpio.csv
"""

import re
import csv
import json
import time
import hashlib
import logging
from datetime import datetime, date
from dataclasses import dataclass, field
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ─── CONFIGURACION ─────────────────────────────────────
OUTPUT_CSV  = "remates_limpio.csv"
OUTPUT_JSON = "docs/remates.json"
LOG_FILE    = "scraper.log"
DELAY       = 1.5
UF          = 39841.72  # actualizar semanalmente

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
    id:              str = ""
    tipo:            str = ""
    region:          str = ""
    comuna:          str = ""
    direccion:       str = ""
    fecha_remate:    str = ""   # DD/MM/YYYY para mostrar
    fecha_sort:      str = ""   # YYYY-MM-DD para ordenar
    precio_clp:      str = ""
    precio_uf:       str = ""
    metros2:         str = ""
    url_ficha:       str = ""
    url_maps:        str = ""
    fecha_publicacion: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d"))

    @staticmethod
    def headers():
        return [
            "id", "tipo", "region", "comuna", "direccion",
            "fecha_remate", "precio_clp", "precio_uf",
            "metros2", "url_ficha", "url_maps", "fecha_publicacion"
        ]

    def to_row(self):
        return [
            self.id, self.tipo, self.region, self.comuna, self.direccion,
            self.fecha_remate, self.precio_clp, self.precio_uf,
            self.metros2, self.url_ficha, self.url_maps, self.fecha_publicacion
        ]

    def to_dict(self):
        return {
            "id":               self.id,
            "tipo":             self.tipo,
            "region":           self.region,
            "comuna":           self.comuna,
            "direccion":        self.direccion,
            "fecha_remate":     self.fecha_remate,
            "fecha_sort":       self.fecha_sort,
            "precio_clp":       self.precio_clp,
            "precio_uf":        self.precio_uf,
            "metros2":          self.metros2,
            "url_ficha":        self.url_ficha,
            "url_maps":         self.url_maps,
            "fecha_publicacion": self.fecha_publicacion,
        }

# ─── UTILIDADES ────────────────────────────────────────
def get_soup(url: str, session: requests.Session) -> Optional[BeautifulSoup]:
    try:
        r = session.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        time.sleep(DELAY)
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        log.warning(f"ERROR {url}: {e}")
        return None

def parsear_fecha(texto: str) -> tuple:
    if not texto:
        return "", ""
    texto = texto.strip()
    m = re.search(r"(\d{1,2})[\-/](\d{1,2})[\-/](\d{4})", texto)
    if m:
        d, mo, y = m.group(1).zfill(2), m.group(2).zfill(2), m.group(3)
        return f"{d}/{mo}/{y}", f"{y}-{mo}-{d}"
    return texto, texto

def es_futuro(fecha_sort: str) -> bool:
    """Retorna True si la fecha del remate es hoy o en el futuro."""
    if not fecha_sort or len(fecha_sort) != 10:
        return True  # si no tiene fecha, la incluimos igual
    try:
        fecha = date.fromisoformat(fecha_sort)
        return fecha >= date.today()
    except Exception:
        return True

def precio_a_clp_uf(texto: str) -> tuple:
    if not texto:
        return "", ""
    num_str = texto.replace("$", "").replace(".", "").replace(",", "").strip()
    try:
        num   = int(num_str)
        uf    = round(num / UF, 1)
        clp   = "$" + f"{num:,}".replace(",", ".")
        uf_fmt = f"{uf:,.1f}".replace(",", ".")
        return clp, uf_fmt
    except Exception:
        return texto, ""

def normalizar_tipo(raw: str) -> str:
    mapa = {
        "departamento": "Departamento", "depto": "Departamento",
        "casa": "Casa", "sitio": "Sitio/Terreno",
        "terreno": "Sitio/Terreno", "lote": "Sitio/Terreno",
        "parcela": "Parcela", "predio": "Parcela",
        "predio agrícola": "Parcela", "predio agricola": "Parcela",
        "parcela con casa": "Parcela con Casa",
        "local": "Local Comercial", "local comercial": "Local Comercial",
        "oficina": "Oficina", "bodega": "Bodega",
        "estacionamiento": "Estacionamiento", "industrial": "Industrial",
        "propiedad": "Inmueble", "inmueble": "Inmueble",
        "derechos de propiedad": "Derechos de Propiedad",
    }
    return mapa.get(raw.lower().strip(), raw.title())

def generar_id(url_ficha: str, fecha: str, comuna: str) -> str:
    raw = f"{url_ficha}|{fecha}|{comuna}"
    return hashlib.md5(raw.encode()).hexdigest()[:10].upper()

# ══════════════════════════════════════════════════════
# SCRAPER — rematesinmobiliarios.cl
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

    session   = requests.Session()
    remates   = []
    ids_vistos = set()
    hoy_str   = date.today().strftime("%Y-%m-%d")

    for slug, nombre_region in REGIONES:
        url  = f"{BASE}/remates/{slug}/" if slug else f"{BASE}/"
        log.info(f"-> {url}")
        soup = get_soup(url, session)
        if not soup:
            continue

        tabla = soup.select_one("table")
        if not tabla:
            log.info("   Sin tabla")
            continue

        filas  = tabla.select("tbody tr") or tabla.select("tr")
        nuevos = 0

        for fila in filas:
            celdas = fila.select("td")
            if len(celdas) < 5:
                continue
            try:
                fecha_display, fecha_sort = parsear_fecha(celdas[2].get_text(strip=True))

                # ── FILTRAR REMATES PASADOS ──────────────────────
                if not es_futuro(fecha_sort):
                    continue

                region = celdas[4].get_text(strip=True) if len(celdas) > 4 else nombre_region
                comuna = celdas[5].get_text(strip=True) if len(celdas) > 5 else ""
                tipo   = normalizar_tipo(celdas[6].get_text(strip=True) if len(celdas) > 6 else "")

                url_maps  = ""
                direccion = ""
                if len(celdas) > 7:
                    maps_a = celdas[7].select_one("a[href]")
                    if maps_a:
                        url_maps = maps_a.get("href", "")
                        m = re.search(r"/maps/search/(.+)", url_maps)
                        if m:
                            dir_raw = m.group(1)
                            for enc, char in [
                                ("%C3%B1","ñ"),("%C3%A9","é"),("%C3%B3","ó"),
                                ("%C3%A1","á"),("%C3%AD","í"),("%C3%BA","ú"),
                                ("%C3%91","Ñ"),("%C3%89","É"),("%27","'"),("+"," "),
                            ]:
                                dir_raw = dir_raw.replace(enc, char)
                            dir_raw = re.sub(rf"\s*{re.escape(comuna.lower())}.*$", "", dir_raw, flags=re.IGNORECASE)
                            dir_raw = re.sub(r"\s*chile\s*$", "", dir_raw, flags=re.IGNORECASE)
                            dir_raw = re.sub(r"\s{2,}", " ", dir_raw)
                            direccion = dir_raw.strip().title()

                metros2   = celdas[8].get_text(strip=True) if len(celdas) > 8 else ""
                precio_clp, precio_uf = precio_a_clp_uf(celdas[9].get_text(strip=True) if len(celdas) > 9 else "")

                url_ficha = ""
                if len(celdas) > 10:
                    a = celdas[10].select_one("a[href]")
                    if a:
                        href = a.get("href", "")
                        url_ficha = href if href.startswith("http") else BASE + href

                rid = generar_id(url_ficha, fecha_sort, comuna)
                if rid in ids_vistos:
                    continue
                ids_vistos.add(rid)

                r = Remate(
                    id=rid, tipo=tipo, region=region, comuna=comuna,
                    direccion=direccion, fecha_remate=fecha_display,
                    fecha_sort=fecha_sort, precio_clp=precio_clp,
                    precio_uf=precio_uf, metros2=metros2,
                    url_ficha=url_ficha, url_maps=url_maps,
                    fecha_publicacion=hoy_str,
                )
                remates.append(r)
                nuevos += 1

            except Exception as e:
                log.warning(f"   Error en fila: {e}")

        log.info(f"   {nuevos} nuevos en {nombre_region or 'todas las regiones'}")

    return remates

# ══════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════
def main():
    import os
    os.makedirs("docs", exist_ok=True)

    log.info("=" * 60)
    log.info(" SCRAPER — rematesinmobiliarios.cl")
    log.info(f" {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    remates = scrape()
    remates.sort(key=lambda r: r.fecha_sort or "9999")

    # ── CSV ─────────────────────────────────────────────
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(Remate.headers())
        for r in remates:
            w.writerow(r.to_row())

    # ── JSON para la web ─────────────────────────────────
    meta = {
        "actualizado": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "total": len(remates),
        "remates": [r.to_dict() for r in remates],
    }
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    log.info(f"COMPLETADO — {len(remates)} remates vigentes")
    print(f"\n✅ Listo. {len(remates)} remates vigentes guardados.\n")

if __name__ == "__main__":
    main()
