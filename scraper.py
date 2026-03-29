#!/usr/bin/env python3
"""
Scraper de Remates Judiciales — mipropiedadchile.cl
Fuente: rematesinmobiliarios.cl

ESTRATEGIA v3:
- La paginacion del sitio es JavaScript dinamico (no links HTML).
- Solucion: lee todas las URLs de comunas desde la pagina principal,
  luego scrapea cada comuna individualmente (max 25 filas c/u).
  Asi captura todos los remates sin necesitar paginacion.
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

OUTPUT_CSV = "remates_limpio.csv"
LOG_FILE   = "scraper.log"
DELAY      = 1.0
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


@dataclass
class Remate:
    id:           str = ""
    tipo:         str = ""
    region:       str = ""
    comuna:       str = ""
    direccion:    str = ""
    fecha_remate: str = ""
    fecha_sort:   str = ""
    precio_clp:   str = ""
    precio_uf:    str = ""
    metros2:      str = ""
    url_ficha:    str = ""
    url_maps:     str = ""
    actualizado:  str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d"))

    @staticmethod
    def headers():
        return ["id","tipo","region","comuna","direccion",
                "fecha_remate","precio_clp","precio_uf",
                "metros2","url_ficha","url_maps","actualizado"]

    def to_row(self):
        return [self.id,self.tipo,self.region,self.comuna,self.direccion,
                self.fecha_remate,self.precio_clp,self.precio_uf,
                self.metros2,self.url_ficha,self.url_maps,self.actualizado]


def get_soup(url, session):
    try:
        r = session.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        time.sleep(DELAY)
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        log.warning(f"  ERROR {url}: {e}")
        return None


def parsear_fecha(texto):
    if not texto:
        return "", ""
    texto = texto.strip()
    m = re.search(r"(\d{1,2})[\-/](\d{1,2})[\-/](\d{4})", texto)
    if m:
        d, mo, y = m.group(1).zfill(2), m.group(2).zfill(2), m.group(3)
        return f"{d}/{mo}/{y}", f"{y}-{mo}-{d}"
    return texto, texto


def precio_a_clp_uf(texto):
    if not texto:
        return "", ""
    num_str = texto.replace("$","").replace(".","").replace(",","").strip()
    try:
        num    = int(num_str)
        uf     = round(num / UF, 1)
        clp    = "$" + f"{num:,}".replace(",",".")
        uf_fmt = f"{uf:,.1f}".replace(",",".")
        return clp, uf_fmt
    except Exception:
        return texto, ""


def normalizar_tipo(raw):
    mapa = {
        "departamento":"Departamento","depto":"Departamento",
        "casa":"Casa","vivienda":"Casa",
        "sitio":"Sitio/Terreno","terreno":"Sitio/Terreno","lote":"Sitio/Terreno",
        "parcela":"Parcela","predio":"Parcela",
        "predio agricola":"Parcela","predio agricola":"Parcela",
        "parcela con casa":"Parcela con Casa",
        "local":"Local Comercial","local comercial":"Local Comercial",
        "oficina":"Oficina","bodega":"Bodega",
        "galpon":"Galpon","industrial":"Industrial",
        "nave":"Industrial","edificio":"Edificio",
        "estacionamiento":"Estacionamiento",
        "derechos":"Derechos de Propiedad",
        "derechos de propiedad":"Derechos de Propiedad",
        "propiedad":"Inmueble","inmueble":"Inmueble",
    }
    return mapa.get(raw.lower().strip(), raw.title())


def generar_id(url_ficha, fecha, comuna):
    raw = f"{url_ficha}|{fecha}|{comuna}"
    return hashlib.md5(raw.encode()).hexdigest()[:10].upper()


def extraer_filas(soup, fallback_region, BASE):
    tabla = soup.select_one("table")
    if not tabla:
        return []
    remates = []
    filas = tabla.select("tbody tr") or tabla.select("tr")
    for fila in filas:
        celdas = fila.select("td")
        if len(celdas) < 10:
            continue
        try:
            fecha_display, fecha_sort = parsear_fecha(celdas[2].get_text(strip=True))
            region  = celdas[4].get_text(strip=True) if len(celdas) > 4 else fallback_region
            comuna  = celdas[5].get_text(strip=True) if len(celdas) > 5 else ""
            tipo    = normalizar_tipo(celdas[6].get_text(strip=True) if len(celdas) > 6 else "")
            url_maps  = ""
            direccion = ""
            if len(celdas) > 7:
                maps_a = celdas[7].select_one("a[href]")
                if maps_a:
                    url_maps = maps_a.get("href","")
                    m = re.search(r"/maps/search/(.+)", url_maps)
                    if m:
                        dr = m.group(1)
                        for enc,char in [
                            ("%C3%B1","n"),("%C3%A9","e"),("%C3%B3","o"),
                            ("%C3%A1","a"),("%C3%AD","i"),("%C3%BA","u"),
                            ("%27","'"),("+"," "),
                        ]:
                            dr = dr.replace(enc, char)
                        dr = re.sub(rf"\s*{re.escape(comuna.lower())}.*$","",dr,flags=re.IGNORECASE)
                        dr = re.sub(r"\s*chile\s*$","",dr,flags=re.IGNORECASE)
                        direccion = re.sub(r"\s{2,}"," ",dr).strip().title()
            metros2 = celdas[8].get_text(strip=True) if len(celdas) > 8 else ""
            precio_clp, precio_uf = precio_a_clp_uf(celdas[9].get_text(strip=True) if len(celdas) > 9 else "")
            url_ficha = ""
            if len(celdas) > 10:
                a = celdas[10].select_one("a[href]")
                if a:
                    href = a.get("href","")
                    url_ficha = href if href.startswith("http") else BASE + href
            remates.append(Remate(
                id=generar_id(url_ficha, fecha_sort, comuna),
                tipo=tipo, region=region, comuna=comuna, direccion=direccion,
                fecha_remate=fecha_display, fecha_sort=fecha_sort,
                precio_clp=precio_clp, precio_uf=precio_uf,
                metros2=metros2, url_ficha=url_ficha, url_maps=url_maps,
            ))
        except Exception as e:
            log.warning(f"  Error en fila: {e}")
    return remates


def obtener_urls_comunas(session, BASE):
    log.info("  Leyendo lista de comunas desde pagina principal...")
    soup = get_soup(BASE + "/", session)
    if not soup:
        return []
    comunas = []
    seen = set()
    for a in soup.select("a[href]"):
        href = a.get("href","")
        texto = a.get_text(strip=True)
        m = re.match(r"https?://www\.rematesinmobiliarios\.cl/remates/([^/]+)/$", href)
        if not m:
            continue
        slug = m.group(1)
        if slug.startswith("region-"):
            continue  # saltar regiones, solo comunas
        if href in seen:
            continue
        seen.add(href)
        count_m = re.search(r"\((\d+)\)", texto)
        count = int(count_m.group(1)) if count_m else 0
        nombre = re.sub(r"\s*\(\d+\)\s*$","",texto).strip()
        if nombre:
            comunas.append((href, nombre, count))
    log.info(f"  {len(comunas)} comunas encontradas")
    return comunas


def scrape():
    BASE    = "https://www.rematesinmobiliarios.cl"
    session = requests.Session()
    ids_vistos = set()
    remates    = []

    comunas = obtener_urls_comunas(session, BASE)

    total = len(comunas)
    for i, (url, nombre, count) in enumerate(comunas, 1):
        log.info(f"  [{i}/{total}] {nombre} ({count}) — {url}")
        soup = get_soup(url, session)
        if not soup:
            continue
        nuevos = extraer_filas(soup, nombre, BASE)
        agregados = 0
        for r in nuevos:
            if r.id not in ids_vistos:
                ids_vistos.add(r.id)
                remates.append(r)
                agregados += 1
        log.info(f"    -> {agregados} nuevos")

    return remates


def main():
    log.info("")
    log.info("="*60)
    log.info("  SCRAPER v3 — mipropiedadchile.cl")
    log.info(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("="*60)

    remates = scrape()
    remates.sort(key=lambda r: r.fecha_sort or "9999")

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(Remate.headers())
        for r in remates:
            w.writerow(r.to_row())

    log.info("")
    log.info("="*60)
    log.info(f"  COMPLETADO — {len(remates)} remates")
    log.info(f"  Archivo: {OUTPUT_CSV}")
    log.info("="*60)
    print(f"\n  Listo. {len(remates)} remates guardados en '{OUTPUT_CSV}'\n")


if __name__ == "__main__":
    main()
