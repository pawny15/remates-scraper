#!/usr/bin/env python3
"""
Scraper v4 — mipropiedadchile.cl
Estrategia: URLs de comunas hardcodeadas (la pagina principal carga comunas via JS).
Cada URL de comuna devuelve max ~25-55 remates en una sola pagina sin paginacion.
"""

import re, csv, time, hashlib, logging
from datetime import datetime
from dataclasses import dataclass, field
import requests
from bs4 import BeautifulSoup

OUTPUT_CSV = "remates_limpio.csv"
LOG_FILE   = "scraper.log"
DELAY      = 0.8
UF         = 39841.72

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "es-CL,es;q=0.9",
    "Referer": "https://www.rematesinmobiliarios.cl/",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# Todas las comunas con remates — extraidas el 2026-03-29
# Formato: (slug, count_aproximado)
COMUNAS = [
    ("santiago", 55), ("estacion-central", 27), ("maipu", 25),
    ("puente-alto", 20), ("la-florida", 17), ("nunoa", 17),
    ("san-miguel", 16), ("las-condes", 15), ("san-bernardo", 15),
    ("independencia", 14), ("lampa", 11), ("antofagasta", 11),
    ("quinta-normal", 10), ("colbun", 10), ("providencia", 10),
    ("conchali", 10), ("coquimbo", 9), ("recoleta", 9),
    ("quilicura", 9), ("buin", 8), ("temuco", 8),
    ("colina", 8), ("pudahuel", 8), ("lo-barnechea", 7),
    ("la-serena", 7), ("puerto-montt", 7), ("talagante", 7),
    ("cerrillos", 7), ("penalolen", 6), ("paine", 6),
    ("rancagua", 6), ("san-joaquin", 6), ("san-pedro-de-la-paz", 6),
    ("huechuraba", 6), ("la-cisterna", 6), ("la-pintana", 5),
    ("vina-del-mar", 5), ("los-angeles", 5), ("pucon", 4),
    ("puerto-varas", 4), ("macul", 4), ("valparaiso", 4),
    ("algarrobo", 4), ("aysen", 4), ("vitacura", 3),
    ("linares", 3), ("melipilla", 3), ("penaflor", 3),
    ("los-muermos", 3), ("curico", 3), ("vallenar", 3),
    ("arica", 3), ("concepcion", 3), ("las-cabras", 3),
    ("purranque", 3), ("renca", 3), ("la-reina", 3),
    ("copiapo", 2), ("la-granja", 2), ("el-monte", 2),
    ("punta-arenas", 2), ("calama", 2), ("pirque", 2),
    ("el-bosque", 2), ("padre-hurtado", 2), ("ovalle", 2),
    ("chillan", 2), ("osorno", 2), ("quilpue", 2),
    ("machali", 2), ("isla-de-maipo", 2), ("alto-hospicio", 2),
    ("los-andes", 2), ("futrono", 2), ("cerro-navia", 2),
    ("san-vicente", 1), ("villa-alemana", 1), ("catemu", 1),
    ("ancud", 1), ("villarrica", 1), ("rinconada", 1),
    ("cartagena", 1), ("victoria", 1), ("calle-larga", 1),
    ("vicuna", 1), ("calera-de-tango", 1), ("til-til", 1),
    ("san-ramon", 1), ("teno", 1), ("arkansas", 1),
    ("talca", 1), ("cabildo", 1), ("la-higuera", 1),
    ("quintero", 1), ("maria-pinto", 1), ("labranza", 1),
    ("galvarino", 1), ("lautaro", 1), ("limache", 1),
    ("llay-llay", 1), ("fresia", 1), ("lo-prado", 1),
    ("lonquimay", 1), ("estacion-18-central", 1), ("el-tabo", 1),
    ("maullin", 1), ("chiguayante", 1), ("mulchen", 1),
    ("navidad", 1), ("pedro-aguirre-cerda", 1), ("pitrufquen", 1),
    ("coyhaique", 1), ("concon", 1), ("puerto-natales", 1),
    ("chillan-viejo", 1), ("putaendo", 1), ("puyehue", 1),
    ("coronel", 1),
]

BASE = "https://www.rematesinmobiliarios.cl"

@dataclass
class Remate:
    id: str = ""
    tipo: str = ""
    region: str = ""
    comuna: str = ""
    direccion: str = ""
    fecha_remate: str = ""
    fecha_sort: str = ""
    precio_clp: str = ""
    precio_uf: str = ""
    metros2: str = ""
    url_ficha: str = ""
    url_maps: str = ""
    actualizado: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d"))

    @staticmethod
    def headers():
        return ["id","tipo","region","comuna","direccion","fecha_remate",
                "precio_clp","precio_uf","metros2","url_ficha","url_maps","actualizado"]

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
    if not texto: return "", ""
    m = re.search(r"(\d{1,2})[\-/](\d{1,2})[\-/](\d{4})", texto.strip())
    if m:
        d, mo, y = m.group(1).zfill(2), m.group(2).zfill(2), m.group(3)
        return f"{d}/{mo}/{y}", f"{y}-{mo}-{d}"
    return texto, texto


def precio_a_clp_uf(texto):
    if not texto: return "", ""
    try:
        num = int(texto.replace("$","").replace(".","").replace(",","").strip())
        return "$"+f"{num:,}".replace(",","."), f"{round(num/UF,1):,.1f}".replace(",",".")
    except:
        return texto, ""


def normalizar_tipo(raw):
    mapa = {
        "departamento":"Departamento","depto":"Departamento",
        "casa":"Casa","vivienda":"Casa",
        "sitio":"Sitio/Terreno","terreno":"Sitio/Terreno","lote":"Sitio/Terreno",
        "parcela":"Parcela","predio":"Parcela","predio agricola":"Parcela",
        "parcela con casa":"Parcela con Casa",
        "local":"Local Comercial","local comercial":"Local Comercial",
        "oficina":"Oficina","bodega":"Bodega","galpon":"Galpon",
        "industrial":"Industrial","nave":"Industrial","edificio":"Edificio",
        "estacionamiento":"Estacionamiento",
        "derechos":"Derechos de Propiedad","derechos de propiedad":"Derechos de Propiedad",
        "propiedad":"Inmueble","inmueble":"Inmueble",
    }
    return mapa.get(raw.lower().strip(), raw.title())


def generar_id(url_ficha, fecha, comuna):
    return hashlib.md5(f"{url_ficha}|{fecha}|{comuna}".encode()).hexdigest()[:10].upper()


def extraer_filas(soup, fallback_region):
    tabla = soup.select_one("table")
    if not tabla: return []
    remates = []
    for fila in (tabla.select("tbody tr") or tabla.select("tr")):
        celdas = fila.select("td")
        if len(celdas) < 10: continue
        try:
            fecha_display, fecha_sort = parsear_fecha(celdas[2].get_text(strip=True))
            region  = celdas[4].get_text(strip=True) or fallback_region
            comuna  = celdas[5].get_text(strip=True) if len(celdas) > 5 else ""
            tipo    = normalizar_tipo(celdas[6].get_text(strip=True) if len(celdas) > 6 else "")
            url_maps = direccion = ""
            if len(celdas) > 7:
                a = celdas[7].select_one("a[href]")
                if a:
                    url_maps = a.get("href","")
                    m = re.search(r"/maps/search/(.+)", url_maps)
                    if m:
                        dr = m.group(1)
                        for enc,ch in [("%C3%B1","n"),("%C3%A9","e"),("%C3%B3","o"),
                                       ("%C3%A1","a"),("%C3%AD","i"),("%C3%BA","u"),("%27","'"),("+"," ")]:
                            dr = dr.replace(enc, ch)
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


def scrape():
    session    = requests.Session()
    ids_vistos = set()
    remates    = []
    total      = len(COMUNAS)

    for i, (slug, count) in enumerate(COMUNAS, 1):
        url = f"{BASE}/remates/{slug}/"
        log.info(f"  [{i}/{total}] {slug} (~{count}) — {url}")
        soup = get_soup(url, session)
        if not soup: continue
        nuevos = extraer_filas(soup, slug)
        agregados = 0
        for r in nuevos:
            if r.id not in ids_vistos:
                ids_vistos.add(r.id)
                remates.append(r)
                agregados += 1
        log.info(f"    -> {agregados} nuevos")

    return remates


def main():
    log.info("\n" + "="*60)
    log.info("  SCRAPER v4 — mipropiedadchile.cl")
    log.info(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("="*60)

    remates = scrape()
    remates.sort(key=lambda r: r.fecha_sort or "9999")

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(Remate.headers())
        for r in remates: w.writerow(r.to_row())

    log.info(f"\n{'='*60}\n  COMPLETADO — {len(remates)} remates\n  Archivo: {OUTPUT_CSV}\n{'='*60}")
    print(f"\n  Listo. {len(remates)} remates guardados en '{OUTPUT_CSV}'\n")

if __name__ == "__main__":
    main()
