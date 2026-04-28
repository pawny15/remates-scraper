"""
scraper_v5.py — mipropiedadchile.cl
Scraper que extrae TODOS los remates desde rematesinmobiliarios.cl
iterando por toda la paginación hasta capturar todos los registros.
"""

import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
import os
import time
import random
from urllib.parse import urljoin

BASE_URL = "https://www.rematesinmobiliarios.cl/"
OUTPUT_FILE = "remates.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-CL,es;q=0.9",
    "Referer": "https://www.google.cl/",
}

def clean(text):
    return " ".join(text.split()) if text else ""

def parse_fecha(texto):
    """Convierte '27-05-2026' -> 'yyyy-mm-dd' para ordenar."""
    try:
        return datetime.strptime(texto.strip(), "%d-%m-%Y").strftime("%Y-%m-%d")
    except Exception:
        return ""

def scrape():
    remates = []
    current_url = BASE_URL
    page_num = 1

    while current_url:
        print(f"[{datetime.now():%H:%M:%S}] Extrayendo página {page_num}: {current_url}")
        
        try:
            r = requests.get(current_url, headers=HEADERS, timeout=40)
            r.raise_for_status()
        except requests.RequestException as e:
            print(f"ERROR al obtener la página {page_num}: {e}")
            break  # Detenemos la paginación pero guardamos lo recolectado hasta el momento

        soup = BeautifulSoup(r.text, "html.parser")

        # Buscar la primera tabla con ficha-remate
        table = None
        for t in soup.find_all("table"):
            if t.find("a", href=re.compile(r"ficha-remate")):
                table = t
                break

        if not table:
            print(f"No se encontró tabla de remates en la página {page_num}. Terminando extracción.")
            break

        rows = table.find_all("tr")[1:]  # saltar header
        print(f"Filas encontradas en página {page_num}: {len(rows)}")

        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 9:
                continue

            try:
                # Extraer link de detalle
                link_tag = row.find("a", href=re.compile(r"ficha-remate"))
                ficha_url = ""
                remate_id = ""
                if link_tag:
                    ficha_url = urljoin(BASE_URL, link_tag["href"].lstrip("/"))
                    m = re.search(r"id=(\d+)", ficha_url)
                    remate_id = m.group(1) if m else ""

                # Maps link
                maps_tag = row.find("a", href=re.compile(r"maps"))
                maps_url = maps_tag["href"] if maps_tag else ""

                # Limpiar texto de cada columna
                fecha_texto = clean(cols[2].get_text())
                publicado_texto = clean(cols[3].get_text())
                region = clean(cols[4].get_text())
                comuna = clean(cols[5].get_text())
                tipo = clean(cols[6].get_text())
                m2_texto = clean(cols[8].get_text()) if len(cols) > 8 else ""
                minimo_texto = clean(cols[9].get_text()) if len(cols) > 9 else ""

                # Precio numérico (remover $, puntos)
                precio_num = re.sub(r"[^\d]", "", minimo_texto)

                remates.append({
                    "id": remate_id,
                    "tipo": tipo,
                    "fecha_remate": fecha_texto,
                    "fecha_sort": parse_fecha(fecha_texto),
                    "publicado": publicado_texto,
                    "region": region,
                    "comuna": comuna,
                    "m2": m2_texto,
                    "precio_clp": minimo_texto,
                    "precio_num": int(precio_num) if precio_num else 0,
                    "precio_uf": "",          
                    "maps_url": maps_url,
                    "ficha_url": ficha_url,
                    "url": ficha_url,
                })
            except Exception as e:
                print(f"Advertencia: Error aislando fila en página {page_num}: {e}. Omitiendo registro.")
                continue

        # Lógica de paginación: buscar el botón de la página siguiente
        next_page_str = str(page_num + 1)
        # Busca el enlace que contenga exactamente el número de la siguiente página
        next_link = soup.find("a", string=re.compile(rf"^\s*{next_page_str}\s*$"))
        
        if next_link and 'href' in next_link.attrs:
            current_url = urljoin(BASE_URL, next_link['href'])
            page_num += 1
            # Pausa aleatoria entre 1.5 y 3 segundos para evitar ban de IP por exceso de peticiones
            time.sleep(random.uniform(1.5, 3.0))
        else:
            current_url = None # Ya no hay más páginas

    print(f"\n====================================")
    print(f"Total de remates parseados: {len(remates)}")
    print(f"====================================")

    # Ordenar por fecha más próxima
    remates.sort(key=lambda x: x["fecha_sort"] or "9999")

    result = {
        "actualizacion": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "total": len(remates),
        "remates": remates,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"✅ Guardado exitoso en {OUTPUT_FILE}")
    return remates

if __name__ == "__main__":
    scrape()
