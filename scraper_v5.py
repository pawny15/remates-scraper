import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
import time
import random
from urllib.parse import urljoin

BASE_URL = "https://www.rematesinmobiliarios.cl/"
OUTPUT_FILE = "remates.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "es-CL,es;q=0.9",
    "Referer": "https://www.google.cl/",
}

def clean(text):
    return " ".join(text.split()) if text else ""

def parse_fecha(texto):
    try:
        return datetime.strptime(texto.strip(), "%d-%m-%Y").strftime("%Y-%m-%d")
    except:
        return ""

def scrape():
    remates = []
    current_url = BASE_URL
    page_num = 1

    while current_url:
        print(f"[{datetime.now():%H:%M:%S}] Extrayendo página {page_num}...")
        try:
            r = requests.get(current_url, headers=HEADERS, timeout=40)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            
            table = None
            for t in soup.find_all("table"):
                if t.find("a", href=re.compile(r"ficha-remate")):
                    table = t
                    break
            
            if not table: break

            rows = table.find_all("tr")[1:]
            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 9: continue
                
                link_tag = row.find("a", href=re.compile(r"ficha-remate"))
                ficha_url = urljoin(BASE_URL, link_tag["href"].lstrip("/")) if link_tag else ""
                
                fecha_texto = clean(cols[2].get_text())
                precio_num = re.sub(r"[^\d]", "", clean(cols[9].get_text()))

                remates.append({
                    "id": re.search(r"id=(\d+)", ficha_url).group(1) if ficha_url else str(random.getrandbits(32)),
                    "tipo": clean(cols[6].get_text()),
                    "fecha_remate": fecha_texto,
                    "fecha_sort": parse_fecha(fecha_texto),
                    "region": clean(cols[4].get_text()),
                    "comuna": clean(cols[5].get_text()),
                    "direccion": clean(cols[0].get_text()), # Ajustado según estructura
                    "precio_clp": clean(cols[9].get_text()),
                    "precio_uf": clean(cols[9].get_text()), # Se recalcula en el navegador
                    "url_ficha": ficha_url
                })

            # Buscar siguiente página
            next_page = str(page_num + 1)
            next_link = soup.find("a", string=re.compile(rf"^\s*{next_page}\s*$"))
            if next_link:
                current_url = urljoin(BASE_URL, next_link['href'])
                page_num += 1
                time.sleep(random.uniform(1, 2))
            else:
                current_url = None
        except Exception as e:
            print(f"Error: {e}")
            break

    result = {"actualizacion": datetime.now().strftime("%Y-%m-%d %H:%M"), "total": len(remates), "remates": remates}
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    return remates

if __name__ == "__main__":
    scrape()
