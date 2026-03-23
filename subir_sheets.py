#!/usr/bin/env python3
"""
Sube remates_limpio.csv a Google Sheets.
Se ejecuta automáticamente desde GitHub Actions.
"""

import os
import csv
import json
import gspread
from google.oauth2.service_account import Credentials

# Lee credenciales desde variable de entorno (GitHub Secret)
creds_json = os.environ.get("GOOGLE_CREDS")
sheet_id   = os.environ.get("SHEET_ID")

if not creds_json or not sheet_id:
    print("ERROR: Faltan variables GOOGLE_CREDS o SHEET_ID")
    exit(1)

# Conectar a Google Sheets
creds_dict = json.loads(creds_json)
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
creds  = Credentials.from_service_account_info(creds_dict, scopes=scopes)
client = gspread.authorize(creds)

# Abrir el Sheet
sheet = client.open_by_key(sheet_id)

# Usar primera hoja o crearla
try:
    ws = sheet.get_worksheet(0)
except Exception:
    ws = sheet.add_worksheet("Remates", rows=2000, cols=20)

# Leer CSV
filas = []
with open("remates_limpio.csv", encoding="utf-8-sig") as f:
    reader = csv.reader(f)
    for row in reader:
        filas.append(row)

# Limpiar y escribir
ws.clear()
ws.update("A1", filas)

# Formato header
ws.format("A1:L1", {
    "backgroundColor": {"red": 0.067, "green": 0.282, "blue": 0.549},
    "textFormat": {
        "foregroundColor": {"red": 1, "green": 1, "blue": 1},
        "bold": True
    }
})

print(f"Subido correctamente: {len(filas)-1} remates a Google Sheets")
print(f"Sheet ID: {sheet_id}")
