# app.py
import time
import os
import requests
from datetime import datetime
import pandas as pd
from pyhomebroker import HomeBroker
import pytz

# Supabase config
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")

# Credenciales de broker
broker = int(os.getenv("BROKER_ID"))
dni = os.getenv("DNI")
user = os.getenv("USER")
password = os.getenv("PASSWORD")

def guardar_en_supabase(tabla, rows):
    url = f"{SUPABASE_URL}/rest/v1/{tabla}"
    headers = {
        "apikey": SUPABASE_API_KEY,
        "Authorization": f"Bearer {SUPABASE_API_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }
    data = rows.to_dict(orient="records")
    for record in data:
        record["updated_at"] = datetime.utcnow().isoformat()
        response = requests.post(url, headers=headers, json=record)
        print(f"[{tabla}] {record.get('symbol')} → {response.status_code}")

def on_options(online, quotes):
    thisData = quotes.drop(['expiration', 'strike', 'kind'], axis=1)
    thisData["change"] = thisData["change"] / 100
    thisData["datetime"] = pd.to_datetime(thisData["datetime"])
    thisData = thisData.rename(columns={"bid_size": "bidsize", "ask_size": "asksize"})
    guardar_en_supabase("opciones", thisData.reset_index())

def on_securities(online, quotes):
    thisData = quotes.reset_index()
    thisData["symbol"] = thisData["symbol"] + " - " + thisData["settlement"]
    thisData = thisData.drop(["settlement"], axis=1)
    thisData["change"] = thisData["change"] / 100
    thisData["datetime"] = pd.to_datetime(thisData["datetime"])
    guardar_en_supabase("acciones", thisData)

def on_repos(online, quotes):
    thisData = quotes.reset_index()
    thisData = thisData.set_index("symbol")
    thisData = thisData[['PESOS' in s for s in quotes.index]]
    thisData = thisData.reset_index()
    thisData["settlement"] = pd.to_datetime(thisData["settlement"])
    thisData = thisData.set_index("settlement")
    thisData["last"] = thisData["last"] / 100
    thisData["bid_rate"] = thisData["bid_rate"] / 100
    thisData["ask_rate"] = thisData["ask_rate"] / 100
    thisData = thisData.drop(['open', 'high', 'low', 'volume', 'operations', 'datetime'], axis=1)
    thisData = thisData[['last', 'turnover', 'bid_amount', 'bid_rate', 'ask_rate', 'ask_amount']]
    guardar_en_supabase("cauciones", thisData.reset_index())

def on_error(online, error):
    print(f"Error Message Received: {error}")

def ejecutar_ciclo():
    hb = HomeBroker(
        broker,
        on_options=on_options,
        on_securities=on_securities,
        on_repos=on_repos,
        on_error=on_error
    )
    
    hb.auth.login(dni=dni, user=user, password=password, raise_exception=True)
    hb.online.connect()
    hb.online.subscribe_options()
    hb.online.subscribe_securities('bluechips', '24hs')
    hb.online.subscribe_securities('bluechips', 'SPOT')
    hb.online.subscribe_securities('government_bonds', '24hs')
    hb.online.subscribe_securities('government_bonds', 'SPOT')
    hb.online.subscribe_securities('cedears', '24hs')
    hb.online.subscribe_securities('general_board', '24hs')
    hb.online.subscribe_securities('short_term_government_bonds', '24hs')
    hb.online.subscribe_securities('corporate_bonds', '24hs')
    hb.online.subscribe_repos()

    print("✅ Conectado y escuchando durante 5 minutos...")
    time.sleep(300)  # Esperar 5 minutos
    print("🔁 Ciclo finalizado. Esperando próximo intervalo...")

def dentro_de_horario():
    ahora = datetime.now(pytz.timezone("America/Argentina/Buenos_Aires"))
    return ahora.hour >= 10 and ahora.hour < 17

if __name__ == "__main__":
    while True:
        if dentro_de_horario():
            try:
                ejecutar_ciclo()
            except Exception as e:
                print(f"❌ Error en ciclo: {e}")
        else:
            print("🕒 Fuera de horario de mercado. Esperando 1 minuto...")
            time.sleep(60)
