# app.py
import time
import os
import requests
from datetime import datetime, timezone
import pandas as pd
from pyhomebroker import HomeBroker
import pytz
from collections import defaultdict
import gc

# Supabase config
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")

# Credenciales de broker
broker = int(os.getenv("BROKER_ID"))
dni = os.getenv("DNI")
user = os.getenv("USER")
password = os.getenv("PASSWORD")

contador_categorias = defaultdict(int)

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
        record["updated_at"] = datetime.now(timezone.utc).isoformat()
        if not record.get("symbol"):
            record["symbol"] = "CAUCION"
        response = requests.post(url, headers=headers, json=record)
        if response.status_code not in (200, 201):
            print(f"❌ Error Supabase [{tabla}] → {response.status_code}: {response.text}")
        contador_categorias[tabla] += 1

def clasificar_symbol(symbol):
    symbol = symbol.upper()

    if symbol.endswith("C"):
        return None

    tasa_fija = {"S31M5", "S16A5", "BBA2S", "S28A5", "S16Y5", "BBY5", "S30Y5", "S18J5", "BJ25", "S30J5", "S31L5", "S29G5", "S29S5", "S30S5", "T17O5", "S30L5", "S10N5", "S28N5", "T30E6", "T3F6", "T30J6", "T15E7"}
    bonos_soberanos = {"AL29", "AL29D", "AL30", "AL30D", "AL35", "AL35D", "AL41D", "AL41", "AL14D", "GD29", "GD29D", "GD30", "GD30D", "GD35", "GD35D", "GD38", "GD38D", "GD41", "GD41D", "GD46", "GD46D"}
    dolar_linked = {"TV25", "TZV25", "TZVD5", "D16F6", "TZV26"}
    bopreales = {"BPJ5D", "BPA7D", "BPB7D", "BPC7D", "BPD7D"}
    bonos_cer = {"TZXM5", "TC24", "TZXJ5", "TZX05", "TZXKD5", "TZXM6", "TX06", "TX26", "TZXM7", "TX27", "TXD7", "TX28"}
    cauciones = {"CAUCI1", "CAUCI2"}
    futuros_dolar = {"DOFUTABR24", "DOFUTJUN24"}

    if symbol in tasa_fija:
        return "tasa_fija"
    elif symbol in bonos_soberonos:
        return "bonos_soberanos"
    elif symbol in dolar_linked:
        return "dolar_linked"
    elif symbol in bopreales:
        return "bopreales"
    elif symbol in bonos_cer:
        return "bonos_cer"
    elif symbol in cauciones:
        return "cauciones"
    elif symbol in futuros_dolar:
        return "futuros_dolar"
    else:
        return None

def on_securities(online, quotes):
    thisData = quotes.reset_index()
    thisData["symbol"] = thisData["symbol"] + " - " + thisData["settlement"]
    thisData = thisData.drop(["settlement"], axis=1)
    thisData["change"] = thisData["change"] / 100
    thisData["datetime"] = pd.to_datetime(thisData["datetime"])

    for _, row in thisData.iterrows():
        symbol = row["symbol"].split(" - ")[0]
        tabla = clasificar_symbol(symbol)
        if tabla:
            guardar_en_supabase(tabla, pd.DataFrame([row]))

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

def on_options(online, quotes):
    pass

def on_error(online, error):
    print(f"Error Message Received: {error}")

def ejecutar_ciclo():
    global contador_categorias
    contador_categorias = defaultdict(int)

    hb = HomeBroker(
        broker,
        on_options=on_options,
        on_securities=on_securities,
        on_repos=on_repos,
        on_error=on_error
    )

    hb.auth.login(dni=dni, user=user, password=password, raise_exception=True)
    hb.online.connect()

    hb.online.subscribe_securities('government_bonds', '24hs')
    hb.online.subscribe_securities('short_term_government_bonds', '24hs')
    hb.online.subscribe_repos()

    print("✅ Conectado. Esperando 5 segundos para recibir datos...")
    time.sleep(5)
    hb.online.disconnect()

    print("📊 Resumen del ciclo:")
    for tabla, cantidad in contador_categorias.items():
        print(f"  - {tabla}: {cantidad} registros guardados")

    gc.collect()
    print("🧹 Memoria limpiada. Esperando 5 minutos para el próximo ciclo...")
    time.sleep(300)

def dentro_de_horario():
    ahora = datetime.now(pytz.timezone("America/Argentina/Buenos_Aires"))
    return ahora.hour >= 10 and ahora.hour < 17

if __name__ == "__main__":
    import sys
    inicio = time.time()

    while True:
        if dentro_de_horario():
            try:
                ejecutar_ciclo()
            except Exception as e:
                print(f"❌ Error en ciclo: {e}")
        else:
            print("🕒 Fuera de horario de mercado. Esperando 1 minuto...")
            time.sleep(60)

        if time.time() - inicio > 3600:
            print("♻️ Reinicio programado cada 1 hora")
            os.execv(sys.executable, ['python'] + sys.argv)
