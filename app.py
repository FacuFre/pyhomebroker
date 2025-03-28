import os
import time
import requests
import gc
from datetime import datetime, timezone
from collections import defaultdict
import pandas as pd
from pyhomebroker import HomeBroker
import pytz

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")

if not SUPABASE_URL or not SUPABASE_API_KEY:
    raise ValueError("❌ SUPABASE_URL o SUPABASE_API_KEY no están configurados!")

broker = int(os.getenv("BROKER_ID"))
dni = os.getenv("DNI")
user = os.getenv("USER")
password = os.getenv("PASSWORD")

contador_categorias = defaultdict(int)

def guardar_en_supabase(tabla, rows):
    url = f"{SUPABASE_URL}/rest/v1/{tabla}?on_conflict=symbol"
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

        print(f"📤 Enviando a Supabase → {tabla} | Payload:")
        print(record)

        response = requests.post(url, headers=headers, json=record)
        if response.status_code not in (200, 201):
            print(f"❌ Error Supabase [{tabla}] → {response.status_code}: {response.text}")
        else:
            contador_categorias[tabla] += 1

def clasificar_symbol(symbol):
    symbol_clean = symbol.upper().split(" - ")[0].strip()

    tasa_fija = {"S31M5", "S16A5", "BBA2S", "S28A5", "S16Y5", "BBY5", "S30Y5", "S18J5", "BJ25",
                 "S30J5", "S31L5", "S29G5", "S29S5", "S30S5", "T17O5", "S30L5", "S10N5",
                 "S28N5", "T30E6", "T3F6", "T30J6", "T15E7"}
    bonos_soberanos = {"AL29", "AL29D", "AL30", "AL30D", "AL35", "AL35D", "AL41D", "AL41",
                       "AL14D", "GD29", "GD29D", "GD30", "GD30D", "GD35", "GD35D", "GD38",
                       "GD38D", "GD41", "GD41D", "GD46", "GD46D"}
    dolar_linked = {"TV25", "TZV25", "TZVD5", "D16F6", "TZV26"}
    bopreales = {"BPJ5D", "BPA7D", "BPB7D", "BPC7D", "BPD7D"}
    bonos_cer = {"TZXM5", "TC24", "TZXJ5", "TZX05", "TZXKD5", "TZXM6", "TX06", "TX26",
                 "TZXM7", "TX27", "TXD7", "TX28"}
    cauciones = {"CAUCI1", "CAUCI2"}
    futuros_dolar = {"DOFUTABR24", "DOFUTJUN24"}

    if symbol_clean in tasa_fija:
        return "tasa_fija"
    elif symbol_clean in bonos_soberanos:
        return "bonos_soberanos"
    elif symbol_clean in dolar_linked:
        return "dolar_linked"
    elif symbol_clean in bopreales:
        return "bopreales"
    elif symbol_clean in bonos_cer:
        return "bonos_cer"
    elif symbol_clean in cauciones:
        return "cauciones"
    elif symbol_clean in futuros_dolar:
        return "futuros_dolar"
    else:
        print(f"⚠️ No se reconoce el símbolo '{symbol_clean}'")
        return None

def on_securities(online, quotes):
    print(f"📥 [on_securities] Cantidad de instrumentos: {len(quotes)}")
    thisData = quotes.reset_index()

    if not thisData.empty:
        print(f"   Ejemplo row: {thisData.iloc[0].to_dict()}")

    thisData["symbol"] = thisData["symbol"] + " - " + thisData["settlement"]
    thisData = thisData.drop(["settlement"], axis=1)
    thisData["change"] = thisData["change"] / 100
    thisData["datetime"] = pd.to_datetime(thisData["datetime"])

    for _, row in thisData.iterrows():
        symbol = row["symbol"]
        tabla = clasificar_symbol(symbol)
        if tabla:
            df_single = pd.DataFrame([row])
            print(f"✅ Insertando symbol='{symbol}' en tabla='{tabla}'")
            guardar_en_supabase(tabla, df_single)

def ejecutar():
    global contador_categorias
    contador_categorias = defaultdict(int)

    hb = HomeBroker(
        broker,
        on_securities=on_securities
    )
    hb.auth.login(dni=dni, user=user, password=password, raise_exception=True)
    hb.online.connect()

    print("📡 Suscribiendo: government_bonds - 24hs")
    hb.online.subscribe_securities('government_bonds', '24hs')
    print("📡 Suscribiendo: short_term_government_bonds - 24hs")
    hb.online.subscribe_securities('short_term_government_bonds', '24hs')

    print("🟢 Conexión continua: se mantendrá escuchando.")
    try:
        while True:
            time.sleep(2)
            gc.collect()
    except KeyboardInterrupt:
        pass
    finally:
        print("🔌 Desconectando HomeBroker...")
        hb.online.disconnect()

def dentro_de_horario():
    ahora = datetime.now(pytz.timezone("America/Argentina/Buenos_Aires"))
    return 9 <= ahora.hour < 20

if __name__ == "__main__":
    import sys
    inicio = time.time()

    print("🚀 Iniciando script (modo conexión continua)")
    while True:
        if dentro_de_horario():
            try:
                ejecutar()   # acá no se sale hasta que interrumpas el proceso
            except Exception as e:
                print(f"❌ Error: {e}")
                time.sleep(10)
        else:
            print("🌙 Fuera de horario, se reintenta en 60s.")
            time.sleep(60)

        # Por si querés un reinicio cada 1 hora
        if time.time() - inicio > 3600:
            print("♻️ Reinicio programado (1 hora).")
            os.execv(sys.executable, ['python'] + sys.argv)
