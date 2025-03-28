# main.py
import os
import time
from datetime import datetime
from pyhomebroker import HomeBroker
import requests

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")
BROKER_ID = int(os.getenv("BROKER_ID"))
DNI = os.getenv("DNI")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")

def guardar_en_supabase(tabla, symbol, description, last, bid, ask):
    url = f"{SUPABASE_URL}/rest/v1/{tabla}"
    headers = {
        "apikey": SUPABASE_API_KEY,
        "Authorization": f"Bearer {SUPABASE_API_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }
    data = {
        "symbol": symbol,
        "description": description,
        "last_price": last,
        "bid": bid,
        "ask": ask,
        "updated_at": datetime.utcnow().isoformat()
    }
    response = requests.post(url, headers=headers, json=data)
    print(f"[{tabla}] {symbol} → {response.status_code}")

def main():
    hb = HomeBroker(broker_id=BROKER_ID)
    hb.auth.login(dni=DNI, user=USER, password=PASSWORD, raise_exception=True)

    tipos = [
        ("acciones", hb.get_stocks),
        ("bonos", hb.get_bonds),
        ("cedears", hb.get_cedears),
        ("letras", hb.get_short_term_government_bonds),
        ("cauciones", hb.get_repos),
        ("opciones", hb.get_options),
        ("ons", hb.get_corporate_bonds),
        ("panel_general", hb.get_general_board_stocks),
    ]

    for tabla, fetch_func in tipos:
        try:
            instrumentos = fetch_func()
            for inst in instrumentos:
                guardar_en_supabase(
                    tabla=tabla,
                    symbol=inst["symbol"],
                    description=inst.get("description", ""),
                    last=inst.get("last", 0),
                    bid=inst.get("bid", 0),
                    ask=inst.get("ask", 0)
                )
                time.sleep(0.1)
        except Exception as e:
            print(f"Error con {tabla}: {e}")

if __name__ == "__main__":
    main()
