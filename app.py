import os
from pyhomebroker import HomeBroker
import requests
from datetime import datetime
import time

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")
BROKER_ID = int(os.getenv("BROKER_ID"))
DNI = os.getenv("DNI")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")

def guardar_en_supabase(symbol, description, last, bid, ask):
    url = f"{SUPABASE_URL}/rest/v1/instrumentos_pyhome"
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
    print(f"[{symbol}] → {response.status_code}")

def main():
    hb = HomeBroker(broker_id=BROKER_ID)
    hb.auth.login(dni=DNI, user=USER, password=PASSWORD, raise_exception=True)
    instruments = hb.get_instruments()

    for inst in instruments:
        guardar_en_supabase(
            symbol=inst["symbol"],
            description=inst.get("description", ""),
            last=inst.get("last", 0),
            bid=inst.get("bid", 0),
            ask=inst.get("ask", 0)
        )
        time.sleep(0.1)

if __name__ == "__main__":
    main()
