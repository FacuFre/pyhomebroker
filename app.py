from flask import Flask
from pyhomebroker import HomeBroker
import requests
import os

app = Flask(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")

def guardar_en_supabase(symbol, description):
    url = f"{SUPABASE_URL}/rest/v1/instrumentos_pyhome"
    headers = {
        "apikey": SUPABASE_API_KEY,
        "Authorization": f"Bearer {SUPABASE_API_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
    data = {
        "symbol": symbol,
        "description": description
    }
    response = requests.post(url, headers=headers, json=data)
    print(f"Insertando {symbol}: {response.status_code}")

@app.route("/")
def home():
    hb = HomeBroker()
    instruments = hb.get_instruments()

    for inst in instruments[:10]:  # ejemplo con los primeros 10
        guardar_en_supabase(inst["symbol"], inst["description"])

    return "Datos enviados a Supabase"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
