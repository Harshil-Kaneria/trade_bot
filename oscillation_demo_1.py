import os
import time
import pandas as pd
from kite_trade import *
from kiteconnect import KiteTicker
from dotenv import load_dotenv
from threading import Lock

# Load credentials
load_dotenv(override=True)
enctoken = os.getenv("ENCTOKEN")
kite = KiteApp(enctoken=enctoken)
user_id = kite.profile()["user_id"]

# Prepare symbol list and instrument info
symbols_list = pd.read_csv('data_500.csv')['SYMBOL'].tolist()
instruments = pd.DataFrame(kite.instruments("NSE"))
df = instruments[instruments["tradingsymbol"].isin(symbols_list)]
df = df[df["segment"] != "INDICES"]
df = df[['instrument_token', 'tradingsymbol']].reset_index(drop=True)

# Initialize price column and map
df['last_price'] = 0.0
token_to_index = {token: idx for idx, token in enumerate(df['instrument_token'])}
tokens = df['instrument_token'].tolist()
price_lock = Lock()

# Setup WebSocket
kws = KiteTicker(api_key="AlgoTrader", access_token=f"{enctoken}&user_id={user_id}")

def on_ticks(ws, ticks):
    with price_lock:
        for tick in ticks:
            token = tick['instrument_token']
            price = tick.get('last_price', 0.0)
            idx = token_to_index.get(token)
            if idx is not None:
                df.at[idx, 'last_price'] = price
                print(f"✅ Tick: {df.at[idx, 'tradingsymbol']} - ₹{price}")

def on_connect(ws, response):
    print("✅ WebSocket Connected")
    ws.subscribe(tokens)
    ws.set_mode(ws.MODE_QUOTE, tokens)

def on_close(ws, code, reason):
    print(f"❌ WebSocket Closed: {code} - {reason}")

# Assign event handlers
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

# Connect WebSocket
kws.connect(threaded=True)

# Wait until connected
while not kws.is_connected():
    time.sleep(0.2)

# Keep alive until interrupted
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\n🛑 Keyboard Interrupt. Closing WebSocket...")
    kws.unsubscribe(tokens)
    kws.close()
