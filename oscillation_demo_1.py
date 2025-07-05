import os
import time
import pandas as pd
from kite_trade import *
from kiteconnect import KiteTicker
from dotenv import load_dotenv
from threading import Lock

# Load .env
load_dotenv(override=True)
enctoken = os.getenv("ENCTOKEN")
kite = KiteApp(enctoken=enctoken)
user_id = kite.profile()["user_id"]

# Read instruments and symbols
symbols_list = pd.read_csv('data_500.csv')['SYMBOL'].tolist()
instruments = pd.DataFrame(kite.instruments("NSE"))
df = instruments[instruments["tradingsymbol"].isin(symbols_list)]
df = df[df["segment"] != "INDICES"]
df = df[['instrument_token', 'tradingsymbol']].reset_index(drop=True)

# Use NumPy array for better speed
df['last_price'] = 0.0
price_lock = Lock()

# Mapping instrument_token to row index (fast lookup)
token_to_index = {token: idx for idx, token in enumerate(df['instrument_token'])}
tokens = df['instrument_token'].tolist()

# Setup WebSocket
kws = KiteTicker(api_key="AlgoTrader", access_token=f"{enctoken}&user_id={user_id}")

def on_ticks(ws, ticks):
    with price_lock:
        for tick in ticks:
            idx = token_to_index.get(tick['instrument_token'])
            if idx is not None:
                df.at[idx, 'last_price'] = tick.get('last_price', 0.0)

def on_connect(ws, response):
    print("✅ WebSocket Connected")
    ws.subscribe(tokens)
    ws.set_mode(ws.MODE_QUOTE, tokens)

def on_close(ws, code, reason):
    print(f"❌ WebSocket Closed: {code} - {reason}")

# Assign callbacks
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

# Start WebSocket in background thread
kws.connect(threaded=True)
while not kws.is_connected():
    time.sleep(0.2)

try:
    while True:
        with price_lock:
            # Use copy to avoid race conditions while printing
            snapshot = df.copy()
        print(f"\n🕒 {time.strftime('%H:%M:%S')} | Top 5 Updated Prices:")
        print(snapshot)  # Only show head for speed
        # time.sleep(0.5)  # Fast loop, but adjustable depending on print/debugging

except KeyboardInterrupt:
    print("\n🛑 Interrupted. Closing connection...")
    kws.unsubscribe(tokens)
    kws.close()
