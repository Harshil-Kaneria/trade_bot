import os
from dotenv import load_dotenv
import time
import pandas as pd
from kite_trade import *
from kiteconnect import KiteTicker
from datetime import datetime
from threading import Event

# Load credentials
load_dotenv(override=True)
enctoken = os.getenv("ENCTOKEN")
kite = KiteApp(enctoken=enctoken)
user_id = kite.profile()["user_id"]

# Read symbols and instrument data
symbols_list = list(pd.read_csv('data_500.csv')['SYMBOL'])
df = pd.DataFrame(kite.instruments("NSE"))
df = df[df["tradingsymbol"].isin(symbols_list)].reset_index(drop=True)
df = df[df["segment"] != "INDICES"].reset_index(drop=True)
df = df[['instrument_token', 'tradingsymbol', 'last_price']]

# Setup price mapping
price_map = {token: 0.0 for token in df['instrument_token']}

# Setup WebSocket
kws = KiteTicker(api_key="AlgoTrader", access_token=enctoken + "&user_id=" + user_id)
chunk_done = Event()

def on_ticks(ws, ticks):
    for tick in ticks:
        token = tick['instrument_token']
        price = tick.get('last_price', 0.0)
        price_map[token] = price
    chunk_done.set()

def on_connect(ws, response):
    print("✅ WebSocket Connected.")

def on_close(ws, code, reason):
    print("❌ WebSocket Closed.", code, reason)

kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

# Connect WebSocket
kws.connect(threaded=True)
while not kws.is_connected():
    time.sleep(0.5)

# Start chunk loop
chunk_size = 100
total_records = len(df)
num_chunks = (total_records + chunk_size - 1) // chunk_size

print("\n🔄 Starting continuous price update loop. Press Ctrl+C to stop.\n")

try:
    while True:
        for i in range(num_chunks):
            chunk_df = df[i*chunk_size : (i+1)*chunk_size]
            current_tokens = list(chunk_df['instrument_token'])

            print(f"\n📦 Chunk {i+1}/{num_chunks} - Subscribing...")
            chunk_done.clear()

            kws.subscribe(current_tokens)
            kws.set_mode(kws.MODE_QUOTE, current_tokens)

            # Wait to get at least one tick or timeout after 5 seconds
            chunk_done.wait(timeout=5)

            kws.unsubscribe(current_tokens)

            # Update DataFrame
            for token in current_tokens:
                df.loc[df['instrument_token'] == token, 'last_price'] = price_map.get(token, 0.0)

            print(f"✅ Updated prices for Chunk {i+1}")
            print(df)  # Print full df after every chunk update

            time.sleep(1)  # Delay before next chunk

except KeyboardInterrupt:
    print("\n🛑 Program manually stopped. Closing WebSocket...")
    kws.close()
