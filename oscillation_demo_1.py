import os
import time
import pandas as pd
from kite_trade import *  # API calls
from kiteconnect import KiteTicker  # WebSocket
from dotenv import load_dotenv
from threading import Lock, Thread
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

# Initialize data columns
df['tick_data'] = [[] for _ in range(len(df))]  # For live ticks
df['history_data'] = [[] for _ in range(len(df))]  # For historical data
token_to_index = {token: idx for idx, token in enumerate(df['instrument_token'])}
tokens = df['instrument_token'].tolist()
price_lock = Lock()

# Setup WebSocket
kws = KiteTicker(api_key="AlgoTrader", access_token=f"{enctoken}&user_id={user_id}")

def get_price_history(symbol, instrument_token):
    """Fetches historical data for a symbol (minute candles for initial fill)."""
    try:
        hist_data = kite.historical_data(
            instrument_token,
            datetime.now() - timedelta(days=4),
            datetime.now(),
            "minute"
        )
        return hist_data[-500:] if hist_data else []
    except Exception as e:
        logger.error(f"Error fetching historical minute data for {symbol} ({instrument_token}): {e}")
        return []

def update_history_data():
    """Updates history data in batches with rate limiting."""
    batch_size = 5
    delay_between_batches = 0.5  # seconds
    while True:
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            
            with price_lock:
                for _, row in batch.iterrows():
                    symbol = row['tradingsymbol']
                    token = row['instrument_token']
                    idx = token_to_index[token]
                    
                    # Fetch historical data
                    history = get_price_history(symbol, token)
                    if history:
                        df.at[idx, 'history_data'] = history
                    # Small delay between each request in batch
                    time.sleep(0.1)
            
            # Delay between batches
            time.sleep(delay_between_batches)

def on_ticks(ws, ticks):
    with price_lock:
        for tick in ticks:
            token = tick['instrument_token']
            price = tick.get('last_price', 0.0)        
            idx = token_to_index.get(token)
            if idx is not None:
                df.at[idx, 'tick_data'].append(price)
                if len(df.at[idx, 'tick_data']) > 1000:
                    df.at[idx, 'tick_data'] = df.at[idx, 'tick_data'][-1000:]

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

# Start historical data updater thread
history_updater = Thread(target=update_history_data, daemon=True)
history_updater.start()

# Connect WebSocket
kws.connect(threaded=True)

# Wait until connected
while not kws.is_connected():
    time.sleep(0.2)

# Main loop to monitor data
try:
    while True:
        print(f"df {df}")
        time.sleep(1)
        
except KeyboardInterrupt:
    print("\n🛑 Keyboard Interrupt. Closing WebSocket...")
    kws.unsubscribe(tokens)
    kws.close()
    history_updater.join(1)  # Wait for history updater to finish   