import os
import time
import pandas as pd
from api_helper import NorenApiPy
from dotenv import load_dotenv
from threading import Lock, Thread
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load credentials
load_dotenv(override=True)
api_key = os.getenv("FLATTRADE_API_KEY")
api_secret = os.getenv("FLATTRADE_SECRET_KEY")
user_id = os.getenv("FLATTRADE_USER_ID")
user_token = os.getenv("FLATTRADE_SESSION")

# Initialize Flattrade API
api = NorenApiPy()
ret = api.set_session(userid=user_id, password='', usertoken=user_token)
if ret is None:
    logger.error("Failed to set session. Check credentials.")
    exit()

# Prepare symbol list and instrument info
symbols_list = pd.read_csv('data_500.csv')['SYMBOL'].tolist()
instruments = pd.read_csv('NSE_Equity_flattrad.csv')

# Filter instruments - adjust column names as per your CSV
df = instruments[instruments['Symbol'].isin(symbols_list)]
df = df[df['Instrument'] == "EQ"]  # Only Equity instruments
df = df[['Token', 'Symbol']].rename(columns={
    'Token': 'instrument_token',
    'Symbol': 'tradingsymbol'
}).reset_index(drop=True)

# Initialize data columns
df['tick_data'] = [[] for _ in range(len(df))]
df['history_data'] = [[] for _ in range(len(df))]
token_to_index = {str(token): idx for idx, token in enumerate(df['instrument_token'])}
tokens = df['instrument_token'].astype(str).tolist()
price_lock = Lock()

print(f"df - {df}")
print(f"token_to_index - {token_to_index}")
print(f"tokens - {tokens}")

def on_ticks(tick_data):
    """Process incoming ticks from Flattrade WebSocket"""
    try:
        # Check if it's a tick message
        # print(f"tick_data - {tick_data}")
        if tick_data.get('t') == 'tf' or tick_data.get('t') == 'tk':
            token = tick_data.get('tk')
            idx = token_to_index[token]
            if idx is not None and tick_data.get('lp') is not None:
                price = float(tick_data.get('lp', 0))
                # print(f"{price}")
                df.at[idx, 'tick_data'].append(price)
                if len(df.at[idx, 'tick_data']) > 1000:
                    df.at[idx, 'tick_data'] = df.at[idx, 'tick_data'][-1000:]
    except Exception as e:
        logger.error(f"Error processing tick: {e}")

def on_connect():
    """WebSocket connected callback"""
    logger.info("✅ WebSocket Connected")
    try:
        # Subscribe with exchange prefix (NSE|token)
        subscription_tokens = [f"NSE|{token}" for token in tokens]
        api.subscribe(subscription_tokens)
        logger.info(f"Subscribed to {len(tokens)} instruments")
    except Exception as e:
        logger.error(f"Subscription failed: {e}")

def on_close():
    """WebSocket closed callback"""
    logger.info("❌ WebSocket Closed")

def on_error(error):
    """WebSocket error callback"""
    logger.error(f"WebSocket Error: {error}")

api.start_websocket(
    subscribe_callback=on_ticks,
    socket_open_callback=on_connect,
    socket_close_callback=on_close,
    socket_error_callback=on_error
)

def get_price_history(symbol, instrument_token):
    """Fetch historical data with Flattrade API"""
    try:       
        lastBusDay = datetime.now() - timedelta(days=4)
        lastBusDay = lastBusDay.replace(hour=0, minute=0, second=0, microsecond=0)
        hist_data = api.get_time_price_series(exchange='NSE', token=str(instrument_token), starttime=lastBusDay.timestamp(), interval=1)
        
        formatted_data = []
        if hist_data and isinstance(hist_data, list):
            for candle in hist_data:
                try:
                    formatted_data.append({
                        'date': datetime.strptime(candle['time'], '%d-%m-%Y %H:%M:%S'),
                        'open': float(candle['into']),
                        'high': float(candle['inth']),
                        'low': float(candle['intl']),
                        'close': float(candle['intc']),
                        'volume': int(candle['v'] if 'v' in candle else 0)
                    })
                except (KeyError, ValueError) as e:
                    logger.warning(f"Invalid candle data for {symbol}: {e}")
        return formatted_data[-500:] if formatted_data else []
    except Exception as e:
        logger.error(f"History fetch failed for {symbol}: {e}")
        return []

def update_history_data():
    """Batch update historical data with rate limiting"""
    batch_size = 3  # Reduced batch size for Flattrade
    delay = 1  # Increased delay between batches
    while True:
        for i in range(0, len(df), batch_size):
            with price_lock:
                batch = df.iloc[i:i+batch_size]
                for _, row in batch.iterrows():
                    history = get_price_history(row['tradingsymbol'], row['instrument_token'])
                    if history:
                        idx = token_to_index[str(row['instrument_token'])]
                        df.at[idx, 'history_data'] = history
            time.sleep(delay)  # Delay between batches

history_thread = Thread(target=update_history_data, daemon=True)
history_thread.start()

# Main loop to monitor data
try:
    while True:
        print(f"df {df}")
        time.sleep(1)
        
except KeyboardInterrupt:
    print("\n🛑 Keyboard Interrupt. Closing WebSocket...")
    unsubscribe_tokens = [f"NSE|{token}" for token in tokens]
    api.unsubscribe(unsubscribe_tokens)
