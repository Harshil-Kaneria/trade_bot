import os
import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from kite_trade import *
from kiteconnect import KiteTicker
from dotenv import load_dotenv
import logging
from threading import Lock

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load credentials
load_dotenv(override=True)
enctoken = os.getenv("ENCTOKEN")
api_key = os.getenv("API_KEY")
kite = KiteApp(enctoken=enctoken)
user_id = kite.profile()["user_id"]

# Configuration
CAPITAL = 100000
TARGET_PROFIT = 180
MAX_ORDER_TIME = 300
RISK_PER_TRADE = 0.01
OSCILLATION_PERIOD = 20
WS_SUBSCRIBE_TIME = 30
MAX_CONCURRENT_SUBS = 5
RECONNECT_DELAY = 5  # seconds between reconnect attempts

# Trackers
active_order = None
trades_completed = 0
total_profit = 0
price_lock = Lock()
kws = None
ws_connected = False

# Prepare symbol list and instrument info
symbols_list = pd.read_csv('data_500.csv')['SYMBOL'].tolist() if os.path.exists('data_500.csv') else []
instruments = pd.DataFrame(kite.instruments("NSE"))
df = instruments[instruments["tradingsymbol"].isin(symbols_list)]
df = df[df["segment"] != "INDICES"]
df = df[['instrument_token', 'tradingsymbol']].reset_index(drop=True)

# Initialize price tracking
df['last_price'] = 0.0
df['price_history'] = df['tradingsymbol'].apply(lambda x: [])
token_to_index = {token: idx for idx, token in enumerate(df['instrument_token'])}
tokens = df['instrument_token'].tolist()
current_subscriptions = []

def get_price_history(symbol, instrument_token, minutes=30):
    try:
        hist_data = kite.historical_data(
            instrument_token,
            datetime.now() - timedelta(minutes=minutes),
            datetime.now(),
            "minute"
        )
        if hist_data:
            return [candle['close'] for candle in hist_data]
    except Exception as e:
        logger.error(f"Error fetching history for {symbol}: {e}")
    return []

def calculate_quantity(price):
    quantity = int(CAPITAL / price)
    return max(1, quantity)

def is_oscillating(prices):
    if len(prices) < 10:
        return False
    
    peaks = []
    troughs = []
    
    for i in range(1, len(prices)-1):
        if prices[i] > prices[i-1] and prices[i] > prices[i+1]:
            peaks.append(prices[i])
        elif prices[i] < prices[i-1] and prices[i] < prices[i+1]:
            troughs.append(prices[i])
    
    if len(peaks) < 3 or len(troughs) < 3:
        return False
    
    avg_trough = np.mean(troughs)
    trough_range = max(troughs) - min(troughs)
    
    return (trough_range/avg_trough < 0.015 and 
            np.all(np.diff(peaks) > 0))

def find_entry_point(prices):
    if len(prices) < 10:
        return False
    
    current_price = prices[-1]
    last_trough = min(prices[-5:-1])
    
    return current_price <= last_trough * 1.005

def place_scalp_order(symbol, instrument_token):
    global active_order

    with price_lock:
        idx = token_to_index.get(instrument_token)
        if idx is None:
            return False
        
        ltp = df.at[idx, 'last_price']
        price_history = df.at[idx, 'price_history']
        
        if not price_history:
            price_history = get_price_history(symbol, instrument_token)
            if not price_history:
                return False
            df.at[idx, 'price_history'] = price_history
        
        ltp = price_history[-1] if price_history else ltp
        quantity = calculate_quantity(ltp)

    buy_price = round(ltp * 0.998, 2)
    sell_price = round(buy_price + (TARGET_PROFIT / quantity), 2)

    logger.info(f"📈 Placing Order: {symbol} | Buy {quantity} @ ₹{buy_price} | Sell @ ₹{sell_price}")

    try:
        buy_order = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=symbol,
            transaction_type=kite.TRANSACTION_TYPE_BUY,
            quantity=quantity,
            product=kite.PRODUCT_CNC,
            order_type=kite.ORDER_TYPE_LIMIT,
            price=buy_price,
            validity=None,
            disclosed_quantity=None,
            trigger_price=None,
            squareoff=None,
            stoploss=None,
            trailing_stoploss=None,
            tag="HK"
        )

        sell_order = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=symbol,
            transaction_type=kite.TRANSACTION_TYPE_SELL,
            quantity=quantity,
            product=kite.PRODUCT_CNC,
            order_type=kite.ORDER_TYPE_LIMIT,
            price=sell_price,
            validity=None,
            disclosed_quantity=None,
            trigger_price=None,
            squareoff=None,
            stoploss=None,
            trailing_stoploss=None,
            tag="HK"
        )

        active_order = {
            "symbol": symbol,
            "instrument_token": instrument_token,
            "buy_order_id": buy_order,
            "sell_order_id": sell_order,
            "quantity": quantity,
            "buy_price": buy_price,
            "sell_price": sell_price,
            "order_time": datetime.now()
        }
        return True

    except Exception as e:
        logger.error(f"❌ Order Failed: {e}")
        return False

def manage_orders():
    global active_order, trades_completed, total_profit
    
    if not active_order:
        return
    
    current_time = datetime.now()
    order_age = (current_time - active_order["order_time"]).total_seconds()
    
    if order_age > MAX_ORDER_TIME:
        logger.info("⏳ Order Timeout (5 mins), Exiting Trade...")
        try:
            kite.cancel_order(variety="regular", order_id=active_order["buy_order_id"])
            kite.cancel_order(variety="regular", order_id=active_order["sell_order_id"])
            
            positions = kite.positions()
            for pos in positions["net"]:
                if pos["tradingsymbol"] == active_order["symbol"] and pos["quantity"] != 0:
                    kite.place_order(
                        variety="regular",
                        exchange="NSE",
                        tradingsymbol=active_order["symbol"],
                        transaction_type="SELL",
                        quantity=abs(pos["quantity"]),
                        product="MIS",
                        order_type="MARKET",
                        validity="DAY"
                    )
                    logger.info("🔴 Force Exited Trade (Market Order)")
                    break
        except Exception as e:
            logger.error(f"⚠️ Error exiting trade: {e}")
        
        active_order = None
        return
    
    try:
        buy_status = kite.order_history(active_order["buy_order_id"])[-1]["status"]
        sell_status = kite.order_history(active_order["sell_order_id"])[-1]["status"]
        
        if buy_status == "COMPLETE" and sell_status == "COMPLETE":
            profit = (active_order["sell_price"] - active_order["buy_price"]) * active_order["quantity"]
            total_profit += profit
            trades_completed += 1
            logger.info(f"✅ Trade Successful! Profit: ₹{profit} | Total Profit: ₹{total_profit}")
            active_order = None
        
        elif buy_status == "COMPLETE" and sell_status != "COMPLETE":
            with price_lock:
                idx = token_to_index.get(active_order["instrument_token"])
                if idx is not None:
                    current_ltp = df.at[idx, 'last_price']
                    if current_ltp > active_order["sell_price"] * 1.02:
                        try:
                            kite.modify_order(
                                variety="regular",
                                order_id=active_order["sell_order_id"],
                                price=round(current_ltp * 0.998, 2)
                            )
                        except Exception as e:
                            logger.error(f"⚠️ Error modifying order: {e}")
    
    except Exception as e:
        logger.error(f"⚠️ Error checking order: {e}")

def on_ticks(ws, ticks):
    global ws_connected
    ws_connected = True
    
    with price_lock:
        for tick in ticks:
            token = tick['instrument_token']
            price = tick.get('last_price', 0.0)
            idx = token_to_index.get(token)
            if idx is not None:
                df.at[idx, 'last_price'] = price
                # Update price history
                price_history = df.at[idx, 'price_history']
                price_history.append(price)
                if len(price_history) > OSCILLATION_PERIOD:
                    price_history.pop(0)
                df.at[idx, 'price_history'] = price_history

def on_connect(ws, response):
    global ws_connected
    ws_connected = True
    logger.info("WebSocket connected")
    if current_subscriptions:
        ws.subscribe(current_subscriptions)
        ws.set_mode(ws.MODE_QUOTE, current_subscriptions)

def on_error(ws, code, reason):
    global ws_connected
    ws_connected = False
    logger.error(f"WebSocket Error: {code} - {reason}")
    reconnect_websocket()

def on_close(ws, code, reason):
    global ws_connected
    ws_connected = False
    logger.info(f"WebSocket Closed: {code} - {reason}")
    reconnect_websocket()

def reconnect_websocket():
    global kws
    time.sleep(RECONNECT_DELAY)
    logger.info("Attempting to reconnect WebSocket...")
    try:
        if kws:
            kws.close()
        initialize_websocket()
    except Exception as e:
        logger.error(f"Reconnection failed: {e}")

def initialize_websocket():
    global kws
    kws = KiteTicker(api_key=api_key, access_token=f"{enctoken}&user_id={user_id}")
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_error = on_error
    kws.on_close = on_close
    kws.connect(threaded=True)

def update_subscriptions(new_tokens):
    global current_subscriptions
    
    if not kws or not ws_connected:
        current_subscriptions = new_tokens
        return
    
    tokens_to_unsubscribe = list(set(current_subscriptions) - set(new_tokens))
    if tokens_to_unsubscribe:
        try:
            kws.unsubscribe(tokens_to_unsubscribe)
        except Exception as e:
            logger.error(f"Error unsubscribing: {e}")

    tokens_to_subscribe = list(set(new_tokens) - set(current_subscriptions))
    if tokens_to_subscribe:
        try:
            kws.subscribe(tokens_to_subscribe)
            kws.set_mode(kws.MODE_QUOTE, tokens_to_subscribe)
        except Exception as e:
            logger.error(f"Error subscribing: {e}")

    current_subscriptions = new_tokens

def rotate_symbol_subscription():
    if len(df) == 0:
        logger.error("No symbols available for subscription")
        return []
    
    batch_size = MAX_CONCURRENT_SUBS if active_order is None else 1
    batch = df.sample(min(batch_size, len(df)))
    
    new_tokens = batch['instrument_token'].tolist()
    update_subscriptions(new_tokens)
    
    # Initialize price history for new symbols
    with price_lock:
        for _, row in batch.iterrows():
            if not df.at[row.name, 'price_history']:
                hist_data = get_price_history(row['tradingsymbol'], row['instrument_token'])
                if hist_data:
                    df.at[row.name, 'price_history'] = hist_data[-OSCILLATION_PERIOD:] if len(hist_data) >= OSCILLATION_PERIOD else [hist_data[-1]] * OSCILLATION_PERIOD
    
    return batch['tradingsymbol'].tolist()

def run_bot():
    initialize_websocket()
    
    iteration = 0
    last_rotation = datetime.now()
    
    while True:
        iteration += 1
        logger.info(f"\n=== Iteration {iteration} ===")
        
        if not ws_connected:
            logger.warning("WebSocket not connected, waiting...")
            time.sleep(RECONNECT_DELAY)
            continue
        
        if (datetime.now() - last_rotation).total_seconds() > WS_SUBSCRIBE_TIME:
            current_symbols = rotate_symbol_subscription()
            logger.info(f"Now monitoring: {', '.join(current_symbols)}")
            last_rotation = datetime.now()
        
        if active_order is None:
            logger.info("Checking for trading opportunities...")
            with price_lock:
                for _, row in df.iterrows():
                    symbol = row['tradingsymbol']
                    price_history = row['price_history']
                    
                    if len(price_history) >= OSCILLATION_PERIOD:
                        try:
                            if is_oscillating(price_history) and find_entry_point(price_history):
                                logger.info(f"Found opportunity in {symbol}")
                                if place_scalp_order(symbol, row['instrument_token']):
                                    update_subscriptions([row['instrument_token']])
                                    break
                        except Exception as e:
                            logger.error(f"Error evaluating {symbol}: {str(e)}")
                            continue
        
        manage_orders()
        
        logger.info(f"Active orders: {1 if active_order else 0}")
        logger.info(f"Total trades: {trades_completed} | Profit: ₹{total_profit}")
        
        time.sleep(5)

if __name__ == "__main__":
    try:
        run_bot()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        if kws:
            kws.close()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        if kws:
            kws.close()