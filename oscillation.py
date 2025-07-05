import os
from dotenv import load_dotenv
import time
import numpy as np
from datetime import datetime, timedelta
from kite_trade import *
from kiteconnect import KiteTicker
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv(override=True)

# Initialize Kite API
enctoken = os.getenv("ENCTOKEN")
api_key = os.getenv("API_KEY")  # Add your API key to .env
kite = KiteApp(enctoken=enctoken)

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
price_history = {}
current_subscriptions = []
kws = None
all_instruments = []
symbol_queue = []
ws_connected = False

def get_nifty500_symbols():
    global all_instruments
    if not all_instruments:
        all_instruments = kite.instruments("NSE")
    return [inst['tradingsymbol'] for inst in all_instruments if inst['tradingsymbol'].endswith('EQ')][:500]

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

def place_scalp_order(symbol):
    global active_order

    ltp = price_history[symbol]['prices'][-1]
    quantity = calculate_quantity(ltp)

    buy_price = round(ltp * 0.998, 2)
    sell_price = round(buy_price + (TARGET_PROFIT / quantity), 2)

    logger.info(f"📈 Placing Order: {symbol} | Buy {quantity} @ ₹{buy_price} | Sell @ ₹{sell_price}")

    try:
        buy_order = kite.place_order(
            variety="regular",
            exchange="NSE",
            tradingsymbol=symbol,
            transaction_type="BUY",
            quantity=quantity,
            product="MIS",
            order_type="LIMIT",
            price=buy_price,
            validity="DAY",
            tag="OSC180"
        )

        sell_order = kite.place_order(
            variety="regular",
            exchange="NSE",
            tradingsymbol=symbol,
            transaction_type="SELL",
            quantity=quantity,
            product="MIS",
            order_type="LIMIT",
            price=sell_price,
            validity="DAY",
            tag="OSC180"
        )

        active_order = {
            "symbol": symbol,
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
            current_ltp = price_history[active_order["symbol"]]['prices'][-1]
            if current_ltp > active_order["sell_price"] * 1.02:
                try:
                    kite.modify_order(
                        variety="regular",
                        order_id=active_order["sell_order_id"],
                        price=round(current_ltp * 0.998, 2)
                    )
                except:
                    pass
    
    except Exception as e:
        logger.error(f"⚠️ Error checking order: {e}")

def on_ticks(ws, ticks):
    global ws_connected
    ws_connected = True
    
    for tick in ticks:
        symbol = None
        for s, data in price_history.items():
            if data['instrument_token'] == tick['instrument_token']:
                symbol = s
                break
                
        if symbol and 'last_price' in tick:
            price_history[symbol]['prices'].append(tick['last_price'])
            price_history[symbol]['prices'] = price_history[symbol]['prices'][-OSCILLATION_PERIOD:]
            price_history[symbol]['last_update'] = datetime.now()

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
    kws = KiteTicker(api_key=api_key, access_token=enctoken)
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
    global symbol_queue
    
    if not symbol_queue:
        symbols = get_nifty500_symbols()
        instruments = kite.instruments("NSE")
        
        symbol_queue = []
        for symbol in symbols:
            instrument = next((i for i in instruments if i['tradingsymbol'] == symbol), None)
            if instrument:
                symbol_queue.append({
                    'symbol': symbol,
                    'instrument_token': instrument['instrument_token']
                })
        
        np.random.shuffle(symbol_queue)
    
    batch_size = MAX_CONCURRENT_SUBS if active_order is None else 1
    batch = symbol_queue[:batch_size]
    symbol_queue = symbol_queue[batch_size:]
    
    new_tokens = [s['instrument_token'] for s in batch]
    update_subscriptions(new_tokens)
    
    for s in batch:
        if s['symbol'] not in price_history:
            try:
                hist_data = kite.historical_data(s['instrument_token'], 
                                               datetime.now() - timedelta(minutes=30), 
                                               datetime.now(), 
                                               "minute")
                if hist_data:
                    prices = [candle['close'] for candle in hist_data]
                    price_history[s['symbol']] = {
                        'prices': prices[-OSCILLATION_PERIOD:] if len(prices) >= OSCILLATION_PERIOD else [prices[-1]] * OSCILLATION_PERIOD,
                        'instrument_token': s['instrument_token'],
                        'last_update': datetime.now()
                    }
            except Exception as e:
                logger.error(f"Error initializing {s['symbol']}: {str(e)}")
    
    return [s['symbol'] for s in batch]

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
            for symbol, data in price_history.items():
                if 'last_update' in data and (datetime.now() - data['last_update']).total_seconds() < WS_SUBSCRIBE_TIME * 2:
                    try:
                        prices = data['prices']
                        if len(prices) >= OSCILLATION_PERIOD and is_oscillating(prices) and find_entry_point(prices):
                            logger.info(f"Found opportunity in {symbol}")
                            if place_scalp_order(symbol):
                                update_subscriptions([data['instrument_token']])
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