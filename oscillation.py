import os
import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from kite_trade import *  # API calls
from kiteconnect import KiteTicker  # WebSocket
from dotenv import load_dotenv
import logging
from threading import Lock, Thread

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('scalper.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Load credentials
load_dotenv(override=True)
enctoken = os.getenv("ENCTOKEN")
api_key = 'API_KEY'

# Global kite object
kite = None
user_id = None
try:
    kite = KiteApp(enctoken=enctoken)
    user_id = kite.profile()["user_id"]
    logger.info(f"KiteApp initialized for user: {user_id}")
except Exception as e:
    logger.critical(f"Failed to initialize KiteApp: {e}. Check ENCTOKEN or network connection.")
    exit()

# Configuration
CAPITAL = 98500
TARGET_PROFIT_PER_TRADE = 183
MAX_ORDER_TIME = 300  # 5 minutes for faster exits
RISK_PER_TRADE = 0.01  # Percentage of capital
OSCILLATION_PERIOD = 10  # Short period for scalping
VOLATILITY_THRESHOLD = 0.0005  # 0.05% for small price movements
WS_SUBSCRIBE_TIME = 30  # Subscription rotation
MAX_CONCURRENT_SUBS = 20  # Concurrent subscriptions
RECONNECT_DELAY = 5  # Seconds between reconnect attempts
MIN_PRICE = 100  # Minimum stock price
MAX_PRICE = 3000  # Maximum stock price
MAX_ACTIVE_TRADES = 1  # Limit concurrent trades
SHORT_EMA_PERIOD = 5  # Short EMA for trend detection
LONG_EMA_PERIOD = 20  # Long EMA for trend detection
TREND_STRENGTH_THRESHOLD = 0.0005  # Minimum EMA difference for trend

# Trackers
pending_trades = {}
trades_completed = 0
total_profit = 0
price_lock = Lock()
kws = None
ws_connected = False
websocket_thread = None

# Prepare symbol list and instrument info
symbols_list = pd.read_csv('data_500.csv')['SYMBOL'].tolist() if os.path.exists('data_500.csv') else []
if not symbols_list:
    logger.critical("data_500.csv not found or is empty. Please create it with a 'SYMBOL' column.")
    exit()

df = pd.DataFrame()
try:
    instruments = pd.DataFrame(kite.instruments("NSE"))
    df = instruments[instruments["tradingsymbol"].isin(symbols_list)]
    df = df[df["segment"] != "INDICES"]
    # df = df[df['last_price'] > 0]  # Remove for testing outside market hours
    df = df[['instrument_token', 'tradingsymbol', 'last_price']].reset_index(drop=True)
    if df.empty:
        logger.critical("No valid symbols found after filtering instruments. Check data_500.csv, NSE instruments, and filters.")
        exit()
except Exception as e:
    logger.critical(f"Error loading instruments: {e}. Ensure API is working and internet connection.")
    exit()

# Initialize price tracking
df['last_price'] = 0.0
df['price_history'] = df['tradingsymbol'].apply(lambda x: [])  # Filled with tick data
token_to_index = {token: idx for idx, token in enumerate(df['instrument_token'])}
tokens = df['instrument_token'].tolist()
current_subscriptions = []

# --- Helper Functions ---

def get_price_history(symbol, instrument_token, minutes=30):
    """Fetches historical data for a symbol (minute candles for initial fill)."""
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
        logger.error(f"Error fetching historical minute data for {symbol} ({instrument_token}): {e}")
    return []

def calculate_quantity(price, is_short_sell=False):
    """Calculates quantity based on target profit and price."""
    if price <= 0:
        return 0
    
    target_profit_percentage = 0.002
    target_profit_per_share = price * target_profit_percentage
    
    if target_profit_per_share <= 0:
        return 0
    
    quantity = int(TARGET_PROFIT_PER_TRADE / target_profit_per_share)
    quantity = max(1, quantity)
    quantity = min(quantity, int(CAPITAL / price))
    
    return quantity

def calculate_ema(prices, period):
    """Calculates Exponential Moving Average."""
    if len(prices) < period:
        return None
    ema = pd.Series(prices).ewm(span=period, adjust=False).mean().iloc[-1]
    return ema

def determine_trend(prices):
    if len(prices) < max(SHORT_EMA_PERIOD, LONG_EMA_PERIOD) * 2:
        return 'neutral'

    short_ema = calculate_ema(prices, SHORT_EMA_PERIOD)
    long_ema = calculate_ema(prices, LONG_EMA_PERIOD)
    current_price = prices[-1]

    if short_ema is None or long_ema is None:
        return 'neutral'

    if short_ema > long_ema and current_price > short_ema and (short_ema - long_ema) / long_ema > TREND_STRENGTH_THRESHOLD:
        return 'positive'
    
    if short_ema < long_ema and current_price < short_ema and (long_ema - short_ema) / long_ema > TREND_STRENGTH_THRESHOLD:
        return 'negative'
    
    return 'neutral'

def calculate_oscillation_score(prices):
    """Calculates an oscillation score based on price range and ROC sign changes."""
    if len(prices) < OSCILLATION_PERIOD:
        return 0.0
    
    recent_prices = np.array(prices[-OSCILLATION_PERIOD:])
    avg_price = np.mean(recent_prices)
    if avg_price == 0:
        return 0.0
    
    # Price range score (normalized percentage range)
    price_range = np.max(recent_prices) - np.min(recent_prices)
    range_score = price_range / avg_price
    
    # Standard deviation filter
    std_dev = np.std(recent_prices)
    if std_dev / avg_price > 0.01:  # Avoid trending stocks
        return 0.0
    
    # ROC sign changes
    roc = [(recent_prices[i] - recent_prices[i-1]) / recent_prices[i-1] for i in range(1, len(recent_prices))]
    sign_changes = 0
    for i in range(1, len(roc)):
        if roc[i] * roc[i-1] < 0:
            sign_changes += 1
    roc_score = sign_changes / (OSCILLATION_PERIOD - 1)  # Normalize by max possible changes
    
    # Combined score (60% range, 40% ROC changes)
    return 0.6 * range_score + 0.4 * roc_score

def is_volatile_and_oscillating(prices):
    if len(prices) < OSCILLATION_PERIOD:
        return False
    
    recent_prices = np.array(prices[-OSCILLATION_PERIOD:])
    
    # Check volatility (price range >= 0.05% of average price)
    price_range = np.max(recent_prices) - np.min(recent_prices)
    avg_price = np.mean(recent_prices)
    if avg_price == 0:
        return False
    
    if price_range / avg_price < VOLATILITY_THRESHOLD:
        return False
    
    # Check standard deviation to avoid strong trends
    std_dev = np.std(recent_prices)
    if std_dev / avg_price > 0.01:
        return False
    
    # Check momentum with Rate of Change (ROC)
    roc = [(recent_prices[i] - recent_prices[i-1]) / recent_prices[i-1] for i in range(1, len(recent_prices))]
    if len(roc) < 2:
        return False
    
    # Look for at least one sign change for oscillation
    sign_changes = 0
    for i in range(1, len(roc)):
        if roc[i] * roc[i-1] < 0:
            sign_changes += 1
    if sign_changes < 1:
        return False
    
    return True

def find_entry_point_long(prices):
    if len(prices) < OSCILLATION_PERIOD:
        return False
    
    current_price = prices[-1]
    sma_short = np.mean(prices[-5:])
    sma_long = np.mean(prices[-OSCILLATION_PERIOD:])
    
    if current_price >= sma_short:
        return False
        
    if sma_short > sma_long * 1.001:
        return False
        
    recent_min = np.min(prices[-OSCILLATION_PERIOD:])
    recent_max = np.max(prices[-OSCILLATION_PERIOD:])
    
    if recent_max == recent_min:
        return False

    if current_price > (recent_min + (recent_max - recent_min) * 0.4):
        return False

    if len(prices) >= 3 and current_price < prices[-2]:
        return True

    return False

def find_entry_point_short(prices):
    if len(prices) < OSCILLATION_PERIOD:
        return False
    
    current_price = prices[-1]
    sma_long = np.mean(prices[-OSCILLATION_PERIOD:])
    
    if sma_long == 0:
        return False
        
    sma_short = np.mean(prices[-5:])
    if sma_short < sma_long * 0.999:
        return False
        
    recent_min = np.min(prices[-OSCILLATION_PERIOD:])
    recent_max = np.max(prices[-OSCILLATION_PERIOD:])
    
    if recent_max == recent_min:
        return False

    if current_price < (recent_max - (recent_max - recent_min) * 0.4):
        return False

    if len(prices) >= 3 and current_price < prices[-2]:
        return True

    return False

def place_trade_order(symbol, instrument_token, current_ltp, trend_direction):
    global pending_trades

    if len(pending_trades) >= MAX_ACTIVE_TRADES:
        logger.info(f"Max active trades ({MAX_ACTIVE_TRADES}) reached. Skipping {symbol}.")
        return False

    try:
        quantity = 0
        product_type = ""
        transaction_type = ""
        target_price = 0.0
        trade_type = ""

        if trend_direction == 'positive' or trend_direction == 'neutral':
            trade_type = 'LONG'
            quantity = calculate_quantity(current_ltp, is_short_sell=False)
            product_type = kite.PRODUCT_CNC
            transaction_type = kite.TRANSACTION_TYPE_BUY
            target_price = round(current_ltp * 1.002 / 0.05) * 0.05
            logger.info(f"📈 Placing LONG Buy Order: {symbol} | Qty: {quantity} @ MARKET | Target Sell: ₹{target_price:.2f} (CNC)")

        elif trend_direction == 'negative':
            trade_type = 'SHORT'
            quantity = calculate_quantity(current_ltp, is_short_sell=True)
            product_type = kite.PRODUCT_MIS
            transaction_type = kite.TRANSACTION_TYPE_SELL
            target_price = round(current_ltp * 0.998 / 0.05) * 0.05
            logger.info(f"📉 Placing SHORT Sell Order: {symbol} | Qty: {quantity} @ MARKET | Target Buy: ₹{target_price:.2f} (MIS)")
        else:
            logger.warning(f"No clear trend for {symbol}. Skipping order placement.")
            return False

        if quantity == 0:
            logger.warning(f"Invalid quantity for {symbol} at {current_ltp}. Skipping order.")
            return False

        initial_order_response = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=symbol,
            transaction_type=transaction_type,
            quantity=quantity,
            product=product_type,
            order_type=kite.ORDER_TYPE_MARKET,
            tag=f"SCALPER_{trade_type}_INITIAL"
        )
        initial_order_id = initial_order_response

        if not initial_order_id:
            logger.error(f"Failed to get order_id for initial {trade_type} order of {symbol}. Response: {initial_order_response}")
            return False

        logger.info(f"Initial {trade_type} Order placed for {symbol}: {initial_order_id}")

        trade_info = {
            "symbol": symbol,
            "instrument_token": instrument_token,
            "initial_order_id": initial_order_id,
            "exit_order_id": None,
            "quantity": quantity,
            "initial_price": None,
            "target_price": target_price,
            "order_time": datetime.now(),
            "initial_completed": False,
            "exit_completed": False,
            "exit_attempted": False,
            "trade_type": trade_type
        }
        pending_trades[initial_order_id] = trade_info
        
        return True

    except Exception as e:
        logger.error(f"❌ Order Placement Failed for {symbol} ({trade_type}): {str(e)}")
        return False

def get_order_details_from_history(order_id):
    """Helper to fetch details of a specific order from full order history."""
    try:
        all_orders = kite.orders()
        for order in all_orders:
            if order.get('order_id') == order_id:
                return order
    except Exception as e:
        logger.error(f"Error fetching order details for {order_id} from history: {e}")
    return None

def manage_orders():
    """Manages all pending orders, including placing exit orders and exiting on timeout."""
    global trades_completed, total_profit, pending_trades

    orders_to_check = list(pending_trades.keys())

    for order_id in orders_to_check:
        trade = pending_trades.get(order_id)
        if not trade:
            continue

        current_time = datetime.now()
        order_age = (current_time - trade["order_time"]).total_seconds()

        try:
            if not trade["initial_completed"]:
                initial_order_status = get_order_details_from_history(trade["initial_order_id"])
                
                if initial_order_status and initial_order_status["status"] == "COMPLETE":
                    trade["initial_completed"] = True
                    trade["initial_price"] = initial_order_status["average_price"]
                    logger.info(f"✅ Initial {trade['trade_type']} Order COMPLETE for {trade['symbol']} @ ₹{trade['initial_price']:.2f}")

                    if trade["exit_order_id"] is None:
                        exit_transaction_type = kite.TRANSACTION_TYPE_SELL if trade["trade_type"] == 'LONG' else kite.TRANSACTION_TYPE_BUY
                        exit_product_type = kite.PRODUCT_CNC if trade["trade_type"] == 'LONG' else kite.PRODUCT_MIS

                        if trade["trade_type"] == 'LONG' and trade["target_price"] <= trade["initial_price"]:
                            trade["target_price"] = round(trade["initial_price"] * 1.002 / 0.05) * 0.05
                            logger.warning(f"Adjusted LONG sell target for {trade['symbol']} to ensure profit: ₹{trade['target_price']:.2f}")
                        elif trade["trade_type"] == 'SHORT' and trade["target_price"] >= trade["initial_price"]:
                            trade["target_price"] = round(trade["initial_price"] * 0.998 / 0.05) * 0.05
                            logger.warning(f"Adjusted SHORT buy target for {trade['symbol']} to ensure profit: ₹{trade['target_price']:.2f}")

                        exit_order_response = kite.place_order(
                            variety=kite.VARIETY_REGULAR,
                            exchange=kite.EXCHANGE_NSE,
                            tradingsymbol=trade["symbol"],
                            transaction_type=exit_transaction_type,
                            quantity=trade["quantity"],
                            product=exit_product_type,
                            order_type=kite.ORDER_TYPE_LIMIT,
                            price=trade["target_price"],
                            tag=f"SCALPER_{trade['trade_type']}_EXIT"
                        )
                        exit_order_id = exit_order_response 
                        if exit_order_id:
                            trade["exit_order_id"] = exit_order_id
                            logger.info(f"Placed Exit Order for {trade['symbol']}: {exit_order_id} @ ₹{trade['target_price']:.2f}")
                        else:
                            logger.error(f"Failed to get order_id for exit order of {trade['symbol']}. Response: {exit_order_response}")
                elif initial_order_status and initial_order_status["status"] in ["CANCELLED", "REJECTED"]:
                    logger.warning(f"Initial {trade['trade_type']} Order for {trade['symbol']} {initial_order_status['status']}. Removing from pending trades.")
                    del pending_trades[order_id]
                    continue

            if trade["initial_completed"] and not trade["exit_completed"] and trade["exit_order_id"]:
                exit_order_status = get_order_details_from_history(trade["exit_order_id"])
                
                if exit_order_status and exit_order_status["status"] == "COMPLETE":
                    trade["exit_completed"] = True
                    executed_exit_price = exit_order_status.get("average_price", trade["target_price"])
                    
                    profit = 0
                    if trade["trade_type"] == 'LONG':
                        profit = (executed_exit_price - trade["initial_price"]) * trade["quantity"]
                    elif trade["trade_type"] == 'SHORT':
                        profit = (trade["initial_price"] - executed_exit_price) * trade["quantity"]

                    total_profit += profit
                    trades_completed += 1
                    logger.info(f"✅ Trade Successful! {trade['symbol']} ({trade['trade_type']}) | Profit: ₹{profit:.2f} | Total Profit: ₹{total_profit:.2f}")
                    del pending_trades[order_id]
                    continue

                with price_lock:
                    idx = token_to_index.get(trade["instrument_token"])
                    if idx is not None:
                        current_ltp = df.at[idx, 'last_price']
                        if current_ltp == 0.0:
                            continue

                        new_target_price = trade["target_price"]

                        if trade["trade_type"] == 'LONG':
                            if current_ltp > trade["target_price"] * 1.003:
                                new_target_price = round(current_ltp * 0.998 / 0.05) * 0.05
                                if new_target_price > trade["target_price"]:
                                    try:
                                        kite.modify_order(
                                            variety="regular",
                                            order_id=trade["exit_order_id"],
                                            price=new_target_price
                                        )
                                        trade["target_price"] = new_target_price
                                        logger.info(f"🔼 Trailing LONG exit order for {trade['symbol']} to ₹{new_target_price:.2f}")
                                    except Exception as e:
                                        logger.error(f"⚠️ Error modifying LONG exit order for {trade['symbol']}: {e}")
                        elif trade["trade_type"] == 'SHORT':
                            if current_ltp < trade["target_price"] * 0.997:
                                new_target_price = round(current_ltp * 1.002 / 0.05) * 0.05
                                if new_target_price < trade["target_price"]:
                                    try:
                                        kite.modify_order(
                                            variety="regular",
                                            order_id=trade["exit_order_id"],
                                            price=new_target_price
                                        )
                                        trade["target_price"] = new_target_price
                                        logger.info(f"🔽 Trailing SHORT exit order for {trade['symbol']} to ₹{new_target_price:.2f}")
                                    except Exception as e:
                                        logger.error(f"⚠️ Error modifying SHORT exit order for {trade['symbol']}: {e}")
            
            if order_age > MAX_ORDER_TIME and not trade["exit_attempted"]:
                logger.info(f"⏳ Trade Timeout ({MAX_ORDER_TIME}s) for {trade['symbol']} ({trade['trade_type']}). Initiating exit.")
                trade["exit_attempted"] = True

                if not trade["initial_completed"]:
                    try:
                        kite.cancel_order(variety="regular", order_id=trade["initial_order_id"])
                        logger.info(f"Cancelled pending initial {trade['trade_type']} order {trade['initial_order_id']} for {trade['symbol']}")
                    except Exception as e:
                        logger.warning(f"Could not cancel initial {trade['trade_type']} order {trade['initial_order_id']}: {e}")

                if trade["initial_completed"] and not trade["exit_completed"] and trade["exit_order_id"]:
                    try:
                        kite.cancel_order(variety="regular", order_id=trade["exit_order_id"])
                        logger.info(f"Cancelled pending exit order {trade['exit_order_id']} for {trade['symbol']}")
                    except Exception as e:
                        logger.warning(f"Could not cancel exit order {trade['exit_order_id']}: {e}")
                    
                try:
                    positions_data = kite.positions()
                    if positions_data and 'net' in positions_data:
                        current_position_qty = 0
                        for pos in positions_data["net"]:
                            if pos["tradingsymbol"] == trade["symbol"] and pos["quantity"] != 0:
                                current_position_qty = pos["quantity"]
                                break
                        
                        if current_position_qty != 0:
                            force_exit_transaction_type = kite.TRANSACTION_TYPE_SELL if current_position_qty > 0 else kite.TRANSACTION_TYPE_BUY
                            force_exit_product_type = kite.PRODUCT_CNC if trade["trade_type"] == 'LONG' else kite.PRODUCT_MIS

                            logger.warning(f"Forcing market exit for {trade['symbol']} {abs(current_position_qty)} @ MARKET (Timeout exit, Type: {force_exit_transaction_type})")
                            kite.place_order(
                                variety=kite.VARIETY_REGULAR,
                                exchange=kite.EXCHANGE_NSE,
                                tradingsymbol=trade["symbol"],
                                transaction_type=force_exit_transaction_type,
                                quantity=abs(current_position_qty),
                                product=force_exit_product_type,
                                order_type=kite.ORDER_TYPE_MARKET,
                                tag="SCALPER_FORCE_EXIT"
                            )
                        else:
                            del pending_trades[order_id]
                            logger.info(f"Trade for {trade['symbol']} removed after timeout and exit attempt (no open position).")
                    else:
                        logger.warning(f"Could not retrieve positions for timeout check of {trade['symbol']}. Assuming no open position.")
                        del pending_trades[order_id]
                        
                except Exception as e:
                    logger.error(f"⚠️ Error forcing exit for {trade['symbol']} on timeout: {e}")
        except Exception as e:
            logger.error(f"⚠️ Error managing order {order_id} for {trade['symbol']}: {e}")

# --- WebSocket Handlers ---

def on_ticks(ws, ticks):
    """Callback for receiving WebSocket ticks."""
    global ws_connected
    ws_connected = True
    
    with price_lock:
        for tick in ticks:
            token = tick['instrument_token']
            price = tick.get('last_price', 0.0)
            idx = token_to_index.get(token)
            if idx is not None and price > 0:
                df.at[idx, 'last_price'] = price
                price_history = df.at[idx, 'price_history']
                price_history.append(price)
                if len(price_history) > max(OSCILLATION_PERIOD, LONG_EMA_PERIOD) * 2:
                    price_history.pop(0)
                df.at[idx, 'price_history'] = price_history

def on_connect(ws, response):
    """Callback for WebSocket connection."""
    global ws_connected
    ws_connected = True
    logger.info("WebSocket connected")
    if current_subscriptions:
        ws.subscribe(current_subscriptions)
        ws.set_mode(ws.MODE_QUOTE, current_subscriptions)
        logger.info(f"Resubscribed to {len(current_subscriptions)} tokens.")

def on_error(ws, code, reason):
    """Callback for WebSocket errors."""
    global ws_connected
    ws_connected = False
    logger.error(f"WebSocket Error: {code} - {reason}")

def on_close(ws, code, reason):
    """Callback for WebSocket close events."""
    global ws_connected
    ws_connected = False
    logger.info(f"WebSocket Closed: {code} - {reason}")

def initialize_websocket():
    """Initializes and connects the KiteTicker WebSocket."""
    global kws, websocket_thread
    if kws and kws.is_connected():
        logger.info("WebSocket already connected.")
        return

    logger.info("Initializing WebSocket...")
    try:
        kws = KiteTicker(api_key=api_key, access_token=f"{enctoken}&user_id={user_id}")
        kws.on_ticks = on_ticks
        kws.on_connect = on_connect
        kws.on_error = on_error
        kws.on_close = on_close
        
        websocket_thread = Thread(target=kws.connect, daemon=True)
        websocket_thread.start()
        
        time.sleep(2)
        if not kws.is_connected():
            logger.warning("WebSocket did not connect immediately. Will retry in main loop.")
            
    except Exception as e:
        logger.error(f"Failed to initialize WebSocket: {e}")

def update_subscriptions(new_tokens):
    """Updates the WebSocket subscriptions."""
    global current_subscriptions
    
    if not kws or not kws.is_connected():
        logger.warning("WebSocket not connected for subscription update. Caching tokens.")
        current_subscriptions = new_tokens
        return
    
    try:
        if set(new_tokens) != set(current_subscriptions):
            to_unsubscribe = list(set(current_subscriptions) - set(new_tokens))
            if to_unsubscribe:
                logger.debug(f"Unsubscribing from {len(to_unsubscribe)} tokens.")
                kws.unsubscribe(to_unsubscribe)
            
            to_subscribe = list(set(new_tokens) - set(current_subscriptions))
            if to_subscribe:
                logger.debug(f"Subscribing to {len(to_subscribe)} tokens.")
                kws.subscribe(to_subscribe)
                kws.set_mode(kws.MODE_QUOTE, to_subscribe)
            
            current_subscriptions = new_tokens
            logger.info(f"WebSocket subscriptions updated to {len(new_tokens)} tokens.")
        else:
            logger.debug("No change in subscriptions needed.")
    except Exception as e:
        logger.error(f"Subscription update error: {e}")

def rotate_symbol_subscription():
    """Selects the best oscillating stocks based on oscillation score."""
    if df.empty:
        logger.error("No symbols available for subscription.")
        return []
    
    active_trade_tokens = {trade["instrument_token"] for trade in pending_trades.values()}
    new_tokens = list(active_trade_tokens)  # Always include active trades

    # Filter out active trade tokens from the pool
    available_for_sampling = df[~df['instrument_token'].isin(active_trade_tokens)]
    candidates = []

    with price_lock:
        for idx, row in available_for_sampling.iterrows():
            prices = row['price_history']
            token = row['instrument_token']
            symbol = row['tradingsymbol']
            
            # Initialize price history if empty
            if not prices:
                hist_data = get_price_history(symbol, token)
                if hist_data:
                    df.at[idx, 'price_history'] = hist_data[-max(OSCILLATION_PERIOD, LONG_EMA_PERIOD)*2:]
                    prices = df.at[idx, 'price_history']
            
            if len(prices) >= OSCILLATION_PERIOD:
                score = calculate_oscillation_score(prices)
                if score > 0:  # Only include stocks with valid oscillation scores
                    candidates.append((token, score, symbol))

    # Sort candidates by oscillation score (descending)
    candidates.sort(key=lambda x: x[1], reverse=True)
    
    # Select top candidates, respecting MAX_CONCURRENT_SUBS
    num_to_add = min(MAX_CONCURRENT_SUBS - len(new_tokens), len(candidates))
    if num_to_add > 0:
        top_candidates = candidates[:num_to_add]
        new_tokens.extend([token for token, score, symbol in top_candidates])
        # Log top candidate for transparency
        if top_candidates:
            top_symbol = top_candidates[0][2]
            top_score = top_candidates[0][1]
            logger.info(f"Top oscillating stock: {top_symbol} (Score: {top_score:.4f})")

    # Fallback to random selection if no candidates
    if len(new_tokens) < MAX_CONCURRENT_SUBS and candidates:
        num_to_add = MAX_CONCURRENT_SUBS - len(new_tokens)
        remaining_pool = available_for_sampling[~available_for_sampling['instrument_token'].isin(new_tokens)]
        if not remaining_pool.empty:
            batch = remaining_pool.sample(min(num_to_add, len(remaining_pool)))
            new_tokens.extend(batch['instrument_token'].tolist())

    return new_tokens

def run_bot():
    """Main trading bot loop."""
    initialize_websocket()
    
    last_rotation = datetime.now()
    
    logger.info("Starting trading loop...")
    
    while True:
        try:
            if not ws_connected or (kws and not kws.is_connected()):
                logger.warning("WebSocket not connected. Attempting reconnection...")
                time.sleep(RECONNECT_DELAY)
                initialize_websocket()
                continue

            if (datetime.now() - last_rotation).total_seconds() > WS_SUBSCRIBE_TIME:
                new_tokens_to_subscribe = rotate_symbol_subscription()
                if new_tokens_to_subscribe:
                    update_subscriptions(new_tokens_to_subscribe)
                    symbols_monitoring = df[df['instrument_token'].isin(new_tokens_to_subscribe)]['tradingsymbol'].tolist()
                    logger.info(f"Currently monitoring: {', '.join(symbols_monitoring)}")
                last_rotation = datetime.now()
            
            manage_orders()
            
            if len(pending_trades) < MAX_ACTIVE_TRADES:
                with price_lock:
                    # Prioritize top oscillating stocks (first in current_subscriptions)
                    for token in current_subscriptions:
                        idx = token_to_index.get(token)
                        if idx is None:
                            continue

                        symbol = df.at[idx, 'tradingsymbol']
                        price_history = df.at[idx, 'price_history']
                        ltp = df.at[idx, 'last_price']
                        
                        if len(price_history) >= max(OSCILLATION_PERIOD, LONG_EMA_PERIOD * 2) and MIN_PRICE <= ltp <= MAX_PRICE:
                            try:
                                if any(trade['symbol'] == symbol for trade in pending_trades.values()):
                                    logger.debug(f"Skipping {symbol}, already in a pending trade.")
                                    continue
                                
                                trend = determine_trend(price_history)
                                is_oscillating = is_volatile_and_oscillating(price_history)

                                if trend == 'positive' and is_oscillating and find_entry_point_long(price_history):
                                    logger.info(f"💡 Found LONG opportunity in {symbol} (LTP: {ltp}, Trend: {trend})")
                                    place_trade_order(symbol, token, ltp, 'positive')
                                elif trend == 'negative' and is_oscillating and find_entry_point_short(price_history):
                                    logger.info(f"💡 Found SHORT opportunity in {symbol} (LTP: {ltp}, Trend: {trend})")
                                    place_trade_order(symbol, token, ltp, 'negative')
                                elif trend == 'neutral' and is_oscillating and find_entry_point_long(price_history):
                                     logger.info(f"💡 Found NEUTRAL (LONG) opportunity in {symbol} (LTP: {ltp}, Trend: {trend})")
                                     place_trade_order(symbol, token, ltp, 'neutral')

                            except Exception as e:
                                logger.error(f"Error evaluating {symbol}: {str(e)}")
            else:
                logger.info(f"Reached max active trades ({MAX_ACTIVE_TRADES}). Waiting for trades to complete.")

            current_active_symbols = ", ".join({trade['symbol'] for trade in pending_trades.values()}) if pending_trades else "None"
            logger.info(f"Active Trades: {current_active_symbols} | Total Profit: ₹{total_profit:.2f} | Trades Completed: {trades_completed}")
            
            time.sleep(1)

        except Exception as e:
            logger.critical(f"Unhandled exception in main bot loop: {e}", exc_info=True)
            time.sleep(RECONNECT_DELAY * 2)

if __name__ == "__main__":
    run_bot()