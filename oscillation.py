import os
import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from kite_trade import * # This is good for API calls
from kiteconnect import KiteTicker # This is good for WebSocket
from dotenv import load_dotenv
import logging
from threading import Lock, Thread

# --- Configuration and Initialization ---

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
api_key = 'API_KEY' # Make sure to replace 'API_KEY' with your actual API key.

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
CAPITAL = 100000
TARGET_PROFIT_PER_TRADE = 183
MAX_ORDER_TIME = 300 # Reduced to 5 minutes (300 seconds) for faster exits
RISK_PER_TRADE = 0.01 # Keep as percentage of capital
OSCILLATION_PERIOD = 30 # Increased period for better volatility assessment from tick data
VOLATILITY_THRESHOLD = 0.001 # Minimum percentage movement required (e.1% within period)
WS_SUBSCRIBE_TIME = 30 # Shorter subscription rotation
MAX_CONCURRENT_SUBS = 20 # Increased concurrent subscriptions to monitor more stocks
RECONNECT_DELAY = 5 # seconds between reconnect attempts
MIN_PRICE = 100 # Minimum stock price to consider
MAX_PRICE = 3000 # Maximum stock price to consider
MAX_ACTIVE_TRADES = 1 # Limit concurrent active (pending) trades

# Trend analysis parameters
SHORT_EMA_PERIOD = 5  # Shorter EMA period for trend detection
LONG_EMA_PERIOD = 20  # Longer EMA period for trend detection
TREND_STRENGTH_THRESHOLD = 0.0005 # Minimum difference in EMAs for a strong trend indication

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
    # df = df[df['last_price'] > 0] # Filter out instruments with no last price (REMOVE for testing outside market hours if not already commented)
    df = df[['instrument_token', 'tradingsymbol', 'last_price']].reset_index(drop=True)
    if df.empty:
        logger.critical("No valid symbols found after filtering instruments. Check data_500.csv, NSE instruments, and filters.")
        exit()
except Exception as e:
    logger.critical(f"Error loading instruments: {e}. Ensure API is working and internet connection.")
    exit()

# Initialize price tracking
df['last_price'] = 0.0
df['price_history'] = df['tradingsymbol'].apply(lambda x: []) # This will be filled with tick data
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
    
    # Target a 0.2% profit per share for both long and short
    target_profit_percentage = 0.002
    target_profit_per_share = price * target_profit_percentage
    
    if target_profit_per_share <= 0:
        return 0
    
    quantity = int(TARGET_PROFIT_PER_TRADE / target_profit_per_share)
    
    # Ensure quantity is at least 1 and within capital limits
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
    """
    Determines the short-term trend based on EMA crossovers and current price position.
    Returns 'positive', 'negative', or 'neutral'.
    """
    if len(prices) < max(SHORT_EMA_PERIOD, LONG_EMA_PERIOD) * 2: # Need enough data for reliable EMA
        return 'neutral'

    short_ema = calculate_ema(prices, SHORT_EMA_PERIOD)
    long_ema = calculate_ema(prices, LONG_EMA_PERIOD)
    current_price = prices[-1]

    if short_ema is None or long_ema is None:
        return 'neutral'

    # Check for positive trend
    if short_ema > long_ema and current_price > short_ema and (short_ema - long_ema) / long_ema > TREND_STRENGTH_THRESHOLD:
        return 'positive'
    
    # Check for negative trend
    if short_ema < long_ema and current_price < short_ema and (long_ema - short_ema) / long_ema > TREND_STRENGTH_THRESHOLD:
        return 'negative'
    
    return 'neutral'

def is_volatile_and_oscillating(prices):
    """
    Determines if prices are showing sufficient volatility and oscillating behavior.
    Uses tick data for faster movement detection.
    """
    if len(prices) < OSCILLATION_PERIOD:
        return False
    
    recent_prices = np.array(prices[-OSCILLATION_PERIOD:])
    
    # Check for sufficient movement (volatility) within the period
    price_range = np.max(recent_prices) - np.min(recent_prices)
    if price_range == 0: # Price is stuck, no movement
        return False
    
    # Calculate percentage movement relative to average price
    avg_price_in_period = np.mean(recent_prices)
    if avg_price_in_period == 0: return False

    percentage_movement = price_range / avg_price_in_period
    
    if percentage_movement < VOLATILITY_THRESHOLD: # Not moving enough
        return False
        
    # Standard deviation to check for non-trending, somewhat chaotic movement
    std_dev = np.std(recent_prices)
    if std_dev / avg_price_in_period > 0.008: # Too volatile, possibly trending
        return False

    # Check for mean reversion pattern (price crossing SMA)
    # This helps distinguish oscillation from a general upward/downward trend
    sma = np.mean(recent_prices)
    crosses = 0
    for i in range(1, len(recent_prices)):
        if (recent_prices[i-1] < sma and recent_prices[i] > sma) or \
           (recent_prices[i-1] > sma and recent_prices[i] < sma):
            crosses += 1
            
    if crosses < 2: # Not enough oscillation (crosses of SMA)
        return False
            
    return True

def find_entry_point_long(prices):
    """
    Identifies a potential buy entry point in an oscillating market using tick data.
    Looks for a dip within the oscillating range suitable for a long position.
    """
    if len(prices) < OSCILLATION_PERIOD:
        return False
    
    current_price = prices[-1]
    
    # Short-term SMA (e.g., last 5 ticks)
    sma_short = np.mean(prices[-5:])
    
    # Long-term SMA (e.g., last OSCILLATION_PERIOD ticks)
    sma_long = np.mean(prices[-OSCILLATION_PERIOD:])
    
    # Condition 1: Current price is below the short-term SMA (indicates a recent dip)
    if current_price >= sma_short:
        return False
        
    # Condition 2: Short-term SMA should not be significantly above long-term SMA,
    # and ideally, it should be slightly below or close to it, confirming a dip within a range.
    if sma_short > sma_long * 1.001: # Avoid buying if short-term is too strong up
        return False
        
    # Condition 3: Price should be in the lower half of the recent oscillation range (but not breaking down)
    recent_min = np.min(prices[-OSCILLATION_PERIOD:])
    recent_max = np.max(prices[-OSCILLATION_PERIOD:])
    
    if recent_max == recent_min: # Flat prices, no entry
        return False

    # Price should be closer to the recent minimum, but not exactly at it if it's breaking down.
    # Check if current price is within the lower 40% of the range
    if current_price > (recent_min + (recent_max - recent_min) * 0.4):
        return False

    # Condition 4: Current price is lower than the price 'n' ticks ago within the short-term trend
    # This specifically looks for a *recent* downward tick or two before the entry
    if len(prices) >= 3 and current_price < prices[-2]: # Check if current is lower than previous tick
        return True

    return False

def find_entry_point_short(prices):
    """
    Identifies a potential sell entry point in an oscillating market using tick data.
    Looks for a peak within the oscillating range suitable for a short position.
    """
    if len(prices) < OSCILLATION_PERIOD:
        return False
    
    current_price = prices[-1]
    
    # Short-term SMA (e.g., last 5 ticks)
    sma_short = np.mean(prices[-5:])
    
    # Long-term SMA (e.g., last OSCILLATION_PERIOD ticks)
    sma_long = np.mean(prices[-OSCILLATION_PERIOD:])
    
    # Condition 1: Current price is above the short-term SMA (indicates a recent peak)
    if current_price <= sma_short:
        return False
        
    # Condition 2: Short-term SMA should not be significantly below long-term SMA,
    # and ideally, it should be slightly above or close to it, confirming a peak within a range.
    if sma_short < sma_long * 0.999: # Avoid selling if short-term is too strong down
        return False
        
    # Condition 3: Price should be in the upper half of the recent oscillation range (but not breaking up)
    recent_min = np.min(prices[-OSCILLATION_PERIOD:])
    recent_max = np.max(prices[-OSCILLATION_PERIOD:])
    
    if recent_max == recent_min: # Flat prices, no entry
        return False

    # Price should be closer to the recent maximum, but not exactly at it if it's breaking up.
    # Check if current price is within the upper 40% of the range
    if current_price < (recent_max - (recent_max - recent_min) * 0.4):
        return False

    # Condition 4: Current price is higher than the price 'n' ticks ago within the short-term trend
    # This specifically looks for a *recent* upward tick or two before the entry
    if len(prices) >= 3 and current_price > prices[-2]: # Check if current is higher than previous tick
        return True

    return False


def place_trade_order(symbol, instrument_token, current_ltp, trend_direction):
    """
    Places trade orders (buy for long, sell for short) based on trend direction.
    """
    global pending_trades

    if len(pending_trades) >= MAX_ACTIVE_TRADES:
        logger.info(f"Max active trades ({MAX_ACTIVE_TRADES}) reached. Skipping {symbol}.")
        return False

    try:
        quantity = 0
        order_type = ""
        product_type = ""
        transaction_type = ""
        target_price = 0.0
        trade_type = ""

        if trend_direction == 'positive' or trend_direction == 'neutral':
            trade_type = 'LONG'
            quantity = calculate_quantity(current_ltp, is_short_sell=False)
            product_type = kite.PRODUCT_CNC # Delivery for long-term/swing
            transaction_type = kite.TRANSACTION_TYPE_BUY
            # Target 0.2% profit per share for long
            target_price = round(current_ltp * 1.002 / 0.05) * 0.05
            logger.info(f"📈 Placing LONG Buy Order: {symbol} | Qty: {quantity} @ MARKET | Target Sell: ₹{target_price:.2f} (CNC)")

        elif trend_direction == 'negative':
            trade_type = 'SHORT'
            quantity = calculate_quantity(current_ltp, is_short_sell=True)
            product_type = kite.PRODUCT_MIS # Intraday for short-selling
            transaction_type = kite.TRANSACTION_TYPE_SELL
            # Target 0.2% profit per share for short (buy back lower)
            target_price = round(current_ltp * 0.998 / 0.05) * 0.05 # Target 0.2% lower for buyback
            logger.info(f"📉 Placing SHORT Sell Order: {symbol} | Qty: {quantity} @ MARKET | Target Buy: ₹{target_price:.2f} (MIS)")
        else:
            logger.warning(f"No clear trend for {symbol}. Skipping order placement.")
            return False

        if quantity == 0:
            logger.warning(f"Invalid quantity for {symbol} at {current_ltp}. Skipping order.")
            return False

        # Place the initial order (Buy for Long, Sell for Short)
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

        # Store pending trade information
        trade_info = {
            "symbol": symbol,
            "instrument_token": instrument_token,
            "initial_order_id": initial_order_id,
            "exit_order_id": None,
            "quantity": quantity,
            "initial_price": None, # This will be the avg buy price for long, or avg sell price for short
            "target_price": target_price,
            "order_time": datetime.now(),
            "initial_completed": False,
            "exit_completed": False,
            "exit_attempted": False,
            "trade_type": trade_type # 'LONG' or 'SHORT'
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
            # Check initial order status
            if not trade["initial_completed"]:
                initial_order_status = get_order_details_from_history(trade["initial_order_id"])
                
                if initial_order_status and initial_order_status["status"] == "COMPLETE":
                    trade["initial_completed"] = True
                    trade["initial_price"] = initial_order_status["average_price"]
                    logger.info(f"✅ Initial {trade['trade_type']} Order COMPLETE for {trade['symbol']} @ ₹{trade['initial_price']:.2f}")

                    # Immediately place the exit limit order after initial is complete
                    if trade["exit_order_id"] is None:
                        exit_transaction_type = kite.TRANSACTION_TYPE_SELL if trade["trade_type"] == 'LONG' else kite.TRANSACTION_TYPE_BUY
                        exit_product_type = kite.PRODUCT_CNC if trade["trade_type"] == 'LONG' else kite.PRODUCT_MIS

                        # Ensure target price is profitable and respects tick size
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
                            order_type=kite.ORDER_TYPE_LIMIT, # Use LIMIT for target profit/loss
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

            # Check exit order status (if placed)
            if trade["initial_completed"] and not trade["exit_completed"] and trade["exit_order_id"]:
                exit_order_status = get_order_details_from_history(trade["exit_order_id"])
                
                if exit_order_status and exit_order_status["status"] == "COMPLETE":
                    trade["exit_completed"] = True
                    executed_exit_price = exit_order_status.get("average_price", trade["target_price"])
                    
                    profit = 0
                    if trade["trade_type"] == 'LONG':
                        profit = (executed_exit_price - trade["initial_price"]) * trade["quantity"]
                    elif trade["trade_type"] == 'SHORT':
                        # For short, initial price is sell price, executed_exit_price is buy-to-cover price
                        profit = (trade["initial_price"] - executed_exit_price) * trade["quantity"]

                    total_profit += profit
                    trades_completed += 1
                    logger.info(f"✅ Trade Successful! {trade['symbol']} ({trade['trade_type']}) | Profit: ₹{profit:.2f} | Total Profit: ₹{total_profit:.2f}")
                    del pending_trades[order_id]
                    continue

                # Trailing logic for exit order
                with price_lock:
                    idx = token_to_index.get(trade["instrument_token"])
                    if idx is not None:
                        current_ltp = df.at[idx, 'last_price']
                        if current_ltp == 0.0:
                            continue

                        new_target_price = trade["target_price"] # Initialize with current target

                        if trade["trade_type"] == 'LONG':
                            # If LTP moved up significantly (e.g., 0.3% from current target), trail stop up
                            if current_ltp > trade["target_price"] * 1.003: 
                                new_target_price = round(current_ltp * 0.998 / 0.05) * 0.05 # Target 0.2% below new LTP
                                if new_target_price > trade["target_price"]: # Only modify if price moves favorably
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
                            # If LTP moved down significantly (e.g., 0.3% from current target), trail stop down
                            if current_ltp < trade["target_price"] * 0.997: # Price decreased significantly
                                new_target_price = round(current_ltp * 1.002 / 0.05) * 0.05 # Target 0.2% above new LTP
                                if new_target_price < trade["target_price"]: # Only modify if price moves favorably
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
            
            # Timeout logic for pending trades
            if order_age > MAX_ORDER_TIME and not trade["exit_attempted"]:
                logger.info(f"⏳ Trade Timeout ({MAX_ORDER_TIME}s) for {trade['symbol']} ({trade['trade_type']}). Initiating exit.")
                trade["exit_attempted"] = True

                # Cancel any pending orders for this trade
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
                    
                # Force exit any open position (market order)
                try:
                    positions_data = kite.positions()
                    if positions_data and 'net' in positions_data:
                        current_position_qty = 0
                        for pos in positions_data["net"]:
                            if pos["tradingsymbol"] == trade["symbol"] and pos["quantity"] != 0:
                                current_position_qty = pos["quantity"]
                                break
                        
                        if current_position_qty != 0:
                            # Determine transaction type for force exit
                            # If current_position_qty > 0 (long position), need to SELL
                            # If current_position_qty < 0 (short position), need to BUY
                            force_exit_transaction_type = kite.TRANSACTION_TYPE_SELL if current_position_qty > 0 else kite.TRANSACTION_TYPE_BUY
                            force_exit_product_type = kite.PRODUCT_CNC if trade["trade_type"] == 'LONG' else kite.PRODUCT_MIS # Use original product for forced exit

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
                        else: # No open position found for this symbol, can remove
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
                
                # Update price history, ensuring it's for the correct token and with tick data
                price_history = df.at[idx, 'price_history']
                price_history.append(price)
                # Keep only relevant history for oscillation and trend analysis (e.g., 2 times long EMA period)
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
        # Only update if there's a change in tokens to avoid unnecessary API calls
        if set(new_tokens) != set(current_subscriptions):
            # Unsubscribe from tokens that are no longer needed
            to_unsubscribe = list(set(current_subscriptions) - set(new_tokens))
            if to_unsubscribe:
                logger.debug(f"Unsubscribing from {len(to_unsubscribe)} tokens.")
                kws.unsubscribe(to_unsubscribe)
            
            # Subscribe to new tokens
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
    """
    Selects a new batch of symbols to subscribe to, prioritizing
    active trades and then randomly selecting new potential oscillating stocks.
    
    Now also tries to filter for potentially more volatile stocks initially.
    """
    if df.empty:
        logger.error("No symbols available for subscription.")
        return []
    
    active_trade_tokens = {trade["instrument_token"] for trade in pending_trades.values()}
    new_tokens = list(active_trade_tokens) # Always include active trades

    # Filter out active trade tokens from the general pool for new selections
    available_for_sampling = df[~df['instrument_token'].isin(active_trade_tokens)]
    
    # Heuristic for selecting potentially more volatile stocks:
    # 1. Ensure enough recent history (from previous runs or initial load)
    # 2. Check for a minimum recent price range/movement within their existing history
    potential_candidates = []
    with price_lock:
        for idx, row in available_for_sampling.iterrows():
            prices = row['price_history']
            if len(prices) >= OSCILLATION_PERIOD:
                recent_prices = np.array(prices[-OSCILLATION_PERIOD:])
                if np.max(recent_prices) - np.min(recent_prices) > row['last_price'] * VOLATILITY_THRESHOLD * 0.5: # Half threshold for initial check
                    potential_candidates.append(row['instrument_token'])
    
    # If not enough potential candidates based on history, fall back to random
    if len(potential_candidates) < MAX_CONCURRENT_SUBS - len(active_trade_tokens):
        # Fallback to pure random if not enough volatile stocks found
        num_to_add = MAX_CONCURRENT_SUBS - len(new_tokens)
        if num_to_add > 0 and not available_for_sampling.empty:
            # Randomly sample from those not already active or already considered.
            remaining_pool = available_for_sampling[~available_for_sampling['instrument_token'].isin(potential_candidates)]
            if not remaining_pool.empty:
                batch = remaining_pool.sample(min(num_to_add, len(remaining_pool)))
                new_tokens.extend(batch['instrument_token'].tolist())
    else:
        # If enough volatile candidates, pick from them
        num_to_add = MAX_CONCURRENT_SUBS - len(new_tokens)
        if num_to_add > 0 and potential_candidates:
            # Randomly select from the high-potential candidates
            selected_volatile_tokens = np.random.choice(potential_candidates, min(num_to_add, len(potential_candidates)), replace=False).tolist()
            new_tokens.extend(selected_volatile_tokens)

    # Initialize price history for new symbols if not already present, using minute data for initial fill
    with price_lock:
        for token in new_tokens:
            idx = token_to_index.get(token)
            if idx is not None and not df.at[idx, 'price_history']:
                symbol_name = df.at[idx, 'tradingsymbol']
                hist_data = get_price_history(symbol_name, token) # Uses minute data here
                if hist_data:
                    # Store enough history for EMA and oscillation analysis
                    df.at[idx, 'price_history'] = hist_data[-max(OSCILLATION_PERIOD, LONG_EMA_PERIOD)*2:]
                    logger.debug(f"Loaded initial minute history for {symbol_name}.")
    
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

            # Rotate subscriptions periodically
            if (datetime.now() - last_rotation).total_seconds() > WS_SUBSCRIBE_TIME:
                new_tokens_to_subscribe = rotate_symbol_subscription()
                if new_tokens_to_subscribe:
                    update_subscriptions(new_tokens_to_subscribe)
                    symbols_monitoring = df[df['instrument_token'].isin(new_tokens_to_subscribe)]['tradingsymbol'].tolist()
                    logger.info(f"Currently monitoring: {', '.join(symbols_monitoring)}")
                last_rotation = datetime.now()
            
            # Manage existing orders
            manage_orders()
            
            # Look for new opportunities if we haven't reached MAX_ACTIVE_TRADES
            if len(pending_trades) < MAX_ACTIVE_TRADES:
                with price_lock:
                    for token in current_subscriptions: # Iterate through currently subscribed symbols
                        idx = token_to_index.get(token)
                        if idx is None:
                            continue

                        symbol = df.at[idx, 'tradingsymbol']
                        price_history = df.at[idx, 'price_history'] # This is the tick-based history
                        ltp = df.at[idx, 'last_price']
                        
                        # Only consider if we have enough price history and price is within range
                        if len(price_history) >= max(OSCILLATION_PERIOD, LONG_EMA_PERIOD * 2) and MIN_PRICE <= ltp <= MAX_PRICE:
                            try:
                                # Check if this symbol is already part of an active trade
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
                                elif trend == 'neutral' and is_oscillating and find_entry_point_long(price_history): # Default to long for neutral oscillating
                                     logger.info(f"💡 Found NEUTRAL (LONG) opportunity in {symbol} (LTP: {ltp}, Trend: {trend})")
                                     place_trade_order(symbol, token, ltp, 'neutral')

                            except Exception as e:
                                logger.error(f"Error evaluating {symbol}: {str(e)}")
            else:
                logger.info(f"Reached max active trades ({MAX_ACTIVE_TRADES}). Waiting for trades to complete.")

            current_active_symbols = ", ".join({trade['symbol'] for trade in pending_trades.values()}) if pending_trades else "None"
            logger.info(f"Active Trades: {current_active_symbols} | Total Profit: ₹{total_profit:.2f} | Trades Completed: {trades_completed}")
            
            time.sleep(1) # Small delay to prevent excessive looping

        except Exception as e:
            logger.critical(f"Unhandled exception in main bot loop: {e}", exc_info=True)
            time.sleep(RECONNECT_DELAY * 2) # Longer delay on critical errors

if __name__ == "__main__":
    run_bot()