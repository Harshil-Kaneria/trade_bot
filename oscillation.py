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
CAPITAL = 50000
TARGET_PROFIT_PER_TRADE = 180
MAX_ORDER_TIME = 300 # Reduced to 5 minutes (300 seconds) for faster exits
RISK_PER_TRADE = 0.01 # Keep as percentage of capital
OSCILLATION_PERIOD = 30 # Increased period for better volatility assessment from tick data
VOLATILITY_THRESHOLD = 0.001 # Minimum percentage movement required (e.g., 0.1% within period)
WS_SUBSCRIBE_TIME = 30 # Shorter subscription rotation
MAX_CONCURRENT_SUBS = 20 # Increased concurrent subscriptions to monitor more stocks
RECONNECT_DELAY = 5 # seconds between reconnect attempts
MIN_PRICE = 100 # Minimum stock price to consider
MAX_PRICE = 3000 # Maximum stock price to consider
MAX_ACTIVE_TRADES = 1 # Limit concurrent active (pending) trades

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
    # ❌ REMOVE or COMMENT OUT this line if initializing outside market hours
    # df = df[df['last_price'] > 0] # Filter out instruments with no last price
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

def calculate_quantity(price):
    """Calculates quantity based on target profit and price."""
    if price <= 0:
        return 0
    
    # Calculate quantity to achieve TARGET_PROFIT_PER_TRADE with a small price move
    # Assuming we target a 0.2% profit per share (as implied by target sell price)
    target_profit_per_share = price * 0.002
    if target_profit_per_share <= 0:
        return 0
    
    quantity = int(TARGET_PROFIT_PER_TRADE / target_profit_per_share)
    
    # Ensure quantity is at least 1 and within capital limits
    quantity = max(1, quantity)
    quantity = min(quantity, int(CAPITAL / price))
    
    return quantity


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


def find_entry_point(prices):
    """
    Identifies a potential buy entry point in an oscillating market using tick data.
    Looks for a dip within the oscillating range.
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


def place_scalp_order(symbol, instrument_token, current_ltp):
    """
    Places buy and sell orders for scalping.
    Now supports market orders for buy and limit for sell for quicker entry.
    """
    global pending_trades

    if len(pending_trades) >= MAX_ACTIVE_TRADES:
        logger.info(f"Max active trades ({MAX_ACTIVE_TRADES}) reached. Skipping {symbol}.")
        return False

    try:
        quantity = calculate_quantity(current_ltp)
        if quantity == 0:
            logger.warning(f"Invalid quantity for {symbol} at {current_ltp}. Skipping order.")
            return False

        # Target 0.2% profit per share
        # Ensure sell price target respects NSE tick size (0.05)
        sell_price_target = round(current_ltp * 1.002 / 0.05) * 0.05
        
        logger.info(f"📈 Placing Buy Order: {symbol} | Qty: {quantity} @ MARKET | Target Sell: ₹{sell_price_target:.2f}")

        # Place buy order (Market Order for instant execution)
        buy_order_response = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=symbol,
            transaction_type=kite.TRANSACTION_TYPE_BUY,
            quantity=quantity,
            product=kite.PRODUCT_CNC,
            order_type=kite.ORDER_TYPE_MARKET,
            tag="SCALPER_BUY"
        )
        # FIX: Your wrapper returns the order_id directly, not a dictionary with 'order_id' key
        buy_order_id = buy_order_response # Corrected this line as discussed

        if not buy_order_id:
            logger.error(f"Failed to get order_id for buy order of {symbol}. Response: {buy_order_response}")
            return False

        logger.info(f"Buy Order placed for {symbol}: {buy_order_id}")

        # Store pending trade information
        trade_info = {
            "symbol": symbol,
            "instrument_token": instrument_token,
            "buy_order_id": buy_order_id,
            "sell_order_id": None,
            "quantity": quantity,
            "buy_price": None,
            "sell_price_target": sell_price_target,
            "order_time": datetime.now(),
            "buy_completed": False,
            "sell_completed": False,
            "exit_attempted": False
        }
        pending_trades[buy_order_id] = trade_info
        
        return True

    except Exception as e:
        logger.error(f"❌ Order Placement Failed for {symbol}: {str(e)}")
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
    """Manages all pending orders, including placing sell orders and exiting on timeout."""
    global trades_completed, total_profit, pending_trades

    orders_to_check = list(pending_trades.keys())

    for order_id in orders_to_check:
        trade = pending_trades.get(order_id)
        if not trade:
            continue

        current_time = datetime.now()
        order_age = (current_time - trade["order_time"]).total_seconds()

        try:
            # Check buy order status
            if not trade["buy_completed"]:
                buy_order_status = get_order_details_from_history(trade["buy_order_id"])
                
                if buy_order_status and buy_order_status["status"] == "COMPLETE":
                    trade["buy_completed"] = True
                    trade["buy_price"] = buy_order_status["average_price"]
                    logger.info(f"✅ Buy Order COMPLETE for {trade['symbol']} @ ₹{trade['buy_price']:.2f}")

                    # Immediately place the sell limit order after buy is complete
                    if trade["sell_order_id"] is None:
                        # Ensure sell price is higher than buy price for profit
                        # Re-calculate target to ensure it's profitable and respects tick size
                        if trade["sell_price_target"] <= trade["buy_price"]:
                            trade["sell_price_target"] = round(trade["buy_price"] * 1.002 / 0.05) * 0.05
                            logger.warning(f"Adjusted sell target for {trade['symbol']} to ensure profit: ₹{trade['sell_price_target']:.2f}")
                            
                        sell_order_response = kite.place_order(
                            variety=kite.VARIETY_REGULAR,
                            exchange=kite.EXCHANGE_NSE,
                            tradingsymbol=trade["symbol"],
                            transaction_type=kite.TRANSACTION_TYPE_SELL,
                            quantity=trade["quantity"],
                            product=kite.PRODUCT_CNC,
                            order_type=kite.ORDER_TYPE_LIMIT, # Use LIMIT for target profit
                            price=trade["sell_price_target"],
                            tag="SCALPER_SELL"
                        )
                        # FIX: Your wrapper returns the order_id directly for sell orders too
                        sell_order_id = sell_order_response 
                        if sell_order_id:
                            trade["sell_order_id"] = sell_order_id
                            logger.info(f"Placed Sell Order for {trade['symbol']}: {sell_order_id} @ ₹{trade['sell_price_target']:.2f}")
                        else:
                            logger.error(f"Failed to get order_id for sell order of {trade['symbol']}. Response: {sell_order_response}")
                elif buy_order_status and buy_order_status["status"] in ["CANCELLED", "REJECTED"]:
                    logger.warning(f"Buy Order for {trade['symbol']} {buy_order_status['status']}. Removing from pending trades.")
                    del pending_trades[order_id]
                    continue

            # Check sell order status (if placed)
            if trade["buy_completed"] and not trade["sell_completed"] and trade["sell_order_id"]:
                sell_order_status = get_order_details_from_history(trade["sell_order_id"])
                
                if sell_order_status and sell_order_status["status"] == "COMPLETE":
                    trade["sell_completed"] = True
                    executed_sell_price = sell_order_status.get("average_price", trade["sell_price_target"])
                    profit = (executed_sell_price - trade["buy_price"]) * trade["quantity"]
                    total_profit += profit
                    trades_completed += 1
                    logger.info(f"✅ Trade Successful! {trade['symbol']} | Profit: ₹{profit:.2f} | Total Profit: ₹{total_profit:.2f}")
                    del pending_trades[order_id]
                    continue

                # Trailing logic for sell order (if buy completed and sell pending)
                with price_lock:
                    idx = token_to_index.get(trade["instrument_token"])
                    if idx is not None:
                        current_ltp = df.at[idx, 'last_price']
                        if current_ltp == 0.0:
                            continue

                        # Trailing by 0.2% below current LTP, if LTP moved up significantly (0.3% from target)
                        # Ensure new_sell_price respects tick size
                        if current_ltp > trade["sell_price_target"] * 1.003: 
                            new_sell_price = round(current_ltp * 0.998 / 0.05) * 0.05 # Target 0.2% below new LTP
                            if new_sell_price > trade["sell_price_target"]:
                                try:
                                    kite.modify_order(
                                        variety="regular",
                                        order_id=trade["sell_order_id"],
                                        price=new_sell_price
                                    )
                                    trade["sell_price_target"] = new_sell_price
                                    logger.info(f"🔼 Trailing sell order for {trade['symbol']} to ₹{new_sell_price:.2f}")
                                except Exception as e:
                                    logger.error(f"⚠️ Error modifying sell order for {trade['symbol']}: {e}")
            
            # Timeout logic for pending trades
            if order_age > MAX_ORDER_TIME and not trade["exit_attempted"]:
                logger.info(f"⏳ Trade Timeout ({MAX_ORDER_TIME}s) for {trade['symbol']}. Initiating exit.")
                trade["exit_attempted"] = True

                # Cancel any pending orders for this trade
                if not trade["buy_completed"]:
                    try:
                        kite.cancel_order(variety="regular", order_id=trade["buy_order_id"])
                        logger.info(f"Cancelled pending buy order {trade['buy_order_id']} for {trade['symbol']}")
                    except Exception as e:
                        logger.warning(f"Could not cancel buy order {trade['buy_order_id']}: {e}")

                if trade["buy_completed"] and not trade["sell_completed"] and trade["sell_order_id"]:
                    try:
                        kite.cancel_order(variety="regular", order_id=trade["sell_order_id"])
                        logger.info(f"Cancelled pending sell order {trade['sell_order_id']} for {trade['symbol']}")
                    except Exception as e:
                        logger.warning(f"Could not cancel sell order {trade['sell_order_id']}: {e}")
                    
                # Force exit any open position (market sell)
                try:
                    positions_data = kite.positions()
                    if positions_data and 'net' in positions_data:
                        current_position_qty = 0
                        for pos in positions_data["net"]:
                            if pos["tradingsymbol"] == trade["symbol"] and pos["quantity"] != 0:
                                current_position_qty = pos["quantity"]
                                break
                        
                        if current_position_qty != 0:
                            transaction_type = kite.TRANSACTION_TYPE_SELL if current_position_qty > 0 else kite.TRANSACTION_TYPE_BUY
                            logger.warning(f"Forcing market exit for {trade['symbol']} {abs(current_position_qty)} @ MARKET (Timeout exit)")
                            kite.place_order(
                                variety=kite.VARIETY_REGULAR,
                                exchange=kite.EXCHANGE_NSE,
                                tradingsymbol=trade["symbol"],
                                transaction_type=transaction_type,
                                quantity=abs(current_position_qty),
                                product=kite.PRODUCT_CNC,
                                order_type=kite.ORDER_TYPE_MARKET,
                                tag="SCALPER_FORCE_EXIT"
                            )
                            # Do not delete from pending_trades immediately. Wait for the forced exit to complete.
                            # It will be handled in the next iteration or during graceful shutdown.
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
                # Keep only relevant history for oscillation analysis (e.g., 2 times oscillation period)
                if len(price_history) > OSCILLATION_PERIOD * 2: 
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
                    df.at[idx, 'price_history'] = hist_data[-OSCILLATION_PERIOD*2:]
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
                        if len(price_history) >= OSCILLATION_PERIOD and MIN_PRICE <= ltp <= MAX_PRICE:
                            try:
                                # Check if this symbol is already part of an active trade
                                if any(trade['symbol'] == symbol for trade in pending_trades.values()):
                                    logger.debug(f"Skipping {symbol}, already in a pending trade.")
                                    continue

                                if is_volatile_and_oscillating(price_history) and find_entry_point(price_history):
                                    logger.info(f"💡 Found opportunity in {symbol} (LTP: {ltp})")
                                    if place_scalp_order(symbol, token, ltp):
                                        # Order placed, immediately check for another opportunity if MAX_ACTIVE_TRADES allows
                                        pass
                                    
                            except Exception as e:
                                logger.error(f"Error evaluating {symbol}: {str(e)}")
            else:
                logger.info(f"Reached max active trades ({MAX_ACTIVE_TRADES}). Waiting for trades to complete.")

            current_active_symbols = ", ".join({trade['symbol'] for trade in pending_trades.values()}) if pending_trades else "None"
            logger.info(f"Status: Trades {trades_completed} | Profit ₹{total_profit:.2f} | Active Trades: {len(pending_trades)} ({current_active_symbols})")
            
            time.sleep(1) # Reduced sleep time to allow faster checks

        except Exception as e:
            logger.error(f"An unhandled error occurred in main loop: {e}")
            time.sleep(RECONNECT_DELAY)

if __name__ == "__main__":
    try:
        logger.info("Starting Scalping Bot")
        run_bot()
    except KeyboardInterrupt:
        logger.info("Shutting down bot. Attempting to close WebSocket and pending positions.")
        # Attempt to exit all open positions gracefully
        try:
            if kws and kws.is_connected():
                kws.close()
                logger.info("WebSocket closed.")
            else:
                logger.info("WebSocket was not connected, no need to close.")
        except Exception as e:
            logger.error(f"Error closing WebSocket: {e}")

        logger.info("Checking for any open positions to exit...")
        try:
            positions = kite.positions()
            if positions and 'net' in positions:
                for pos in positions["net"]:
                    if pos["quantity"] != 0:
                        transaction_type = kite.TRANSACTION_TYPE_SELL if pos["quantity"] > 0 else kite.TRANSACTION_TYPE_BUY
                        logger.info(f"Exiting open position for {pos['tradingsymbol']} (Qty: {pos['quantity']})")
                        kite.place_order(
                            variety=kite.VARIETY_REGULAR,
                            exchange=kite.EXCHANGE_NSE,
                            tradingsymbol=pos["tradingsymbol"],
                            transaction_type=transaction_type,
                            quantity=abs(pos["quantity"]),
                            product=kite.PRODUCT_CNC,
                            order_type=kite.ORDER_TYPE_MARKET,
                            tag="SHUTDOWN_EXIT"
                        )
                        time.sleep(1) # Give a moment for order to process
            else:
                logger.info("No open positions found or failed to retrieve positions.")
        except Exception as e:
            logger.error(f"Error while exiting positions on shutdown: {e}")
        
        logger.info("Bot shutdown complete.")