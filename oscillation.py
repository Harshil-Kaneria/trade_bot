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
        logging.FileHandler('scalper.log', encoding='utf-8') # Added encoding
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
    # --- DEBUGGING: Print available methods of kite object ---
    # logger.info(f"KiteApp object methods: {dir(kite)}")
    # --- END DEBUGGING ---
except Exception as e:
    logger.critical(f"Failed to initialize KiteApp: {e}. Check ENCTOKEN or network connection.")
    exit()

# Configuration
CAPITAL = 100000
TARGET_PROFIT_PER_TRADE = 180 # Adjusted for more trades
MAX_ORDER_TIME = 60 # Reduced to 1 minute for faster exits
RISK_PER_TRADE = 0.01 # Keep as percentage of capital
OSCILLATION_PERIOD = 20
WS_SUBSCRIBE_TIME = 15 # Shorter subscription rotation
MAX_CONCURRENT_SUBS = 10 # More concurrent subscriptions
RECONNECT_DELAY = 5 # seconds between reconnect attempts
MIN_PRICE = 100 # Minimum stock price to consider
MAX_PRICE = 3000 # Maximum stock price to consider
MAX_ACTIVE_TRADES = 1 # Limit concurrent active (pending) trades

# Trackers
pending_trades = {} # Dictionary to store active orders: {order_id: trade_info}
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

df = pd.DataFrame() # Initialize df outside try-except
try:
    instruments = pd.DataFrame(kite.instruments("NSE"))
    df = instruments[instruments["tradingsymbol"].isin(symbols_list)]
    df = df[df["segment"] != "INDICES"]
    df = df[['instrument_token', 'tradingsymbol', 'last_price']].reset_index(drop=True)
    if df.empty:
        logger.critical("No valid symbols found after filtering instruments. Check data_500.csv and NSE instruments.")
        exit()
except Exception as e:
    logger.critical(f"Error loading instruments: {e}. Ensure API is working and internet connection.")
    exit()

# Initialize price tracking
df['last_price'] = 0.0
df['price_history'] = df['tradingsymbol'].apply(lambda x: [])
token_to_index = {token: idx for idx, token in enumerate(df['instrument_token'])}
tokens = df['instrument_token'].tolist() # All available tokens
current_subscriptions = []


# --- Helper Functions ---

def get_price_history(symbol, instrument_token, minutes=30):
    """Fetches historical data for a symbol."""
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
        logger.error(f"Error fetching history for {symbol} ({instrument_token}): {e}")
    return []

def calculate_quantity(price):
    """Calculates quantity based on risk per trade and price."""
    if price <= 0:
        return 0
    
    # Calculate quantity to achieve TARGET_PROFIT_PER_TRADE with a small price move
    # Assuming we target a 0.2% profit per share (as implied by target sell price)
    target_profit_per_share = price * 0.002
    if target_profit_per_share <= 0: # Avoid division by zero or negative
        return 0
    
    quantity = int(TARGET_PROFIT_PER_TRADE / target_profit_per_share)
    
    # Ensure quantity is at least 1 and within capital limits
    quantity = max(1, quantity)
    quantity = min(quantity, int(CAPITAL / price)) # Max quantity affordable
    
    return quantity


def is_oscillating(prices):
    """
    Determines if prices are oscillating around a mean.
    Improved logic for better detection.
    """
    if len(prices) < OSCILLATION_PERIOD:
        return False
    
    recent_prices = np.array(prices[-OSCILLATION_PERIOD:])
    
    # Calculate simple moving average
    sma = np.mean(recent_prices)
    
    # Calculate standard deviation
    std_dev = np.std(recent_prices)
    
    # Check for low volatility relative to price and mean reversion
    if sma == 0: return False # Avoid division by zero
    
    # If standard deviation is too high, it's trending or highly volatile, not oscillating
    if std_dev / sma > 0.008: # Adjusted sensitivity, lower means less volatile
        return False
        
    # Check for price staying within a band around the SMA
    upper_band = sma * (1 + 0.005) # 0.5% above SMA
    lower_band = sma * (1 - 0.005) # 0.5% below SMA
    
    # Ensure most recent prices are within this band
    if not (lower_band <= recent_prices[-1] <= upper_band):
        return False
        
    # Check for mean reversion pattern (price crossing SMA)
    crosses = 0
    for i in range(1, len(recent_prices)):
        if (recent_prices[i-1] < sma and recent_prices[i] > sma) or \
           (recent_prices[i-1] > sma and recent_prices[i] < sma):
            crosses += 1
            
    # For a 20-period oscillation, expect at least a few crosses (e.g., > 2)
    if crosses < 2:
        return False
            
    return True


def find_entry_point(prices):
    """
    Identifies a potential buy entry point in an oscillating market.
    Looks for price dipping below a short-term moving average within an oscillating context.
    """
    if len(prices) < 20: # Need enough data for MAs
        return False
    
    current_price = prices[-1]
    
    # Short-term SMA (e.g., 5 periods)
    sma_short = np.mean(prices[-5:])
    
    # Long-term SMA (e.g., 20 periods)
    sma_long = np.mean(prices[-20:])
    
    # Condition 1: Price is below the short-term SMA (potential dip)
    if current_price >= sma_short:
        return False
        
    # Condition 2: Short-term SMA is not significantly above long-term SMA
    # This suggests a non-trending or slightly bearish short-term dip in a generally flat/oscillating trend
    if sma_short > sma_long * 1.002:
        return False
        
    # Condition 3: Price is near the lower band of its recent oscillation (using std dev from SMA_long)
    std_dev_long = np.std(prices[-20:])
    lower_bound = sma_long - 1.5 * std_dev_long # 1.5 standard deviations below long SMA
    
    if current_price < lower_bound:
        # Price is too low, might be breaking down. Avoid catching falling knives.
        return False
        
    # Condition 4: The current price is a recent low within the oscillation period (but not extreme)
    # Check if current price is lower than previous n bars within the oscillation period.
    # This implies a recent dip.
    if current_price < np.min(prices[-OSCILLATION_PERIOD:-1]):
        return True # It's a recent low point, good entry

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
        sell_price_target = round(current_ltp * 1.002, 2)
        
        logger.info(f"📈 Placing Buy Order: {symbol} | Qty: {quantity} @ MARKET | Target Sell: ₹{sell_price_target}")

        # Place buy order (Market Order for instant execution)
        buy_order_response = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=symbol,
            transaction_type=kite.TRANSACTION_TYPE_BUY,
            quantity=quantity,
            product=kite.PRODUCT_CNC, # Using CNC for simpler position management
            order_type=kite.ORDER_TYPE_MARKET, # Changed to MARKET
            tag="SCALPER_BUY"
        )
        buy_order_id = buy_order_response.get('order_id') # Get order_id from response

        if not buy_order_id:
            logger.error(f"Failed to get order_id for buy order of {symbol}. Response: {buy_order_response}")
            return False

        logger.info(f"Buy Order placed for {symbol}: {buy_order_id}")

        # Store pending trade information
        trade_info = {
            "symbol": symbol,
            "instrument_token": instrument_token,
            "buy_order_id": buy_order_id,
            "sell_order_id": None, # Will be set after buy is complete
            "quantity": quantity,
            "buy_price": None, # Will be set after buy is complete (actual execution price)
            "sell_price_target": sell_price_target,
            "order_time": datetime.now(),
            "buy_completed": False,
            "sell_completed": False,
            "exit_attempted": False # To prevent multiple exit attempts
        }
        pending_trades[buy_order_id] = trade_info
        
        return True

    except Exception as e:
        logger.error(f"❌ Order Placement Failed for {symbol}: {str(e)}")
        return False

def get_order_details_from_history(order_id):
    """Helper to fetch details of a specific order from full order history."""
    try:
        # kite.orders() returns all orders, then filter by order_id
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

    # Create a list of order IDs to iterate over safely, as pending_trades might change
    orders_to_check = list(pending_trades.keys())

    for order_id in orders_to_check:
        trade = pending_trades.get(order_id)
        if not trade:
            continue # Should not happen, but a safeguard

        current_time = datetime.now()
        order_age = (current_time - trade["order_time"]).total_seconds()

        try:
            # Check buy order status
            if not trade["buy_completed"]:
                # Use kite.orders() to get all orders, then filter by order_id
                buy_order_status = get_order_details_from_history(trade["buy_order_id"])
                
                if buy_order_status and buy_order_status["status"] == "COMPLETE":
                    trade["buy_completed"] = True
                    trade["buy_price"] = buy_order_status["average_price"]
                    logger.info(f"✅ Buy Order COMPLETE for {trade['symbol']} @ ₹{trade['buy_price']:.2f}")

                    # Immediately place the sell limit order after buy is complete
                    if trade["sell_order_id"] is None:
                        # Ensure sell price is higher than buy price for profit
                        if trade["sell_price_target"] <= trade["buy_price"]:
                             # Adjust sell target if it's not profitable
                            trade["sell_price_target"] = round(trade["buy_price"] * 1.002, 2)
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
                        sell_order_id = sell_order_response.get('order_id')
                        if sell_order_id:
                            trade["sell_order_id"] = sell_order_id
                            logger.info(f"Placed Sell Order for {trade['symbol']}: {sell_order_id} @ ₹{trade['sell_price_target']:.2f}")
                        else:
                            logger.error(f"Failed to get order_id for sell order of {trade['symbol']}. Response: {sell_order_response}")
                            # Potentially handle this by force exiting later if stuck
                elif buy_order_status and buy_order_status["status"] in ["CANCELLED", "REJECTED"]:
                    logger.warning(f"Buy Order for {trade['symbol']} {buy_order_status['status']}. Removing from pending trades.")
                    del pending_trades[order_id]
                    continue # Move to next trade

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
                    del pending_trades[order_id] # Remove completed trade
                    continue # Move to next trade

                # Trailing logic for sell order (if buy completed and sell pending)
                with price_lock:
                    idx = token_to_index.get(trade["instrument_token"])
                    if idx is not None:
                        current_ltp = df.at[idx, 'last_price']
                        if current_ltp == 0.0: # Skip if no recent price update
                            continue

                        # Trailing sell price up if price moves favorably
                        # If current_ltp is significantly higher than our sell target, trail it up
                        # Example: if current_ltp is 0.5% above our current sell_price_target
                        if current_ltp > trade["sell_price_target"] * 1.005: # Trailing by 0.5%
                            new_sell_price = round(current_ltp * 0.998, 2) # Target 0.2% below new LTP
                            # Ensure new_sell_price is still profitable and higher than old target
                            if new_sell_price > trade["sell_price_target"]:
                                try:
                                    kite.modify_order(
                                        variety="regular",
                                        order_id=trade["sell_order_id"],
                                        price=new_sell_price
                                    )
                                    trade["sell_price_target"] = new_sell_price # Update target price
                                    logger.info(f"🔼 Trailing sell order for {trade['symbol']} to ₹{new_sell_price:.2f}")
                                except Exception as e:
                                    logger.error(f"⚠️ Error modifying sell order for {trade['symbol']}: {e}")
            
            # Timeout logic for pending trades
            if order_age > MAX_ORDER_TIME and not trade["exit_attempted"]:
                logger.info(f"⏳ Trade Timeout ({MAX_ORDER_TIME}s) for {trade['symbol']}. Initiating exit.")
                trade["exit_attempted"] = True # Mark as attempted to prevent re-entry

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
                # It's better to check positions *after* attempting to cancel orders
                try:
                    positions_data = kite.positions()
                    # Ensure positions_data is not None and has 'net' key
                    if positions_data and 'net' in positions_data:
                        for pos in positions_data["net"]:
                            if pos["tradingsymbol"] == trade["symbol"] and pos["quantity"] != 0:
                                # Ensure we sell the correct quantity and direction
                                transaction_type = kite.TRANSACTION_TYPE_SELL if pos["quantity"] > 0 else kite.TRANSACTION_TYPE_BUY
                                logger.warning(f"Forcing market exit for {trade['symbol']} {abs(pos['quantity'])} @ MARKET (Timeout exit)")
                                kite.place_order(
                                    variety=kite.VARIETY_REGULAR,
                                    exchange=kite.EXCHANGE_NSE,
                                    tradingsymbol=trade["symbol"],
                                    transaction_type=transaction_type,
                                    quantity=abs(pos["quantity"]),
                                    product=kite.PRODUCT_CNC,
                                    order_type=kite.ORDER_TYPE_MARKET, # Market order for quick exit
                                    tag="SCALPER_FORCE_EXIT"
                                )
                                break
                        # If no open position found for this symbol, or if it was exited, remove from pending
                        if not any(p["tradingsymbol"] == trade["symbol"] and p["quantity"] != 0 for p in positions_data["net"]):
                            del pending_trades[order_id]
                            logger.info(f"Trade for {trade['symbol']} removed after timeout and exit attempt (no open position).")
                    else:
                        logger.warning(f"Could not retrieve positions for timeout check of {trade['symbol']}. Assuming no open position.")
                        del pending_trades[order_id] # Assume it's gone if we can't verify
                        
                except Exception as e:
                    logger.error(f"⚠️ Error forcing exit for {trade['symbol']} on timeout: {e}")
                    # If an error occurs during exit, keep it in pending_trades for re-attempt
                    # Or implement a retry mechanism. For now, we'll just log and continue.


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
            if idx is not None and price > 0: # Ensure price is valid
                df.at[idx, 'last_price'] = price
                
                # Update price history, ensuring it's for the correct token
                price_history = df.at[idx, 'price_history']
                price_history.append(price)
                if len(price_history) > OSCILLATION_PERIOD * 2: # Keep more history for analysis
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
    # Don't call reconnect_websocket here directly, let the main loop handle it.

def on_close(ws, code, reason):
    """Callback for WebSocket close events."""
    global ws_connected
    ws_connected = False
    logger.info(f"WebSocket Closed: {code} - {reason}")
    # Don't call reconnect_websocket here directly, let the main loop handle it.

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
        
        # Connect in a non-blocking way
        websocket_thread = Thread(target=kws.connect, daemon=True)
        websocket_thread.start()
        
        # Give it a moment to connect
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
            # Unsubscribe from old tokens
            if current_subscriptions:
                logger.debug(f"Unsubscribing from {len(current_subscriptions)} tokens.")
                kws.unsubscribe(current_subscriptions)
            
            # Subscribe to new tokens
            if new_tokens:
                logger.debug(f"Subscribing to {len(new_tokens)} tokens.")
                kws.subscribe(new_tokens)
                kws.set_mode(kws.MODE_QUOTE, new_tokens)
            
            current_subscriptions = new_tokens
            logger.info(f"WebSocket subscriptions updated to {len(new_tokens)} tokens.")
        else:
            logger.debug("No change in subscriptions needed.")
    except Exception as e:
        logger.error(f"Subscription update error: {e}")

def rotate_symbol_subscription():
    """Selects a new batch of symbols to subscribe to."""
    if df.empty:
        logger.error("No symbols available for subscription.")
        return []
    
    # If there are active trades, only subscribe to those symbols to ensure price updates
    if pending_trades:
        active_trade_tokens = {trade["instrument_token"] for trade in pending_trades.values()}
        # Also ensure we're still monitoring a few other symbols for new opportunities
        additional_tokens_needed = MAX_CONCURRENT_SUBS - len(active_trade_tokens)
        
        if additional_tokens_needed > 0:
            # Filter out already active trade tokens from the general list
            available_for_sampling = df[~df['instrument_token'].isin(active_trade_tokens)]
            if not available_for_sampling.empty:
                batch = available_for_sampling.sample(min(additional_tokens_needed, len(available_for_sampling)))
                new_tokens = list(active_trade_tokens) + batch['instrument_token'].tolist()
            else:
                new_tokens = list(active_trade_tokens)
        else:
            new_tokens = list(active_trade_tokens)
        logger.debug(f"Monitoring active trades and {len(new_tokens) - len(active_trade_tokens)} other symbols.")
    else:
        # If no active trades, select a new random batch
        batch_size = min(MAX_CONCURRENT_SUBS, len(df))
        batch = df.sample(batch_size)
        new_tokens = batch['instrument_token'].tolist()
        logger.debug(f"Monitoring {len(new_tokens)} random symbols.")

    # Initialize price history for new symbols if not already present
    with price_lock:
        for token in new_tokens:
            idx = token_to_index.get(token)
            if idx is not None and not df.at[idx, 'price_history']:
                symbol_name = df.at[idx, 'tradingsymbol']
                hist_data = get_price_history(symbol_name, token)
                if hist_data:
                    df.at[idx, 'price_history'] = hist_data[-OSCILLATION_PERIOD*2:]
                    logger.debug(f"Loaded initial history for {symbol_name}.")
    
    return new_tokens

def run_bot():
    """Main trading bot loop."""
    initialize_websocket()
    
    last_rotation = datetime.now()
    
    # Initial price history load for all available symbols (can be slow for many symbols)
    logger.info("Loading initial price history for all symbols (this might take a while)...")
    if not df.empty:
        for idx, row in df.iterrows():
            if not df.at[idx, 'price_history']:
                hist_data = get_price_history(row['tradingsymbol'], row['instrument_token'])
                if hist_data:
                    df.at[idx, 'price_history'] = hist_data[-OSCILLATION_PERIOD*2:]
                if idx % 100 == 0 and idx > 0:
                    logger.info(f"Loaded history for {idx+1}/{len(df)} symbols.")
    logger.info("Initial price history loaded.")

    logger.info("Starting trading loop...")
    
    while True:
        try:
            if not ws_connected or (kws and not kws.is_connected()):
                logger.warning("WebSocket not connected. Attempting reconnection...")
                time.sleep(RECONNECT_DELAY)
                initialize_websocket() # Attempt to re-initialize if disconnected
                continue # Skip current loop iteration until connected

            # Rotate subscriptions periodically
            if (datetime.now() - last_rotation).total_seconds() > WS_SUBSCRIBE_TIME:
                new_tokens = rotate_symbol_subscription()
                if new_tokens:
                    update_subscriptions(new_tokens)
                    symbols_monitoring = df[df['instrument_token'].isin(new_tokens)]['tradingsymbol'].tolist()
                    logger.info(f"Currently monitoring: {', '.join(symbols_monitoring)}")
                last_rotation = datetime.now()
            
            # Manage existing orders
            manage_orders()
            
            # Look for new opportunities if we haven't reached MAX_ACTIVE_TRADES
            if len(pending_trades) < MAX_ACTIVE_TRADES:
                with price_lock:
                    # Iterate through currently subscribed symbols to find opportunities
                    for token in current_subscriptions:
                        idx = token_to_index.get(token)
                        if idx is None:
                            continue

                        symbol = df.at[idx, 'tradingsymbol']
                        price_history = df.at[idx, 'price_history']
                        ltp = df.at[idx, 'last_price']
                        
                        # Only consider if we have enough price history and price is within range
                        if len(price_history) >= OSCILLATION_PERIOD and MIN_PRICE <= ltp <= MAX_PRICE:
                            try:
                                # Check if this symbol is already part of an active trade
                                if any(trade['symbol'] == symbol for trade in pending_trades.values()):
                                    logger.debug(f"Skipping {symbol}, already in a pending trade.")
                                    continue

                                if is_oscillating(price_history) and find_entry_point(price_history):
                                    logger.info(f"💡 Found opportunity in {symbol} (LTP: {ltp})")
                                    if place_scalp_order(symbol, token, ltp):
                                        # Order placed, immediately check for another opportunity
                                        pass # Don't break, keep scanning for other opportunities
                                    
                            except Exception as e:
                                logger.error(f"Error evaluating {symbol}: {str(e)}")
                                # continue to next symbol
            else:
                logger.info(f"Reached max active trades ({MAX_ACTIVE_TRADES}). Waiting for trades to complete.")

            current_active_symbols = ", ".join({trade['symbol'] for trade in pending_trades.values()}) if pending_trades else "None"
            logger.info(f"Status: Trades {trades_completed} | Profit ₹{total_profit:.2f} | Active Trades: {len(pending_trades)} ({current_active_symbols})")
            
            # Short sleep to prevent busy-waiting, but not too long
            time.sleep(1) # Reduced sleep time significantly

        except Exception as e:
            logger.error(f"An unhandled error occurred in main loop: {e}")
            time.sleep(RECONNECT_DELAY) # Sleep before retrying loop

if __name__ == "__main__":
    try:
        logger.info("Starting Scalping Bot")
        run_bot()
    except KeyboardInterrupt:
        logger.info("Shutting down bot. Attempting to close WebSocket and pending positions.")
        if kws: # Ensure kws exists before trying to close
            if kws.is_connected():
                kws.close()
                logger.info("WebSocket closed.")
            else:
                logger.info("WebSocket not connected, no need to close.")
        
        # Attempt to exit any open positions gracefully
        logger.info("Checking for any open positions to exit...")
        try:
            positions = kite.positions() # Fetch current positions
            if positions and 'net' in positions:
                for pos in positions["net"]:
                    if pos["quantity"] != 0:
                        logger.info(f"Exiting open position for {pos['tradingsymbol']} (Qty: {pos['quantity']})")
                        transaction_type = kite.TRANSACTION_TYPE_SELL if pos["quantity"] > 0 else kite.TRANSACTION_TYPE_BUY
                        kite.place_order(
                            variety=kite.VARIETY_REGULAR,
                            exchange=kite.EXCHANGE_NSE,
                            tradingsymbol=pos["tradingsymbol"],
                            transaction_type=transaction_type,
                            quantity=abs(pos["quantity"]),
                            product=kite.PRODUCT_CNC,
                            order_type=kite.ORDER_TYPE_MARKET, # Market order for quick exit on shutdown
                            tag="SCALPER_SHUTDOWN_EXIT"
                        )
                        time.sleep(0.5) # Small delay between exits
            else:
                logger.info("No open positions found or failed to retrieve positions.")
        except Exception as e:
            logger.error(f"Error while exiting positions on shutdown: {e}")
        
        logger.info("Bot shutdown complete.")
    except Exception as e:
        logger.critical(f"Fatal error during bot execution: {e}", exc_info=True)
        if kws:
            kws.close()