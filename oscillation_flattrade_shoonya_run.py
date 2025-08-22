import os
import argparse
import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from api_helper_flattrade import NorenApiPy
from api_helper_shoonya import ShoonyaApiPy
from dotenv import load_dotenv
import logging
from threading import Lock, Thread
from collections import deque
from queue import Queue
import pyotp

parser = argparse.ArgumentParser()
parser.add_argument("--broker", type=str, default="F", help="Value for broker is (SHOONYA/FLATTRADE) so S/F")
args = parser.parse_args()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('oscillation_scalper.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Load credentials
load_dotenv(override=True)

api = None
apiSocket = None
broker = args.broker

if broker == 'F':
    # Initialize Flattrade API
    api_key = os.getenv("FLATTRADE_API_KEY")
    api_secret = os.getenv("FLATTRADE_SECRET_KEY")
    user_id = os.getenv("FLATTRADE_USER_ID")
    user_token = os.getenv("FLATTRADE_SESSION")
    api = NorenApiPy()
    ret = api.set_session(userid=user_id, password='', usertoken=user_token)
    if ret is None:
        logger.error("Failed to set session. Check credentials.")
        exit()
        
    apiSocket = api
    print("FLATTRADE")

else:
    # Initialize Shoonya API
    uid = os.getenv("SHOONYA_UID")
    pwd = os.getenv("SHOONYA_PASSWORD")
    factor2 = pyotp.TOTP(os.getenv("SHOONYA_TOTP")).now()
    vc = os.getenv("SHOONYA_VENDOR_CODE")
    app_key = os.getenv("SHOONYA_API_KEY")
    imei = os.getenv("SHOONYA_IMEI")
    api = ShoonyaApiPy()
    ret = api.login(userid=uid, password=pwd, twoFA=factor2, vendor_code=vc, api_secret=app_key, imei=imei)
    if ret is None:
        logger.error("Failed to set session. Check credentials.")
        exit()
    apiSocket = api
    print("SHOONYA")

# Trading Configuration (adapted from Zerodha with Shoonya values where applicable)
CAPITAL = 90000  # ₹90,000
TARGET_PROFIT_PER_TRADE = 180  # ₹180 per trade
MAX_LOSS_PER_TRADE = 150
MAX_POSITION_DURATION = 300  # 5 minutes in seconds
AUTO_CLOSE_ENABLED = True
VOLATILITY_THRESHOLD = 0.0005  # 0.05% for small price movements
MAX_CONCURRENT_SUBS = 500  # Assuming higher limit for Shoonya/Flattrade; adjust if needed
MIN_PRICE = 100  # Minimum stock price
MAX_PRICE = 3000  # Maximum stock price
MAX_ACTIVE_TRADES = 1  # Limit concurrent trades
SHORT_EMA_PERIOD = 5  # Short EMA for trend detection
LONG_EMA_PERIOD = 20  # Long EMA for trend detection
TREND_STRENGTH_THRESHOLD = 0.0005  # Minimum EMA difference for trend
OSCILLATION_PERIOD = 10  # Short period for scalping
HISTORY_WINDOW = max(OSCILLATION_PERIOD, LONG_EMA_PERIOD) * 2  # For price 
TICK_SIZE = 0.10

# Global trackers
active_position = False
position_lock = Lock()
processing_data = True
trade_signal_queue = Queue()
price_history = {}  # {token: deque of prices}
pending_trades = {}  # Track pending trades like in Zerodha

# Prepare symbol list and instrument info
symbols_list = pd.read_csv('data_500.csv')['SYMBOL'].tolist()
instruments = pd.read_csv('NSE_Equity_Master.csv')

# Filter instruments
df = instruments[instruments['Symbol'].isin(symbols_list)]
df = df[df['Instrument'] == "EQ"]  # Only Equity instruments
df = df[['Token', 'Symbol']].rename(columns={
    'Token': 'instrument_token',
    'Symbol': 'tradingsymbol'
}).reset_index(drop=True)

# Initialize data structures
for token in df['instrument_token']:
    price_history[str(token)] = deque(maxlen=HISTORY_WINDOW)

token_to_index = {str(token): idx for idx, token in enumerate(df['instrument_token'])}
tokens = df['instrument_token'].astype(str).tolist()

def round_to_tick(value, tick=TICK_SIZE):
    try:
        return round(round(float(value) / float(tick)) * float(tick), 2)
    except Exception:
        return round(value, 2)

def get_price_history(token, minutes=30):
    """Fetches historical data for a token (minute candles for initial fill)."""
    try:
        endtime = int(time.time())
        starttime = endtime - minutes * 60
        hist_data = api.get_time_price_series(exchange='NSE', token=token, starttime=starttime, endtime=endtime, interval=1)
        if hist_data:
            return [float(candle['intc']) for candle in hist_data]  # 'intc' is close price
    except Exception as e:
        logger.error(f"Error fetching historical minute data for token {token}: {e}")
    return []

def get_todays_high_low(token):
    """Return today's high/low via broker quote API (Shoonya/Flattrade compatible).
    Tries common key variations and optional nested 'data'.
    """
    try:
        quote = api.get_quotes(exchange='NSE', token=str(token))
        if not quote:
            return None, None
        # Sometimes wrapped in {'stat': 'Ok', 'data': {...}}
        if isinstance(quote, dict) and quote.get('data') and isinstance(quote.get('data'), dict):
            quote = quote.get('data')
        # Accept multiple possible key names
        high_keys = ['h', 'high', 'day_high', 'dayHigh', 'ih']
        low_keys = ['l', 'low', 'day_low', 'dayLow', 'il']
        day_high = None
        day_low = None
        for k in high_keys:
            if k in quote and quote.get(k) not in (None, ''):
                day_high = float(quote.get(k))
                break
        for k in low_keys:
            if k in quote and quote.get(k) not in (None, ''):
                day_low = float(quote.get(k))
                break
        return day_high, day_low
    except Exception as e:
        logger.warning(f"Failed to fetch quote high/low for token {token}: {e}")
        return None, None

def calculate_ema(prices, period):
    """Calculates Exponential Moving Average."""
    if len(prices) < period:
        return None
    ema = pd.Series(prices).ewm(span=period, adjust=False).mean().iloc[-1]
    return ema

def determine_trend(prices):
    if len(prices) < HISTORY_WINDOW:
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

def calculate_quantity(price):
    """Calculates quantity based on target profit and price."""
    if price <= 0:
        return 0
    
    # Use full capital to determine max affordable quantity
    quantity = int(CAPITAL // price)
    quantity = max(1, quantity)
    
    return quantity

def on_ticks(tick_data):
    """Process incoming ticks and update price history"""
    try:
        if isinstance(tick_data, dict):
            if broker == 'F':
                # Flattrade tick format
                token = tick_data.get('tk')
                price = float(tick_data.get('lp', 0))
            else:
                # Shoonya tick format
                token = tick_data.get('tk')
                price = float(tick_data.get('lp', 0))
            
            if token in price_history:
                price_history[token].append(price)
    except Exception as e:
        logger.error(f"Error processing tick: {e}")

def on_connect():
    """WebSocket connected callback"""
    logger.info("✅ WebSocket Connected")
    try:
        if broker == 'F':
            # Flattrade subscription format
            subscription_tokens = tokens
            apiSocket.subscribe(subscription_tokens)
        else:
            # Shoonya subscription format
            subscription_tokens = [f"NSE|{token}" for token in tokens]
            apiSocket.subscribe(subscription_tokens)
        logger.info(f"Subscribed to {len(tokens)} instruments")
    except Exception as e:
        logger.error(f"Subscription failed: {e}")

def on_close():
    """WebSocket closed callback"""
    logger.info("❌ WebSocket Closed")

def on_error(error):
    """WebSocket error callback"""
    logger.error(f"WebSocket Error: {error}")

def find_oscillation_opportunities():
    """Identify stocks with optimal oscillation patterns"""
    opportunities = []
    
    for token in price_history:
        prices = list(price_history.get(token, []))
        if len(prices) < HISTORY_WINDOW:
            # Fetch historical if needed
            hist_prices = get_price_history(token)
            if hist_prices:
                price_history[token].extend(hist_prices[-HISTORY_WINDOW:])
            prices = list(price_history[token])
            if len(prices) < HISTORY_WINDOW:
                continue
        
        score = calculate_oscillation_score(prices)
        if score > 0 and is_volatile_and_oscillating(prices):
            idx = token_to_index.get(token)
            if idx is not None:
                symbol = df.at[idx, 'tradingsymbol']
                current_price = prices[-1]
                if MIN_PRICE <= current_price <= MAX_PRICE:
                    opportunities.append({
                        'symbol': symbol,
                        'token': token,
                        'current_price': current_price,
                        'score': score,
                        'prices': prices
                    })
    
    # Sort by oscillation score descending
    opportunities.sort(key=lambda x: x['score'], reverse=True)
    return opportunities

def determine_trade_parameters(opportunity):
    """Calculate trade parameters for an oscillation opportunity"""
    prices = opportunity['prices']
    price = opportunity['current_price']
    trend = determine_trend(prices)
    direction = None
    target = None

    if trend == 'positive' and find_entry_point_long(prices):
        direction = 'BUY'
    elif trend == 'negative' and find_entry_point_short(prices):
        direction = 'SELL'
    elif trend == 'neutral' and find_entry_point_long(prices):
        direction = 'BUY'

    if direction is None:
        return None

    quantity = calculate_quantity(price)
    if quantity == 0:
        return None

    return {
        'symbol': opportunity['symbol'],
        'token': opportunity['token'],
        'direction': direction,
        'price': price,
        'quantity': quantity,
        'score': opportunity['score']
    }

def stock_picker():
    """Continuously scan for oscillation-based opportunities"""
    global active_position
    
    while processing_data:
        try:
            with position_lock:
                if active_position:
                    time.sleep(1)
                    continue
            
            # Find best oscillation opportunities
            opportunities = find_oscillation_opportunities()
            if not opportunities:
                time.sleep(1)
                continue
            
            # Check top opportunities for entry
            for opp in opportunities:
                trade_params = determine_trade_parameters(opp)
                if trade_params:
                    logger.info(f"Found oscillation opportunity: {trade_params['symbol']} | Score: {trade_params['score']:.4f}")
                    trade_signal_queue.put(trade_params)
                    time.sleep(5)
                    break
            else:
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"Error in stock picker: {e}")
            time.sleep(5)

def get_order_details(order_id):
    """Fetch details of a specific order from order book."""
    try:
        order_book = api.get_order_book()
        for order in order_book:
            if order.get('norenordno') == order_id:
                return order
    except Exception as e:
        logger.error(f"Error fetching order details for {order_id}: {e}")
    return None

def verify_order_pricing(order_id, expected_entry, expected_sl, expected_tp, is_bracket):
    """Verify that the placed order has the expected pricing parameters."""
    try:
        order_details = get_order_details(order_id)
        if not order_details:
            logger.warning(f"Could not fetch order details for verification: {order_id}")
            return False
            
        logger.info(f"🔍 Verifying order {order_id} pricing...")
        logger.info(f"   Expected Entry: {expected_entry:.2f}")
        logger.info(f"   Expected SL: {expected_sl:.2f}")
        logger.info(f"   Expected TP: {expected_tp:.2f}")
        logger.info(f"   Is Bracket: {is_bracket}")
        
        # Log actual order details for comparison
        actual_price = order_details.get('prc', order_details.get('price', 'N/A'))
        actual_trigger = order_details.get('trgprc', order_details.get('trigger_price', 'N/A'))
        actual_status = order_details.get('status', 'N/A')
        actual_product = order_details.get('prd', order_details.get('product_type', 'N/A'))
        
        logger.info(f"   Actual Price: {actual_price}")
        logger.info(f"   Actual Trigger: {actual_trigger}")
        logger.info(f"   Actual Status: {actual_status}")
        logger.info(f"   Actual Product: {actual_product}")
        
        # Check for significant price discrepancies
        if actual_price != 'N/A' and actual_price != '0':
            try:
                actual_price_float = float(actual_price)
                if abs(actual_price_float - expected_entry) > 1.0:  # More than ₹1 difference
                    logger.warning(f"⚠️  Price discrepancy detected! Expected: {expected_entry:.2f}, Actual: {actual_price_float:.2f}")
                    return False
            except ValueError:
                logger.warning(f"Could not parse actual price: {actual_price}")
        
        logger.info(f"✅ Order pricing verification completed")
        return True
        
    except Exception as e:
        logger.error(f"Error in order pricing verification: {e}")
        return False

def get_open_position_qty_for_symbol(symbol):
    """Return current net open quantity for given symbol from positions.
    Positive for long, negative for short, 0 if flat or not found.
    """
    try:
        positions_data = api.get_positions()
    except Exception as e:
        logger.warning(f"Could not fetch positions for {symbol}: {e}")
        return 0

    iterable_positions = []
    if isinstance(positions_data, list):
        iterable_positions = positions_data
    elif isinstance(positions_data, dict):
        if positions_data.get('data') is not None:
            iterable_positions = positions_data.get('data', [])
        elif positions_data.get('net') is not None:
            iterable_positions = positions_data.get('net', [])

    for pos in iterable_positions:
        try:
            tsym = pos.get('tsym') or pos.get('tradingsymbol') or pos.get('symbol')
            base_symbol = tsym.split('-')[0] if tsym else None
            qty_raw = pos.get('netqty', pos.get('qty', 0))
            qty_val = int(qty_raw) if str(qty_raw).strip() != '' else 0
            if base_symbol == symbol:
                return qty_val
        except Exception:
            continue
    return 0

def position_manager():
    """Manage trade execution and monitoring"""
    global active_position, pending_trades
    
    current_trade = None
    entry_time = None
    
    while processing_data:
        try:
            # Check for new signals if no active position
            if not active_position and not trade_signal_queue.empty():
                # Take the next signal and immediately mark active to avoid duplicate placements
                current_trade = trade_signal_queue.get()
                with position_lock:
                    if active_position:
                        # Another thread took it; skip
                        current_trade = None
                        continue
                    active_position = True

                logger.info(f"Executing trade signal for {current_trade['symbol']} | Dir: {current_trade['direction']} | Qty: {current_trade['quantity']}")

                # Compute BO legs: TP and SL per-share derived from total rupee goals
                ltp = None
                if len(price_history.get(current_trade['token'], [])) > 0:
                    ltp = price_history[current_trade['token']][-1]
                entry_ref_price = float(ltp if ltp is not None else current_trade['price'])
                qty_int = max(1, int(current_trade['quantity']))

                # Per-share rupee goals
                tp_per_share = float(TARGET_PROFIT_PER_TRADE) / qty_int
                risk_per_share = float(MAX_LOSS_PER_TRADE) / qty_int

                # Derive target and stop from entry
                if current_trade['direction'] == 'BUY':
                    bo_target = entry_ref_price + tp_per_share
                    stop_trigger = max(0.0, entry_ref_price - risk_per_share)
                else:
                    bo_target = entry_ref_price - tp_per_share
                    stop_trigger = entry_ref_price + risk_per_share

                # Round to tick size (0.10)
                bo_target = round_to_tick(bo_target)
                stop_trigger = round_to_tick(stop_trigger)

                logger.info(
                    f"Entry ~ {entry_ref_price:.2f} | TP {bo_target:.2f} | SL {stop_trigger:.2f} | Qty {qty_int}"
                )

                # Place Market Order for immediate execution
                logger.info(f"📤 Placing Market Order for {current_trade['symbol']}...")
                logger.info(f"   Direction: {current_trade['direction']}")
                logger.info(f"   Quantity: {qty_int}")
                logger.info(f"   Entry Price: {entry_ref_price:.2f}")
                logger.info(f"   Stop Loss: {stop_trigger:.2f}")
                logger.info(f"   Take Profit: {bo_target:.2f}")
                
                order_result, is_bracket = place_bracket_order_with_fallback(
                    symbol=current_trade['symbol'],
                    token=current_trade['token'],
                    direction=current_trade['direction'],
                    quantity=qty_int,
                    entry_price=entry_ref_price,
                    stop_loss=stop_trigger,
                    take_profit=bo_target
                )
                
                if order_result and isinstance(order_result, dict) and order_result.get('stat') == 'Ok':
                    current_trade['order_id'] = order_result.get('norenordno')
                    current_trade['initial_completed'] = False
                    current_trade['exit_attempted'] = False
                    current_trade['is_bracket'] = False  # Always false with new strategy
                    current_trade['bookloss_price'] = stop_trigger
                    current_trade['bookprofit_price'] = bo_target
                    current_trade['sl_tp_placed'] = False  # Track if SL/TP orders are placed
                    entry_time = datetime.now()
                    pending_trades[current_trade['order_id']] = current_trade
                    
                    logger.info(f"✅ Market Order placed successfully: {current_trade['symbol']} | Order ID: {current_trade['order_id']}")
                    logger.info(f"   Note: SL/TP orders will be placed after position is established")
                    
                    # Verify order pricing
                    time.sleep(1)  # Wait a moment for order to be processed
                    verify_order_pricing(current_trade['order_id'], entry_ref_price, stop_trigger, bo_target, False)
                else:
                    logger.error(f"❌ Market order placement failed: {order_result}")
                    current_trade = None
                    with position_lock:
                        active_position = False
                    continue
            
            # Monitor active position
            if active_position and current_trade:
                current_time = datetime.now()
                position_duration = (current_time - entry_time).total_seconds()
                
                # Get current price
                current_price = None
                if len(price_history.get(current_trade['token'], [])) > 0:
                    current_price = price_history[current_trade['token']][-1]
                
                if current_price is None:
                    time.sleep(1)
                    continue
                
                order_id = current_trade['order_id']
                
                if not current_trade['initial_completed']:
                    initial_order_status = get_order_details(order_id)
                    
                    if initial_order_status and initial_order_status.get('status') == 'COMPLETE':
                        # Check for partial fills
                        filled_qty = int(initial_order_status.get('fillshares', initial_order_status.get('filled_qty', 0)))
                        if filled_qty < qty_int:
                            logger.warning(f"⚠️  Partial fill detected: {filled_qty}/{qty_int} for {current_trade['symbol']}")
                            # Handle partial fill
                            if handle_partial_fill(order_id, qty_int, current_trade['symbol'], current_trade['token'], current_trade['direction']):
                                logger.info(f"✅ Partial fill handled, waiting for completion...")
                                continue  # Wait for additional order to complete
                            else:
                                logger.error(f"❌ Failed to handle partial fill for {current_trade['symbol']}")
                                # Continue with partial position
                        
                        current_trade['initial_completed'] = True
                        current_trade['entry_price'] = float(initial_order_status.get('avgprc', current_price))
                        current_trade['actual_quantity'] = filled_qty  # Store actual filled quantity
                        logger.info(f"✅ Initial {current_trade['direction']} Order COMPLETE for {current_trade['symbol']} @ ₹{current_trade['entry_price']:.2f}")
                        logger.info(f"   Filled Quantity: {filled_qty}/{qty_int}")
                        
                        # Now place SL/TP orders since position is established
                        if not current_trade.get('sl_tp_placed', False):
                            logger.info(f"📤 Placing SL/TP orders for completed position...")
                            # Use actual filled quantity for SL/TP orders
                            actual_qty = current_trade.get('actual_quantity', qty_int)
                            sl_tp_success = place_sl_tp_orders(
                                symbol=current_trade['symbol'],
                                token=current_trade['token'],
                                direction=current_trade['direction'],
                                quantity=actual_qty,
                                entry_price=current_trade['entry_price'],
                                stop_loss=current_trade['bookloss_price'],
                                take_profit=current_trade['bookprofit_price']
                            )
                            
                            if sl_tp_success:
                                current_trade['sl_tp_placed'] = True
                                logger.info(f"✅ SL/TP orders placed successfully for {current_trade['symbol']}")
                            else:
                                logger.error(f"❌ Failed to place SL/TP orders for {current_trade['symbol']}")
                                # Continue monitoring but without SL/TP protection
                                current_trade['sl_tp_placed'] = False

                if current_trade['initial_completed']:
                    # Closure handling - check if position is closed by SL/TP orders
                    net_qty_now = get_open_position_qty_for_symbol(current_trade['symbol'])
                    
                    if net_qty_now == 0:
                        logger.info(f"Position closed for {current_trade['symbol']} (likely by SL/TP orders)")
                        del pending_trades[order_id]
                        current_trade = None
                        active_position = False
                        entry_time = None
                        continue
                    
                    # If SL/TP orders failed, use manual monitoring
                    if not current_trade.get('sl_tp_placed', False):
                        # Manual OCO: exit when TP or SL hit
                        dir_buy = current_trade['direction'] == 'BUY'
                        hit_tp = (current_price >= current_trade['bookprofit_price']) if dir_buy else (current_price <= current_trade['bookprofit_price'])
                        hit_sl = (current_price <= current_trade['bookloss_price']) if dir_buy else (current_price >= current_trade['bookloss_price'])
                        
                        if (hit_tp or hit_sl) and not current_trade['exit_attempted']:
                            current_trade['exit_attempted'] = True
                            exit_side = 'SELL' if dir_buy else 'BUY'
                            exit_reason = 'TP' if hit_tp else 'SL'
                            # Use actual filled quantity for exit
                            actual_qty = current_trade.get('actual_quantity', qty_int)
                            logger.info(f"{exit_reason} hit for {current_trade['symbol']} at {current_price:.2f}. Exiting {exit_side} {actual_qty}.")
                            
                            force_exit_result = place_order(
                                symbol=current_trade['symbol'],
                                token=current_trade['token'],
                                direction=exit_side,
                                quantity=actual_qty,
                                price=0  # Market exit
                            )
                            
                            if force_exit_result and isinstance(force_exit_result, dict) and force_exit_result.get('stat') == 'Ok':
                                logger.info(f"Exit placed for {current_trade['symbol']} due to {exit_reason}.")
                            else:
                                logger.warning(f"Exit placement failed for {current_trade['symbol']} due to {exit_reason}: {force_exit_result}")
                        
                        # If flat after manual exit, clean up
                        if net_qty_now == 0:
                            logger.info(f"Position flat for {current_trade['symbol']} (manual exit)")
                            del pending_trades[order_id]
                            current_trade = None
                            active_position = False
                            entry_time = None
                            continue

                # Check timeout (only if auto-close is enabled)
                if AUTO_CLOSE_ENABLED and position_duration > MAX_POSITION_DURATION and not current_trade['exit_attempted']:
                    logger.info(f"⏳ Trade Timeout for {current_trade['symbol']}. Initiating exit.")
                    current_trade['exit_attempted'] = True

                    if not current_trade['initial_completed']:
                        try:
                            api.cancel_order(orderno=current_trade['order_id'])
                            logger.info(f"Cancelled pending initial order {current_trade['order_id']}")
                        except Exception as e:
                            logger.warning(f"Could not cancel initial order: {e}")

                    # For timeout exit, use market order to ensure immediate execution
                    positions_data = None
                    current_position_qty = 0
                    try:
                        positions_data = api.get_positions()
                    except Exception as e:
                        logger.warning(f"Could not fetch positions for timeout exit: {e}")

                    iterable_positions = []
                    if isinstance(positions_data, list):
                        iterable_positions = positions_data
                    elif isinstance(positions_data, dict):
                        # Shoonya/Flattrade often return {'stat': 'Ok', 'data': [...]} or similar
                        if positions_data.get('data') is not None:
                            iterable_positions = positions_data.get('data', [])
                        elif positions_data.get('net') is not None:
                            iterable_positions = positions_data.get('net', [])

                    for pos in iterable_positions:
                        try:
                            tsym = pos.get('tsym') or pos.get('tradingsymbol') or pos.get('symbol')
                            base_symbol = tsym.split('-')[0] if tsym else None
                            qty_raw = pos.get('netqty', pos.get('qty', 0))
                            qty_val = int(qty_raw) if str(qty_raw).strip() != '' else 0
                            if base_symbol == current_trade['symbol'] and qty_val != 0:
                                current_position_qty = qty_val
                                break
                        except Exception:
                            continue
                    
                    if current_position_qty != 0:
                        force_exit_direction = 'SELL' if current_position_qty > 0 else 'BUY'
                        logger.info(f"📤 Placing timeout market exit for {current_trade['symbol']}...")
                        force_exit_result = place_order(
                            symbol=current_trade['symbol'],
                            token=current_trade['token'],
                            direction=force_exit_direction,
                            quantity=abs(current_position_qty),
                            price=0  # Market exit
                        )
                        if force_exit_result:
                            logger.info(f"✅ Timeout market exit placed for {current_trade['symbol']}")
                        else:
                            logger.warning(f"❌ Timeout market exit failed for {current_trade['symbol']}")
                    
                    del pending_trades[order_id]
                    current_trade = None
                    active_position = False
                    entry_time = None
            
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error in position manager: {e}")
            time.sleep(5)

def place_order(symbol, token, direction, quantity, price=0, price_type=None, trigger_price=0, product_type=None, bookloss_price=None, bookprofit_price=None, amo=None):
    """Place order using Flattrade/Shoonya API with correct parameters"""
    try:
        # Prepare order parameters according to API
        order_params = {
            'buy_or_sell': direction[0].upper(),  # 'B' or 'S'
            'product_type': (product_type if product_type else 'C'),  # Intraday default; 'B' for BO
            'exchange': 'NSE',
            'tradingsymbol': symbol+'-EQ',
            'quantity': str(int(quantity)),
            'discloseqty': '0',
            'price_type': (price_type if price_type else ('MKT' if price == 0 else 'LMT')),
            'price': '0' if price == 0 else str(round(float(price), 2)),
            'retention': 'DAY',
            'remarks': 'OSC_SCALPER'
        }
        
        # Handle trigger price for stop-loss orders
        if trigger_price and trigger_price > 0:
            order_params['trigger_price'] = str(round(float(trigger_price), 2))
        else:
            order_params['trigger_price'] = '0'
        
        # Handle bracket order parameters
        if product_type == 'B' and bookloss_price is not None and bookprofit_price is not None:
            # For bracket orders, ensure proper formatting
            order_params['bookloss_price'] = str(round(float(bookloss_price), 2))
            order_params['bookprofit_price'] = str(round(float(bookprofit_price), 2))
            # Bracket orders need proper price type
            if price > 0:
                order_params['price_type'] = 'LMT'
            else:
                order_params['price_type'] = 'MKT'
        
        # Optional AMO
        if amo is not None:
            order_params['amo'] = 'YES' if amo else 'NO'
        
        logger.info(f"📤 Sending order to {('FLATTRADE' if broker == 'F' else 'SHOONYA')} API:")
        logger.info(f"   Parameters: {order_params}")
        
        # Place order
        response = api.place_order(**order_params)
        
        if response is None:
            logger.error("Order placement failed with None response.")
            return None
            
        logger.info(f"📥 API Response: {response}")
        
        if isinstance(response, dict) and response.get('stat') == 'Ok':
            logger.info(f"✅ Order placed successfully: {response.get('norenordno', 'N/A')}")
            return response
        else:
            logger.error(f"❌ Order placement failed. Response: {response}")
            # Return raw response so caller can decide on fallback logic
            return response
            
    except Exception as e:
        logger.error(f"Order placement error: {str(e)}")
        return None

def place_bracket_order_with_fallback(symbol, token, direction, quantity, entry_price, stop_loss, take_profit):
    """Place market order for immediate execution, then apply SL/TP orders."""
    logger.info(f"🔄 Using Market Order Strategy for immediate execution...")
    
    # Strategy 1: Market order for immediate execution
    logger.info("   Strategy 1: Market order for immediate execution")
    market_order_result = place_order(
        symbol=symbol,
        token=token,
        direction=direction,
        quantity=quantity,
        price=0,  # Market order
        price_type='MKT',
        product_type='I'
    )
    
    if market_order_result and isinstance(market_order_result, dict) and market_order_result.get('stat') == 'Ok':
        logger.info("✅ Strategy 1 successful: Market order placed")
        return market_order_result, False  # Not a bracket order, will manage SL/TP separately
    else:
        logger.error(f"❌ Market order failed: {market_order_result}")
        return None, False

def place_sl_tp_orders(symbol, token, direction, quantity, entry_price, stop_loss, take_profit):
    """Place separate stop-loss and take-profit orders after position is established."""
    try:
        logger.info(f"📤 Placing SL/TP orders for {symbol}...")
        logger.info(f"   Entry Price: {entry_price:.2f}")
        logger.info(f"   Stop Loss: {stop_loss:.2f}")
        logger.info(f"   Take Profit: {take_profit:.2f}")
        
        # Place Stop Loss order
        sl_direction = 'SELL' if direction == 'BUY' else 'BUY'
        sl_order = place_order(
            symbol=symbol,
            token=token,
            direction=sl_direction,
            quantity=quantity,
            price=0,  # Market order for SL
            price_type='SL-MKT',  # Stop Loss Market
            trigger_price=stop_loss,
            product_type='I'
        )
        
        if not sl_order or not isinstance(sl_order, dict) or sl_order.get('stat') != 'Ok':
            logger.error(f"❌ Stop Loss order failed: {sl_order}")
            return False
        
        logger.info(f"✅ Stop Loss order placed: {sl_order.get('norenordno', 'N/A')}")
        
        # Place Take Profit order
        tp_direction = 'SELL' if direction == 'BUY' else 'BUY'
        tp_order = place_order(
            symbol=symbol,
            token=token,
            direction=tp_direction,
            quantity=quantity,
            price=take_profit,  # Limit order for TP
            price_type='LMT',
            product_type='I'
        )
        
        if not tp_order or not isinstance(tp_order, dict) or tp_order.get('stat') != 'Ok':
            logger.error(f"❌ Take Profit order failed: {tp_order}")
            # Cancel SL order if TP fails
            try:
                api.cancel_order(orderno=sl_order.get('norenordno'))
                logger.info(f"Cancelled SL order due to TP failure")
            except Exception as e:
                logger.warning(f"Could not cancel SL order: {e}")
            return False
        
        logger.info(f"✅ Take Profit order placed: {tp_order.get('norenordno', 'N/A')}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error placing SL/TP orders: {e}")
        return False

def handle_partial_fill(order_id, expected_quantity, symbol, token, direction):
    """Handle partial fills by placing additional market orders to complete the position."""
    try:
        order_details = get_order_details(order_id)
        if not order_details:
            logger.warning(f"Could not fetch order details for partial fill handling: {order_id}")
            return False
        
        # Get filled quantity
        filled_qty = int(order_details.get('fillshares', order_details.get('filled_qty', 0)))
        if filled_qty == 0:
            logger.info(f"Order {order_id} not yet filled")
            return False
        
        remaining_qty = expected_quantity - filled_qty
        if remaining_qty <= 0:
            logger.info(f"Order {order_id} fully filled: {filled_qty}/{expected_quantity}")
            return True
        
        logger.warning(f"⚠️  Partial fill detected: {filled_qty}/{expected_quantity} for {symbol}")
        logger.info(f"📤 Placing additional market order for remaining {remaining_qty} shares...")
        
        # Place additional market order for remaining quantity
        additional_order = place_order(
            symbol=symbol,
            token=token,
            direction=direction,
            quantity=remaining_qty,
            price=0,  # Market order
            price_type='MKT',
            product_type='I'
        )
        
        if additional_order and isinstance(additional_order, dict) and additional_order.get('stat') == 'Ok':
            logger.info(f"✅ Additional market order placed: {additional_order.get('norenordno', 'N/A')}")
            logger.info(f"   This will complete the position to {expected_quantity} shares")
            return True
        else:
            logger.error(f"❌ Additional market order failed: {additional_order}")
            return False
            
    except Exception as e:
        logger.error(f"Error handling partial fill: {e}")
        return False

def check_broker_capabilities():
    """Check what order types and features the broker supports."""
    try:
        logger.info(f"🔍 Checking broker capabilities for: {'FLATTRADE' if broker == 'F' else 'SHOONYA'}")
        
        # Try to get account details to verify connection
        try:
            if broker == 'F':
                # Flattrade - try to get user details
                user_details = api.get_user_details()
                if user_details:
                    logger.info(f"✅ Flattrade connection verified")
                    logger.info(f"   User: {user_details.get('uname', 'N/A')}")
                    logger.info(f"   Account: {user_details.get('actid', 'N/A')}")
                else:
                    logger.warning("⚠️  Could not verify Flattrade user details")
            else:
                # Shoonya - try to get user details
                user_details = api.get_user_details()
                if user_details:
                    logger.info(f"✅ Shoonya connection verified")
                    logger.info(f"   User: {user_details.get('uname', 'N/A')}")
                    logger.info(f"   Account: {user_details.get('actid', 'N/A')}")
                else:
                    logger.warning("⚠️  Could not verify Shoonya user details")
        except Exception as e:
            logger.warning(f"Could not fetch user details: {e}")
        
        # Explain the new trading strategy
        logger.info("📋 Trading Strategy: Market Orders + Separate SL/TP")
        logger.info("   ✅ Market orders for immediate execution (no partial fills)")
        logger.info("   ✅ Separate stop-loss and take-profit orders after position establishment")
        logger.info("   ✅ Automatic partial fill handling with additional market orders")
        logger.info("   ✅ No missed opportunities due to limit order rejections")
        
    except Exception as e:
        logger.error(f"Error checking broker capabilities: {e}")

def main():
    """Main function to start all components"""
    try:
        global processing_data
        
        # Check broker capabilities first
        check_broker_capabilities()
        
        # Start WebSocket
        if broker == 'F':
            # Flattrade WebSocket
            apiSocket.start_websocket(
                subscribe_callback=on_ticks,
                socket_open_callback=on_connect,
                socket_close_callback=on_close,
                socket_error_callback=on_error
            )
        else:
            # Shoonya WebSocket
            apiSocket.start_websocket(
                order_update_callback=None,
                subscribe_callback=on_ticks,
                socket_open_callback=on_connect,
                socket_close_callback=on_close,
                socket_error_callback=on_error
            )
        
        # Start stock picker thread
        picker_thread = Thread(target=stock_picker, daemon=True)
        picker_thread.start()
        
        # Start position manager thread
        manager_thread = Thread(target=position_manager, daemon=True)
        manager_thread.start()
        
        # Keep main thread alive
        while processing_data:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("\n🛑 Keyboard Interrupt. Shutting down...")
        processing_data = False
        if broker == 'F':
            unsubscribe_tokens = tokens
        else:
            unsubscribe_tokens = [f"NSE|{token}" for token in tokens]
        apiSocket.unsubscribe(unsubscribe_tokens)
        time.sleep(1)
        exit()

if __name__ == "__main__":
    main()