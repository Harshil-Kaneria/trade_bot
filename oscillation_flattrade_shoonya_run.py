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
parser.add_argument("--debug", action="store_true", help="Enable debug mode for detailed stock analysis")
parser.add_argument("--summary", action="store_true", help="Show oscillation market summary and exit")
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
CAPITAL = 80000  # ₹80,000
TARGET_PROFIT_PER_TRADE = 180  # ₹180 per trade
MAX_LOSS_PER_TRADE = 150
MAX_POSITION_DURATION = 300  # 5 minutes in seconds
AUTO_CLOSE_ENABLED = True
VOLATILITY_THRESHOLD = 0.0005  # Match Kite version for better opportunities
MAX_CONCURRENT_SUBS = 500  # Assuming higher limit for Shoonya/Flattrade; adjust if needed
MIN_PRICE = 100  # Minimum stock price
MAX_PRICE = 3000  # Maximum stock price
MAX_ACTIVE_TRADES = 1  # Limit concurrent trades
SHORT_EMA_PERIOD = 5  # Short EMA for trend detection
LONG_EMA_PERIOD = 20  # Long EMA for trend detection
TREND_STRENGTH_THRESHOLD = 0.0005  # Minimum EMA difference for trend
OSCILLATION_PERIOD = 10  # Match Kite version for faster detection
HISTORY_WINDOW = max(OSCILLATION_PERIOD, LONG_EMA_PERIOD) * 2  # Match Kite version
TICK_SIZE = 0.10

# Enhanced oscillation parameters
MIN_OSCILLATION_SCORE = 0.0  # Remove minimum score restriction to match Kite version
MAX_REPEAT_PICKS = 3  # Maximum times same stock can be picked in a session
MIN_VOLUME_THRESHOLD = 100000  # Minimum volume for liquidity
PRICE_CHANGE_THRESHOLD = 0.0005  # 0.05% min change for micro oscillation
MAX_TREND_STRENGTH = 0.004  # Tighter to avoid trending stocks

# Price movement tracking parameters
MIN_OSCILLATION_CYCLES = 1  # Allow a single recent cycle for faster entries
MIN_CYCLE_DURATION = 45  # ~45s between swings for semi-HFT
PRICE_MOVEMENT_THRESHOLD = 0.001  # 0.1% min movement per cycle
MAX_CYCLES_TO_TRACK = 10  # Maximum cycles to track per stock

# Global trackers
active_position = False
position_lock = Lock()
processing_data = True
trade_signal_queue = Queue()
price_history = {}  # {token: deque of prices}
pending_trades = {}  # Track pending trades like in Zerodha
recently_picked_stocks = {}  # Track recently picked stocks to avoid repetition
session_start_time = datetime.now()

# Enhanced price movement tracking
price_movements = {}  # {token: {'cycles': [], 'last_cycle_time': timestamp, 'current_direction': 'up/down'}}
target_hit_history = {}  # {token: {'last_target_hit': timestamp, 'target_price': float, 'hit_count': int}}

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
    """Enhanced oscillation score calculation with multiple factors."""
    if len(prices) < OSCILLATION_PERIOD:
        return 0.0
    
    recent_prices = np.array(prices[-OSCILLATION_PERIOD:])
    avg_price = np.mean(recent_prices)
    if avg_price == 0:
        return 0.0
    
    # 1. Price Range Score (25% weight)
    price_range = np.max(recent_prices) - np.min(recent_prices)
    range_score = min(1.0, price_range / avg_price / 0.01)  # Normalize to 1% range
    
    # 2. Volatility Consistency Score (20% weight)
    # Calculate rolling volatility and check consistency
    volatility_windows = []
    for i in range(5, len(recent_prices), 5):
        window_prices = recent_prices[max(0, i-5):i]
        if len(window_prices) >= 3:
            vol = np.std(window_prices) / np.mean(window_prices)
            volatility_windows.append(vol)
    
    if volatility_windows:
        vol_consistency = 1.0 - np.std(volatility_windows) / np.mean(volatility_windows)
        vol_consistency = max(0.0, min(1.0, vol_consistency))
    else:
        vol_consistency = 0.0
    
    # 3. Direction Change Score (25% weight)
    # Count significant direction changes
    direction_changes = 0
    min_change = avg_price * PRICE_CHANGE_THRESHOLD
    
    for i in range(2, len(recent_prices)):
        change1 = recent_prices[i-1] - recent_prices[i-2]
        change2 = recent_prices[i] - recent_prices[i-1]
        
        if abs(change1) > min_change and abs(change2) > min_change:
            if (change1 > 0 and change2 < 0) or (change1 < 0 and change2 > 0):
                direction_changes += 1
    
    direction_score = min(1.0, direction_changes / (OSCILLATION_PERIOD // 3))
    
    # 4. Mean Reversion Score (20% weight)
    # Check if prices tend to revert to mean
    deviations = recent_prices - avg_price
    mean_reversion_score = 0.0
    
    if len(deviations) >= 3:
        # Calculate correlation between current deviation and next price change
        correlations = []
        for i in range(1, len(deviations)-1):
            if abs(deviations[i]) > min_change:
                # Check if next price change is opposite to current deviation
                next_change = recent_prices[i+1] - recent_prices[i]
                if (deviations[i] > 0 and next_change < 0) or (deviations[i] < 0 and next_change > 0):
                    correlations.append(1.0)
                else:
                    correlations.append(0.0)
        
        if correlations:
            mean_reversion_score = np.mean(correlations)
    
    # 5. Trend Avoidance Score (10% weight)
    # Penalize strong trends
    trend_strength = abs(recent_prices[-1] - recent_prices[0]) / avg_price
    trend_penalty = max(0.0, 1.0 - (trend_strength / MAX_TREND_STRENGTH))
    
    # Combined weighted score
    final_score = (
        0.25 * range_score +
        0.20 * vol_consistency +
        0.25 * direction_score +
        0.20 * mean_reversion_score +
        0.10 * trend_penalty
    )
    
    return min(1.0, final_score)

def is_volatile_and_oscillating(prices):
    """Enhanced volatility and oscillation detection."""
    if len(prices) < OSCILLATION_PERIOD:
        return False
    
    recent_prices = np.array(prices[-OSCILLATION_PERIOD:])
    avg_price = np.mean(recent_prices)
    
    if avg_price == 0:
        return False
    
    # 1. Minimum volatility check
    price_range = np.max(recent_prices) - np.min(recent_prices)
    if price_range / avg_price < VOLATILITY_THRESHOLD:
        return False
    
    # 2. Maximum volatility check (avoid extreme volatility)
    if price_range / avg_price > 0.05:  # 5% max range
        return False
    
    # 3. Trend strength check
    trend_strength = abs(recent_prices[-1] - recent_prices[0]) / avg_price
    if trend_strength > MAX_TREND_STRENGTH:
        return False
    
    # 4. Direction change requirement
    min_change = avg_price * PRICE_CHANGE_THRESHOLD
    direction_changes = 0
    
    for i in range(2, len(recent_prices)):
        change1 = recent_prices[i-1] - recent_prices[i-2]
        change2 = recent_prices[i] - recent_prices[i-1]
        
        if abs(change1) > min_change and abs(change2) > min_change:
            if (change1 > 0 and change2 < 0) or (change1 < 0 and change2 > 0):
                direction_changes += 1
    
    if direction_changes < 2:  # Require at least 2 direction changes
        return False
    
    # 5. Price distribution check (avoid skewed distributions)
    price_std = np.std(recent_prices)
    if price_std / avg_price < 0.001:  # Too stable
        return False
    
    return True

def find_entry_point_long(prices):
    """Enhanced long entry point detection."""
    if len(prices) < OSCILLATION_PERIOD:
        return False
    
    current_price = prices[-1]
    recent_prices = prices[-OSCILLATION_PERIOD:]
    
    # 1. Price position relative to recent range
    recent_min = np.min(recent_prices)
    recent_max = np.max(recent_prices)
    price_range = recent_max - recent_min
    
    if price_range == 0:
        return False
    
    # Current price should be in lower 40% of range
    price_position = (current_price - recent_min) / price_range
    if price_position > 0.4:
        return False
    
    # 2. Recent price decline check
    if len(prices) >= 5:
        recent_trend = np.mean(prices[-3:]) - np.mean(prices[-6:-3])
        if recent_trend > 0:  # Still trending up
            return False
    
    # 3. Volume confirmation (if available)
    # This would require volume data integration
    
    # 4. RSI-like oversold condition
    if len(prices) >= 14:
        gains = [max(0, prices[i] - prices[i-1]) for i in range(1, len(prices))]
        losses = [max(0, prices[i-1] - prices[i]) for i in range(1, len(prices))]
        
        if len(gains) >= 14 and len(losses) >= 14:
            avg_gain = np.mean(gains[-14:])
            avg_loss = np.mean(losses[-14:])
            
            if avg_loss > 0:
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
                
                if rsi > 30:  # Not oversold enough
                    return False
    
    # 5. Confirmation: price should be below short-term moving average
    if len(prices) >= 5:
        short_ma = np.mean(prices[-5:])
        if current_price >= short_ma:
            return False
    
    return True

def find_entry_point_short(prices):
    """Enhanced short entry point detection."""
    if len(prices) < OSCILLATION_PERIOD:
        return False
    
    current_price = prices[-1]
    recent_prices = prices[-OSCILLATION_PERIOD:]
    
    # 1. Price position relative to recent range
    recent_min = np.min(recent_prices)
    recent_max = np.max(recent_prices)
    price_range = recent_max - recent_min
    
    if price_range == 0:
        return False
    
    # Current price should be in upper 40% of range
    price_position = (recent_max - current_price) / price_range
    if price_position > 0.4:
        return False
    
    # 2. Recent price rise check
    if len(prices) >= 5:
        recent_trend = np.mean(prices[-3:]) - np.mean(prices[-6:-3])
        if recent_trend < 0:  # Still trending down
            return False
    
    # 3. RSI-like overbought condition
    if len(prices) >= 14:
        gains = [max(0, prices[i] - prices[i-1]) for i in range(1, len(prices))]
        losses = [max(0, prices[i-1] - prices[i]) for i in range(1, len(prices))]
        
        if len(gains) >= 14 and len(losses) >= 14:
            avg_gain = np.mean(gains[-14:])
            avg_loss = np.mean(losses[-14:])
            
            if avg_loss > 0:
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
                
                if rsi < 70:  # Not overbought enough
                    return False
    
    # 4. Confirmation: price should be above short-term moving average
    if len(prices) >= 5:
        short_ma = np.mean(prices[-5:])
        if current_price <= short_ma:
            return False
    
    return True

def is_stock_eligible_for_pick(symbol, token, current_price):
    """Simplified eligibility check to match Kite version performance."""
    # 1. Price range (basic check only)
    if not (MIN_PRICE <= current_price <= MAX_PRICE):
        return False
    
    # 2. Sufficient history (simplified)
    if token not in price_history or len(price_history[token]) < max(OSCILLATION_PERIOD, LONG_EMA_PERIOD * 2):
        return False
    
    return True

def update_stock_pick_history(symbol):
    """Update the history of picked stocks."""
    global recently_picked_stocks
    
    current_time = datetime.now()
    
    if symbol in recently_picked_stocks:
        recently_picked_stocks[symbol]['count'] += 1
        recently_picked_stocks[symbol]['last_pick'] = current_time
    else:
        recently_picked_stocks[symbol] = {
            'count': 1,
            'last_pick': current_time
        }
    
    logger.info(f"📊 Updated pick history for {symbol}: {recently_picked_stocks[symbol]}")

def get_stock_volume_data(token):
    """Get volume data for a stock if available."""
    try:
        # This would need to be implemented based on broker API capabilities
        # For now, return a default value
        return 1000000  # Default volume
    except Exception as e:
        logger.warning(f"Could not fetch volume data for token {token}: {e}")
        return 1000000  # Default volume

def calculate_quantity(price):
    """Calculate quantity to use full capital (₹80,000) for maximum position size."""
    if price <= 0:
        return 0
    
    # Use full capital to determine quantity (exact calculation as per user)
    target_quantity = int(CAPITAL / price)
    
    # Minimum 1 share
    target_quantity = max(1, target_quantity)
    
    # Calculate actual order value
    actual_order_value = target_quantity * price
    
    logger.info(f"💰 Quantity calculation for ₹{price:.2f}:")
    logger.info(f"   Target Capital: ₹{CAPITAL:,}")
    logger.info(f"   Calculated Quantity: {target_quantity} (₹{CAPITAL:,} ÷ ₹{price:.2f})")
    logger.info(f"   Actual Order Value: ₹{actual_order_value:,.2f}")
    logger.info(f"   Capital Utilization: {(actual_order_value/CAPITAL)*100:.1f}%")
    logger.info(f"   Remaining Capital: ₹{CAPITAL - actual_order_value:,.2f}")
    
    return target_quantity

def on_ticks(tick_data):
    """Process incoming ticks and update price history with movement tracking"""
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
                # Update price history
                price_history[token].append(price)
                
                # Track price movements for oscillation detection
                current_time = datetime.now()
                track_price_movements(token, price, current_time)
                
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

# Removed complex opportunity detection - using direct scanning instead

# Removed determine_trade_parameters - creating trade parameters directly in stock picker

def stock_picker():
    """Direct stock scanning to match Kite version performance."""
    global active_position
    
    while processing_data:
        try:
            if not is_ready_for_new_trades():
                time.sleep(0.5)
                continue
            
            # Direct scanning like Kite version - check all stocks with price data
            for token in price_history:
                prices = list(price_history.get(token, []))
                if len(prices) < max(OSCILLATION_PERIOD, LONG_EMA_PERIOD * 2):
                    continue
                
                # Get symbol info
                idx = token_to_index.get(token)
                if idx is None:
                    continue
                    
                symbol = df.at[idx, 'tradingsymbol']
                current_price = prices[-1]
                
                # Basic eligibility check
                if not is_stock_eligible_for_pick(symbol, token, current_price):
                    continue
                
                # Check if already in pending trades
                if any(trade.get('symbol') == symbol for trade in pending_trades.values()):
                    continue
                
                try:
                    # Direct analysis like Kite version
                    trend = determine_trend(prices)
                    is_oscillating = is_volatile_and_oscillating(prices)
                    
                    if trend == 'positive' and is_oscillating and find_entry_point_long(prices):
                        logger.info(f"💡 Found LONG opportunity in {symbol} (LTP: {current_price}, Trend: {trend})")
                        trade_params = {
                            'symbol': symbol,
                            'token': token,
                            'direction': 'BUY',
                            'price': current_price,
                            'quantity': calculate_quantity(current_price),
                            'score': calculate_oscillation_score(prices)
                        }
                        trade_signal_queue.put(trade_params)
                        logger.info(f"📥 Trade signal queued for {symbol}")
                        break  # Only one trade at a time
                        
                    elif trend == 'negative' and is_oscillating and find_entry_point_short(prices):
                        logger.info(f"💡 Found SHORT opportunity in {symbol} (LTP: {current_price}, Trend: {trend})")
                        trade_params = {
                            'symbol': symbol,
                            'token': token,
                            'direction': 'SELL',
                            'price': current_price,
                            'quantity': calculate_quantity(current_price),
                            'score': calculate_oscillation_score(prices)
                        }
                        trade_signal_queue.put(trade_params)
                        logger.info(f"📥 Trade signal queued for {symbol}")
                        break  # Only one trade at a time
                        
                    elif trend == 'neutral' and is_oscillating and find_entry_point_long(prices):
                        logger.info(f"💡 Found NEUTRAL (LONG) opportunity in {symbol} (LTP: {current_price}, Trend: {trend})")
                        trade_params = {
                            'symbol': symbol,
                            'token': token,
                            'direction': 'BUY',
                            'price': current_price,
                            'quantity': calculate_quantity(current_price),
                            'score': calculate_oscillation_score(prices)
                        }
                        trade_signal_queue.put(trade_params)
                        logger.info(f"📥 Trade signal queued for {symbol}")
                        break  # Only one trade at a time
                        
                except Exception as e:
                    logger.error(f"Error evaluating {symbol}: {str(e)}")
                    continue
            
            time.sleep(1)  # Match Kite version timing
            
        except Exception as e:
            logger.error(f"Error in stock picker: {e}")
            time.sleep(1.0)

def is_ready_for_new_trades():
    """Check if the system is ready to accept new trades."""
    global active_position
    
    try:
        # Check if we have an active position
        with position_lock:
            if active_position:
                return False
        
        # Check if there are any pending trades
        if pending_trades:
            return False
        
        # Verify no open positions
        if not verify_no_open_positions():
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"Error checking trade readiness: {e}")
        return False  # Assume not ready on error

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

def verify_no_open_positions():
    """Verify that there are no open positions before starting new trades."""
    try:
        positions_data = api.get_positions()
        if not positions_data:
            return True
            
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
                qty_raw = pos.get('netqty', pos.get('qty', 0))
                qty_val = int(qty_raw) if str(qty_raw).strip() != '' else 0
                if qty_val != 0:
                    symbol = pos.get('tsym') or pos.get('tradingsymbol') or pos.get('symbol')
                    logger.warning(f"⚠️  Found open position: {symbol} with quantity {qty_val}")
                    return False
            except Exception:
                continue
        return True
    except Exception as e:
        logger.warning(f"Could not verify positions: {e}")
        return True  # Assume safe to proceed

def cleanup_stale_trades():
    """Clean up any stale pending trades and reset position state."""
    global active_position, pending_trades
    
    try:
        # Check if we have any pending trades that should be cleaned up
        if pending_trades:
            logger.info(f"🧹 Cleaning up {len(pending_trades)} pending trades...")
            
            for order_id, trade_info in list(pending_trades.items()):
                try:
                    # Check if order still exists and is not completed
                    order_details = get_order_details(order_id)
                    if order_details:
                        status = order_details.get('status', 'UNKNOWN')
                        if status in ['COMPLETE', 'CANCELLED', 'REJECTED']:
                            logger.info(f"🗑️  Removing completed/cancelled order {order_id} from pending trades")
                            del pending_trades[order_id]
                        else:
                            logger.info(f"⏳ Order {order_id} still pending with status: {status}")
                    else:
                        logger.warning(f"⚠️  Could not fetch details for order {order_id}, removing from pending")
                        del pending_trades[order_id]
                except Exception as e:
                    logger.error(f"Error checking order {order_id}: {e}")
                    del pending_trades[order_id]
        
        # Reset position state if no pending trades
        if not pending_trades:
            with position_lock:
                active_position = False
            logger.info("✅ Position state reset - ready for new opportunities")
            
    except Exception as e:
        logger.error(f"Error in cleanup_stale_trades: {e}")
        # Force reset on error
        with position_lock:
            active_position = False
        pending_trades.clear()

def position_manager():
    """Manage trade execution and monitoring"""
    global active_position, pending_trades
    
    current_trade = None
    entry_time = None
    
    while processing_data:
        try:
            # Check for new signals if no active position
            if not active_position and not trade_signal_queue.empty():
                # Verify no open positions before starting new trade
                if not verify_no_open_positions():
                    logger.warning("⚠️  Found open positions. Waiting for closure before new trades...")
                    time.sleep(5)
                    continue
                
                # Take the next signal and immediately mark active to avoid duplicate placements
                current_trade = trade_signal_queue.get()
                with position_lock:
                    if active_position:
                        # Another thread took it; skip
                        current_trade = None
                        continue
                    active_position = True

                logger.info(f"🚀 Executing trade signal for {current_trade['symbol']} | Dir: {current_trade['direction']} | Qty: {current_trade['quantity']}")

                # Compute BO legs: TP and SL per-share derived from total rupee goals
                ltp = None
                if len(price_history.get(current_trade['token'], [])) > 0:
                    ltp = price_history[current_trade['token']][-1]
                entry_ref_price = float(ltp if ltp is not None else current_trade['price'])
                qty_int = max(1, int(current_trade['quantity']))

                # Calculate per-share targets based on total profit/loss goals
                # Total profit target: ₹180, Total loss limit: ₹150
                tp_per_share = TARGET_PROFIT_PER_TRADE / qty_int
                sl_per_share = MAX_LOSS_PER_TRADE / qty_int

                # Derive target and stop from entry (exact calculation as per user)
                if current_trade['direction'] == 'BUY':
                    bo_target = entry_ref_price + tp_per_share
                    stop_trigger = max(0.0, entry_ref_price - sl_per_share)
                else:
                    bo_target = entry_ref_price - tp_per_share
                    stop_trigger = entry_ref_price + sl_per_share

                # Round to tick size (0.10)
                bo_target = round_to_tick(bo_target)
                stop_trigger = round_to_tick(stop_trigger)

                logger.info(
                    f"📊 Trade Setup for {current_trade['symbol']}:"
                )
                logger.info(f"   Entry Price: ₹{entry_ref_price:.2f}")
                logger.info(f"   Quantity: {qty_int}")
                logger.info(f"   Order Value: ₹{qty_int * entry_ref_price:,.2f}")
                logger.info(f"   Take Profit: ₹{bo_target:.2f} (₹{tp_per_share:.2f} per share)")
                logger.info(f"   Stop Loss: ₹{stop_trigger:.2f} (₹{sl_per_share:.2f} per share)")
                logger.info(f"   Total Profit Target: ₹{TARGET_PROFIT_PER_TRADE}")
                logger.info(f"   Total Loss Limit: ₹{MAX_LOSS_PER_TRADE}")
                logger.info(f"   Calculation: TP = {entry_ref_price:.2f} + (₹{TARGET_PROFIT_PER_TRADE} ÷ {qty_int}) = ₹{bo_target:.2f}")
                logger.info(f"   Calculation: SL = {entry_ref_price:.2f} - (₹{MAX_LOSS_PER_TRADE} ÷ {qty_int}) = ₹{stop_trigger:.2f}")

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
                        logger.info(f"✅ Position closed for {current_trade['symbol']} (likely by SL/TP orders)")
                        # Clean up and reset for new opportunity
                        del pending_trades[order_id]
                        current_trade = None
                        with position_lock:
                            active_position = False
                        entry_time = None
                        logger.info(f"🔄 Ready to scan for new opportunities...")
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
                            
                            # NEW: Record target hit to prevent immediate re-entry
                            target_price = current_trade['bookprofit_price'] if hit_tp else current_trade['bookloss_price']
                            record_target_hit(current_trade['token'], target_price, exit_reason)
                            
                            # Use actual filled quantity for exit
                            actual_qty = current_trade.get('actual_quantity', qty_int)
                            logger.info(f"🎯 {exit_reason} hit for {current_trade['symbol']} at {current_price:.2f}. Exiting {exit_side} {actual_qty}.")
                            
                            force_exit_result = place_order(
                                symbol=current_trade['symbol'],
                                token=current_trade['token'],
                                direction=exit_side,
                                quantity=actual_qty,
                                price=0  # Market exit
                            )
                            
                            if force_exit_result and isinstance(force_exit_result, dict) and force_exit_result.get('stat') == 'Ok':
                                logger.info(f"✅ Exit placed for {current_trade['symbol']} due to {exit_reason}.")
                            else:
                                logger.warning(f"❌ Exit placement failed for {current_trade['symbol']} due to {exit_reason}: {force_exit_result}")
                        
                        # If flat after manual exit, clean up
                        if net_qty_now == 0:
                            logger.info(f"✅ Position flat for {current_trade['symbol']} (manual exit)")
                            # Clean up and reset for new opportunity
                            del pending_trades[order_id]
                            current_trade = None
                            with position_lock:
                                active_position = False
                            entry_time = None
                            logger.info(f"🔄 Ready to scan for new opportunities...")
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
                    with position_lock:
                        active_position = False
                    entry_time = None
                    logger.info(f"🔄 Ready to scan for new opportunities after timeout exit...")
            
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
        logger.info("   ✅ Position state management to prevent duplicate trades")
        logger.info("   ✅ Automatic cleanup of stale trades and positions")
        
    except Exception as e:
        logger.error(f"Error checking broker capabilities: {e}")

def log_system_state():
    """Log the current system state for debugging."""
    global active_position, pending_trades
    
    try:
        logger.info("📊 Current System State:")
        logger.info(f"   Active Position: {active_position}")
        logger.info(f"   Pending Trades: {len(pending_trades)}")
        logger.info(f"   Trade Signal Queue Size: {trade_signal_queue.qsize()}")
        
        if pending_trades:
            logger.info("   Pending Trade Details:")
            for order_id, trade_info in pending_trades.items():
                symbol = trade_info.get('symbol', 'N/A')
                status = 'Initial' if not trade_info.get('initial_completed', False) else 'Active'
                logger.info(f"     Order {order_id}: {symbol} - {status}")
        
        # Check actual positions
        if verify_no_open_positions():
            logger.info("   ✅ No open positions detected")
        else:
            logger.info("   ⚠️  Open positions detected - scanning for closure")
            
    except Exception as e:
        logger.error(f"Error logging system state: {e}")

def reset_pick_history():
    """Reset pick history for new trading session."""
    global recently_picked_stocks, session_start_time
    
    current_time = datetime.now()
    
    # Reset if more than 2 hours have passed
    if (current_time - session_start_time).total_seconds() > 7200:
        logger.info("🔄 Resetting pick history for new trading session")
        recently_picked_stocks.clear()
        session_start_time = current_time
        return True
    
    # Reset if no stocks have been picked in the last hour
    if recently_picked_stocks:
        oldest_pick = min(recently_picked_stocks.values(), key=lambda x: x['last_pick'])
        if (current_time - oldest_pick['last_pick']).total_seconds() > 3600:
            logger.info("🔄 Resetting pick history due to inactivity")
            recently_picked_stocks.clear()
            session_start_time = current_time
            return True
    
    return False

def log_oscillation_market_summary():
    """Log comprehensive summary of oscillation patterns across all stocks."""
    try:
        logger.info("📊 Oscillation Market Summary:")
        
        total_stocks = len(price_history)
        stocks_with_cycles = 0
        stocks_with_sufficient_cycles = 0
        stocks_with_target_hits = 0
        total_cycles = 0
        avg_cycle_score = 0.0
        
        cycle_scores = []
        pattern_qualities = {'Poor': 0, 'Fair': 0, 'Good': 0, 'Excellent': 0}
        
        for token in price_history:
            if token in price_movements:
                movement_data = price_movements[token]
                cycles = movement_data['cycles']
                
                if cycles:
                    stocks_with_cycles += 1
                    total_cycles += len(cycles)
                    
                    # Check if sufficient cycles for trading
                    if has_sufficient_oscillation_cycles(token):
                        stocks_with_sufficient_cycles += 1
                    
                    # Get cycle score
                    cycle_score = get_oscillation_cycle_score(token)
                    if cycle_score > 0:
                        cycle_scores.append(cycle_score)
                    
                    # Get pattern quality
                    pattern_analysis = analyze_oscillation_patterns(token)
                    if pattern_analysis:
                        quality = pattern_analysis['pattern_quality']
                        pattern_qualities[quality] += 1
            
            # Check target hits
            if token in target_hit_history:
                stocks_with_target_hits += 1
        
        # Calculate averages
        if cycle_scores:
            avg_cycle_score = np.mean(cycle_scores)
        
        logger.info(f"   Total Stocks Monitored: {total_stocks}")
        logger.info(f"   Stocks with Cycles: {stocks_with_cycles}")
        logger.info(f"   Stocks with Sufficient Cycles: {stocks_with_sufficient_cycles}")
        logger.info(f"   Total Cycles Detected: {total_cycles}")
        logger.info(f"   Average Cycle Score: {avg_cycle_score:.4f}")
        logger.info(f"   Stocks with Target Hits: {stocks_with_target_hits}")
        
        logger.info(f"   Pattern Quality Distribution:")
        for quality, count in pattern_qualities.items():
            percentage = (count / max(1, stocks_with_cycles)) * 100
            logger.info(f"     {quality}: {count} ({percentage:.1f}%)")
        
        # Show top oscillating stocks
        top_oscillators = []
        for token in price_history:
            if token in price_movements:
                cycle_score = get_oscillation_cycle_score(token)
                if cycle_score > 0.2:  # Only show good oscillators
                    idx = token_to_index.get(token)
                    if idx is not None:
                        symbol = df.at[idx, 'tradingsymbol']
                        top_oscillators.append((symbol, cycle_score))
        
        if top_oscillators:
            top_oscillators.sort(key=lambda x: x[1], reverse=True)
            logger.info(f"   Top Oscillating Stocks (Score > 0.2):")
            for i, (symbol, score) in enumerate(top_oscillators[:10]):
                logger.info(f"     {i+1}. {symbol}: {score:.4f}")
        
        # Show recent target hits
        recent_targets = []
        current_time = datetime.now()
        for token, hit_data in target_hit_history.items():
            time_since = (current_time - hit_data['last_target_hit']).total_seconds() / 60
            if time_since <= 60:  # Last hour
                idx = token_to_index.get(token)
                if idx is not None:
                    symbol = df.at[idx, 'tradingsymbol']
                    recent_targets.append((symbol, hit_data['hit_type'], hit_data['target_price'], time_since))
        
        if recent_targets:
            recent_targets.sort(key=lambda x: x[3])  # Sort by time
            logger.info(f"   Recent Target Hits (Last Hour):")
            for symbol, hit_type, price, time_since in recent_targets[:5]:
                logger.info(f"     {symbol}: {hit_type} at ₹{price:.2f} ({time_since:.1f} min ago)")
        
    except Exception as e:
        logger.error(f"Error in oscillation market summary: {e}")

def log_oscillation_analysis():
    """Log detailed analysis of current oscillation patterns."""
    try:
        logger.info("📊 Oscillation Analysis Summary:")
        
        total_stocks = len(price_history)
        stocks_with_data = sum(1 for prices in price_history.values() if len(prices) >= HISTORY_WINDOW)
        eligible_stocks = 0
        high_score_stocks = 0
        
        for token in price_history:
            prices = list(price_history.get(token, []))
            if len(prices) >= HISTORY_WINDOW:
                eligible_stocks += 1
                score = calculate_oscillation_score(prices)
                if score >= MIN_OSCILLATION_SCORE:
                    high_score_stocks += 1
        
        logger.info(f"   Total Stocks Monitored: {total_stocks}")
        logger.info(f"   Stocks with Sufficient Data: {stocks_with_data}")
        logger.info(f"   Eligible for Analysis: {eligible_stocks}")
        logger.info(f"   High Score Candidates: {high_score_stocks}")
        logger.info(f"   Recently Picked Stocks: {len(recently_picked_stocks)}")
        
        if recently_picked_stocks:
            logger.info("   Recent Pick Details:")
            for symbol, data in list(recently_picked_stocks.items())[:5]:
                time_since_pick = (datetime.now() - data['last_pick']).total_seconds() / 60
                logger.info(f"     {symbol}: {data['count']} picks, {time_since_pick:.1f} min ago")
        
        # Add oscillation market summary
        log_oscillation_market_summary()
        
    except Exception as e:
        logger.error(f"Error in oscillation analysis logging: {e}")

def debug_stock_analysis(symbol, token, prices):
    """Comprehensive debugging function to analyze why a stock was selected or rejected."""
    try:
        if len(prices) < OSCILLATION_PERIOD:
            logger.info(f"🔍 {symbol}: Insufficient price data ({len(prices)} < {OSCILLATION_PERIOD})")
            return
        
        current_price = prices[-1]
        recent_prices = np.array(prices[-OSCILLATION_PERIOD:])
        avg_price = np.mean(recent_prices)
        
        logger.info(f"🔍 Detailed Analysis for {symbol}:")
        logger.info(f"   Current Price: ₹{current_price:.2f}")
        logger.info(f"   Average Price: ₹{avg_price:.2f}")
        logger.info(f"   Price Range: ₹{np.min(recent_prices):.2f} - ₹{np.max(recent_prices):.2f}")
        
        # 1. Eligibility Check
        eligible = is_stock_eligible_for_pick(symbol, token, current_price)
        logger.info(f"   Eligibility: {'✅' if eligible else '❌'}")
        
        if not eligible:
            if symbol in recently_picked_stocks:
                pick_data = recently_picked_stocks[symbol]
                time_since = (datetime.now() - pick_data['last_pick']).total_seconds() / 60
                logger.info(f"     Reason: Recently picked {pick_data['count']} times, {time_since:.1f} min ago")
            return
        
        # 2. Oscillation Score Components
        score = calculate_oscillation_score(prices)
        logger.info(f"   Oscillation Score: {score:.4f}")
        
        if score < MIN_OSCILLATION_SCORE:
            logger.info(f"     ❌ Below minimum threshold ({MIN_OSCILLATION_SCORE})")
            return
        
        # 3. Volatility Analysis
        price_range = np.max(recent_prices) - np.min(recent_prices)
        volatility = price_range / avg_price
        logger.info(f"   Volatility: {volatility:.4f} ({'✅' if VOLATILITY_THRESHOLD <= volatility <= 0.05 else '❌'})")
        
        # 4. Trend Analysis
        trend_strength = abs(recent_prices[-1] - recent_prices[0]) / avg_price
        logger.info(f"   Trend Strength: {trend_strength:.4f} ({'✅' if trend_strength <= MAX_TREND_STRENGTH else '❌'})")
        
        # 5. Direction Changes
        min_change = avg_price * PRICE_CHANGE_THRESHOLD
        direction_changes = 0
        for i in range(2, len(recent_prices)):
            change1 = recent_prices[i-1] - recent_prices[i-2]
            change2 = recent_prices[i] - recent_prices[i-1]
            if abs(change1) > min_change and abs(change2) > min_change:
                if (change1 > 0 and change2 < 0) or (change1 < 0 and change2 > 0):
                    direction_changes += 1
        
        logger.info(f"   Direction Changes: {direction_changes} ({'✅' if direction_changes >= 2 else '❌'})")
        
        # 6. Entry Point Analysis
        long_entry = find_entry_point_long(prices)
        short_entry = find_entry_point_short(prices)
        logger.info(f"   Long Entry: {'✅' if long_entry else '❌'}")
        logger.info(f"   Short Entry: {'✅' if short_entry else '❌'}")
        
        # 7. Final Recommendation
        if long_entry or short_entry:
            direction = 'BUY' if long_entry else 'SELL'
            logger.info(f"   🎯 RECOMMENDATION: {direction} {symbol}")
            logger.info(f"   📊 Composite Score: {score:.4f}")
        else:
            logger.info(f"   ⏸️  No entry point found for {symbol}")
        
    except Exception as e:
        logger.error(f"Error in stock analysis for {symbol}: {e}")

def analyze_market_conditions():
    """Analyze overall market conditions and adjust parameters if needed."""
    try:
        logger.info("🌍 Market Conditions Analysis:")
        
        # Calculate average volatility across all stocks
        volatilities = []
        scores = []
        
        for token in price_history:
            prices = list(price_history.get(token, []))
            if len(prices) >= HISTORY_WINDOW:
                recent_prices = prices[-10:]
                if len(recent_prices) >= 5:
                    vol = np.std(recent_prices) / np.mean(recent_prices)
                    volatilities.append(vol)
                    
                    score = calculate_oscillation_score(prices)
                    if score > 0:
                        scores.append(score)
        
        if volatilities:
            avg_volatility = np.mean(volatilities)
            logger.info(f"   Average Market Volatility: {avg_volatility:.4f}")
            
            # Adjust thresholds based on market conditions
            if avg_volatility < 0.001:  # Low volatility market
                logger.info("   📉 Low volatility market detected - consider lowering thresholds")
            elif avg_volatility > 0.005:  # High volatility market
                logger.info("   📈 High volatility market detected - consider raising thresholds")
        
        if scores:
            avg_score = np.mean(scores)
            logger.info(f"   Average Oscillation Score: {avg_score:.4f}")
            
            # Adjust minimum score threshold
            if avg_score < MIN_OSCILLATION_SCORE:
                logger.info(f"   ⚠️  Market conditions poor - few high-quality opportunities")
            elif avg_score > 0.3:
                logger.info(f"   ✅ Excellent market conditions - many opportunities available")
        
        # Count opportunities by score ranges
        score_ranges = {'0.15-0.25': 0, '0.25-0.35': 0, '0.35+': 0}
        for token in price_history:
            prices = list(price_history.get(token, []))
            if len(prices) >= HISTORY_WINDOW:
                score = calculate_oscillation_score(prices)
                if 0.15 <= score < 0.25:
                    score_ranges['0.15-0.25'] += 1
                elif 0.25 <= score < 0.35:
                    score_ranges['0.25-0.35'] += 1
                elif score >= 0.35:
                    score_ranges['0.35+'] += 1
        
        logger.info(f"   Score Distribution:")
        for range_name, count in score_ranges.items():
            logger.info(f"     {range_name}: {count} stocks")
        
    except Exception as e:
        logger.error(f"Error in market conditions analysis: {e}")

def enable_debug_mode():
    """Enable debug mode for detailed analysis."""
    global args
    
    if args.debug:
        logger.info("🔍 Debug mode enabled - detailed stock analysis available")
        logger.info("   Use debug_stock_analysis() function to analyze specific stocks")
        logger.info("   Example: debug_stock_analysis('RELIANCE', '12345', price_history['12345'])")
        
        # Add debug info to main logging
        logger.info("   Enhanced logging and analysis features active")
        logger.info("   Market conditions analysis will run every 10 minutes")
        logger.info("   Detailed oscillation analysis will run every 5 minutes")
        
        return True
    return False

def debug_top_opportunities():
    """Debug analyze top oscillation opportunities with cycle analysis."""
    if not args.debug:
        return
    
    try:
        opportunities = find_oscillation_opportunities()
        if not opportunities:
            logger.info("🔍 Debug: No opportunities found for analysis")
            return
        
        logger.info(f"🔍 Debug: Analyzing top {min(3, len(opportunities))} opportunities:")
        
        for i, opp in enumerate(opportunities[:3]):
            logger.info(f"\n--- Analysis {i+1}: {opp['symbol']} ---")
            debug_stock_analysis(opp['symbol'], opp['token'], opp['prices'])
            
            # Add cycle analysis
            logger.info(f"\n--- Cycle Analysis for {opp['symbol']} ---")
            debug_oscillation_cycles(opp['token'], opp['symbol'])
            
    except Exception as e:
        logger.error(f"Error in debug analysis: {e}")

def track_price_movements(token, current_price, timestamp):
    """Track price movements and detect oscillation cycles."""
    global price_movements
    
    if token not in price_movements:
        price_movements[token] = {
            'cycles': [],
            'last_cycle_time': timestamp,
            'current_direction': None,
            'start_price': current_price,
            'peak_price': current_price,
            'trough_price': current_price,
            'last_price': current_price
        }
        return
    
    movement_data = price_movements[token]
    last_price = movement_data['last_price']
    price_change = (current_price - last_price) / last_price
    
    # Only track significant movements
    if abs(price_change) < PRICE_MOVEMENT_THRESHOLD:
        movement_data['last_price'] = current_price
        return
    
    current_direction = 'up' if price_change > 0 else 'down'
    
    # Detect direction change
    if movement_data['current_direction'] and movement_data['current_direction'] != current_direction:
        # Direction changed - potential cycle completion
        if movement_data['current_direction'] == 'up':
            # Up cycle completed, record peak
            movement_data['peak_price'] = max(movement_data['peak_price'], last_price)
        else:
            # Down cycle completed, record trough
            movement_data['trough_price'] = min(movement_data['trough_price'], last_price)
        
        # Check if this constitutes a valid cycle
        cycle_range = abs(movement_data['peak_price'] - movement_data['trough_price'])
        cycle_percentage = cycle_range / movement_data['start_price']
        
        if cycle_percentage >= PRICE_MOVEMENT_THRESHOLD:
            # Valid cycle detected
            cycle = {
                'start_time': movement_data['last_cycle_time'],
                'end_time': timestamp,
                'start_price': movement_data['start_price'],
                'peak_price': movement_data['peak_price'],
                'trough_price': movement_data['trough_price'],
                'range': cycle_range,
                'percentage': cycle_percentage,
                'direction': movement_data['current_direction']
            }
            
            movement_data['cycles'].append(cycle)
            
            # Keep only recent cycles
            if len(movement_data['cycles']) > MAX_CYCLES_TO_TRACK:
                movement_data['cycles'].pop(0)
            
            # Reset for next cycle
            movement_data['start_price'] = current_price
            movement_data['peak_price'] = current_price
            movement_data['trough_price'] = current_price
            movement_data['last_cycle_time'] = timestamp
            
            logger.debug(f"🔄 Cycle detected for token {token}: {cycle['percentage']:.4f} range")
    
    # Update current state
    movement_data['current_direction'] = current_direction
    movement_data['last_price'] = current_price

# Removed complex cycle analysis functions - using simplified approach

# Removed complex cycle scoring - using simplified approach

def check_target_hit_eligibility(token, current_price):
    """Simplified eligibility check - always allow for better performance."""
    return True

def record_target_hit(token, target_price, hit_type='TP'):
    """Record when a target (TP/SL) is hit to prevent immediate re-entry."""
    global target_hit_history
    
    current_time = datetime.now()
    
    if token not in target_hit_history:
        target_hit_history[token] = {
            'last_target_hit': current_time,
            'target_price': target_price,
            'hit_type': hit_type,
            'hit_count': 1
        }
    else:
        target_hit_history[token].update({
            'last_target_hit': current_time,
            'target_price': target_price,
            'hit_type': hit_type,
            'hit_count': target_hit_history[token]['hit_count'] + 1
        })
    
    logger.info(f"🎯 Target {hit_type} hit for token {token} at ₹{target_price:.2f}")
    logger.info(f"   Hit count: {target_hit_history[token]['hit_count']}")
    logger.info(f"   Waiting period: {MIN_CYCLE_DURATION * 2} seconds before next trade")

def analyze_oscillation_patterns(token):
    """Analyze oscillation patterns for a specific token."""
    if token not in price_movements:
        return None
    
    movement_data = price_movements[token]
    cycles = movement_data['cycles']
    
    if not cycles:
        return None
    
    analysis = {
        'total_cycles': len(cycles),
        'recent_cycles': 0,
        'avg_range': 0.0,
        'consistency': 0.0,
        'last_cycle_time': None,
        'pattern_quality': 'Poor'
    }
    
    if cycles:
        current_time = datetime.now()
        recent_cycles = [cycle for cycle in cycles 
                         if (current_time - cycle['end_time']).total_seconds() <= MIN_CYCLE_DURATION * 2]
        
        analysis['recent_cycles'] = len(recent_cycles)
        analysis['last_cycle_time'] = cycles[-1]['end_time']
        
        if len(cycles) >= 2:
            ranges = [cycle['percentage'] for cycle in cycles]
            analysis['avg_range'] = np.mean(ranges)
            analysis['consistency'] = 1.0 - min(1.0, np.std(ranges) / np.mean(ranges))
            
            # Determine pattern quality
            if len(recent_cycles) >= MIN_OSCILLATION_CYCLES and analysis['consistency'] > 0.7:
                analysis['pattern_quality'] = 'Excellent'
            elif len(recent_cycles) >= MIN_OSCILLATION_CYCLES and analysis['consistency'] > 0.5:
                analysis['pattern_quality'] = 'Good'
            elif len(recent_cycles) >= 1:
                analysis['pattern_quality'] = 'Fair'
    
    return analysis

def debug_oscillation_cycles(token, symbol):
    """Debug analyze oscillation cycles for a specific stock."""
    if token not in price_movements:
        logger.info(f"🔍 {symbol}: No price movement data available")
        return
    
    movement_data = price_movements[token]
    cycles = movement_data['cycles']
    
    logger.info(f"🔍 Oscillation Cycle Analysis for {symbol}:")
    logger.info(f"   Total Cycles Tracked: {len(cycles)}")
    logger.info(f"   Current Direction: {movement_data['current_direction']}")
    logger.info(f"   Last Price: ₹{movement_data['last_price']:.2f}")
    logger.info(f"   Peak Price: ₹{movement_data['peak_price']:.2f}")
    logger.info(f"   Trough Price: ₹{movement_data['trough_price']:.2f}")
    
    if cycles:
        logger.info(f"   Recent Cycles:")
        for i, cycle in enumerate(cycles[-5:]):  # Show last 5 cycles
            duration = (cycle['end_time'] - cycle['start_time']).total_seconds() / 60
            logger.info(f"     Cycle {i+1}: {cycle['start_price']:.2f} → {cycle['peak_price']:.2f} → {cycle['trough_price']:.2f}")
            logger.info(f"       Range: {cycle['range']:.2f} ({cycle['percentage']:.4f}) | Duration: {duration:.1f} min")
        
        # Calculate statistics
        ranges = [cycle['percentage'] for cycle in cycles]
        durations = [(cycle['end_time'] - cycle['start_time']).total_seconds() / 60 for cycle in cycles]
        
        logger.info(f"   Statistics:")
        logger.info(f"     Average Range: {np.mean(ranges):.4f}")
        logger.info(f"     Range Std Dev: {np.std(ranges):.4f}")
        logger.info(f"     Average Duration: {np.mean(durations):.1f} min")
        logger.info(f"     Consistency Score: {1.0 - min(1.0, np.std(ranges) / np.mean(ranges)):.4f}")
    
    # Check target hit history
    if token in target_hit_history:
        hit_data = target_hit_history[token]
        time_since_hit = (datetime.now() - hit_data['last_target_hit']).total_seconds() / 60
        logger.info(f"   Target Hit History:")
        logger.info(f"     Last {hit_data['hit_type']} hit: {time_since_hit:.1f} min ago at ₹{hit_data['target_price']:.2f}")
        logger.info(f"     Total hits: {hit_data['hit_count']}")
        
        # Check eligibility
        current_price = movement_data['last_price']
        eligible = check_target_hit_eligibility(token, current_price)
        logger.info(f"     Eligible for new trade: {'✅' if eligible else '❌'}")
    
    # Check if sufficient cycles for trading
    sufficient_cycles = has_sufficient_oscillation_cycles(token)
    logger.info(f"   Sufficient Cycles for Trading: {'✅' if sufficient_cycles else '❌'}")
    
    # Get cycle score
    cycle_score = get_oscillation_cycle_score(token)
    logger.info(f"   Cycle Score: {cycle_score:.4f}")

def main():
    """Enhanced main function with better session management and logging."""
    try:
        global processing_data, session_start_time
        
        # Check if user just wants summary
        if args.summary:
            logger.info("📊 Oscillation Market Summary Mode")
            logger.info("   Loading price data for analysis...")
            
            # Initialize basic data structures
            for token in df['instrument_token']:
                price_history[str(token)] = deque(maxlen=HISTORY_WINDOW)
            
            # Try to load some historical data for analysis
            logger.info("   Fetching historical data for analysis...")
            for i, token in enumerate(df['instrument_token'][:50]):  # Sample first 50 stocks
                try:
                    hist_prices = get_price_history(str(token), minutes=60)
                    if hist_prices:
                        price_history[str(token)].extend(hist_prices)
                        # Simulate some price movements for demonstration
                        if len(hist_prices) >= 10:
                            for j in range(10):
                                price = hist_prices[-1] + (np.random.random() - 0.5) * hist_prices[-1] * 0.01
                                track_price_movements(str(token), price, datetime.now() - timedelta(minutes=10-j))
                except Exception as e:
                    continue
                
                if i % 10 == 0:
                    logger.info(f"   Processed {i+1}/50 stocks...")
            
            logger.info("   Analysis complete. Showing oscillation market summary:")
            log_oscillation_market_summary()
            return
        
        # Initialize session
        session_start_time = datetime.now()
        logger.info(f"🚀 Starting Oscillation Scalper - Session: {session_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"   Broker: {'FLATTRADE' if broker == 'F' else 'SHOONYA'}")
        logger.info(f"   Capital: ₹{CAPITAL:,}")
        logger.info(f"   Target Profit: ₹{TARGET_PROFIT_PER_TRADE}")
        logger.info(f"   Max Loss: ₹{MAX_LOSS_PER_TRADE}")
        logger.info(f"   Min Oscillation Score: {MIN_OSCILLATION_SCORE}")
        logger.info(f"   Max Repeat Picks: {MAX_REPEAT_PICKS}")
        logger.info(f"   Min Oscillation Cycles: {MIN_OSCILLATION_CYCLES}")
        logger.info(f"   Min Cycle Duration: {MIN_CYCLE_DURATION} seconds")
        
        # Enable debug mode if requested
        debug_active = enable_debug_mode()
        
        # Check broker capabilities first
        check_broker_capabilities()
        
        # Initial cleanup to ensure clean state
        logger.info("🧹 Performing initial cleanup...")
        cleanup_stale_trades()
        
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
        logger.info("✅ Stock picker thread started")
        
        # Start position manager thread
        manager_thread = Thread(target=position_manager, daemon=True)
        manager_thread.start()
        logger.info("✅ Position manager thread started")
        
        # Keep main thread alive with periodic cleanup and state logging
        last_cleanup = time.time()
        last_state_log = time.time()
        last_analysis_log = time.time()
        last_pick_reset = time.time()
        last_market_analysis = time.time()
        last_debug_analysis = time.time()
        
        while processing_data:
            current_time = time.time()
            
            # Periodic cleanup every 5 minutes
            if current_time - last_cleanup > 300:  # 5 minutes
                logger.info("🧹 Performing periodic cleanup...")
                cleanup_stale_trades()
                last_cleanup = current_time
            
            # Periodic system state logging every 2 minutes
            if current_time - last_state_log > 120:  # 2 minutes
                log_system_state()
                last_state_log = current_time
            
            # Periodic oscillation analysis every 5 minutes
            if current_time - last_analysis_log > 300:  # 5 minutes
                log_oscillation_analysis()
                last_analysis_log = current_time
            
            # Periodic pick history reset every 30 minutes
            if current_time - last_pick_reset > 1800:  # 30 minutes
                if reset_pick_history():
                    last_pick_reset = current_time
            
            # Periodic market conditions analysis every 10 minutes
            if current_time - last_market_analysis > 600:  # 10 minutes
                analyze_market_conditions()
                last_market_analysis = current_time
            
            # Periodic debug analysis every 15 minutes (only in debug mode)
            if debug_active and current_time - last_debug_analysis > 900:  # 15 minutes
                debug_top_opportunities()
                last_debug_analysis = current_time
            
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