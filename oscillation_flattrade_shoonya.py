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
MAX_POSITION_DURATION = 300  # 5 minutes in seconds
VOLATILITY_THRESHOLD = 0.0005  # 0.05% for small price movements
MAX_CONCURRENT_SUBS = 500  # Assuming higher limit for Shoonya/Flattrade; adjust if needed
MIN_PRICE = 100  # Minimum stock price
MAX_PRICE = 3000  # Maximum stock price
MAX_ACTIVE_TRADES = 1  # Limit concurrent trades
SHORT_EMA_PERIOD = 5  # Short EMA for trend detection
LONG_EMA_PERIOD = 20  # Long EMA for trend detection
TREND_STRENGTH_THRESHOLD = 0.0005  # Minimum EMA difference for trend
OSCILLATION_PERIOD = 10  # Short period for scalping
HISTORY_WINDOW = max(OSCILLATION_PERIOD, LONG_EMA_PERIOD) * 2  # For price history

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
    
    target_profit_percentage = 0.002
    target_profit_per_share = price * target_profit_percentage
    
    if target_profit_per_share <= 0:
        return 0
    
    quantity = int(TARGET_PROFIT_PER_TRADE / target_profit_per_share)
    quantity = max(1, quantity)
    quantity = min(quantity, int(CAPITAL / price))
    
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

    # Round target to nearest 0.05
    if direction == 'BUY':
        target = round(price * 1.002 / 0.05) * 0.05
    else:
        target = round(price * 0.998 / 0.05) * 0.05

    return {
        'symbol': opportunity['symbol'],
        'token': opportunity['token'],
        'direction': direction,
        'price': price,
        'quantity': quantity,
        'target': target,
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

def position_manager():
    """Manage trade execution and monitoring"""
    global active_position, pending_trades
    
    current_trade = None
    entry_time = None
    
    while processing_data:
        try:
            # Check for new signals if no active position
            if not active_position and not trade_signal_queue.empty():
                current_trade = trade_signal_queue.get()
                logger.info(f"Executing trade: {current_trade}")
                
                # Place market entry order
                order_result = place_order(
                    symbol=current_trade['symbol'],
                    token=current_trade['token'],
                    direction=current_trade['direction'],
                    quantity=current_trade['quantity'],
                    price=0  # Market order
                )
                
                if order_result and order_result.get('stat') == 'Ok':
                    current_trade['order_id'] = order_result.get('norenordno')
                    current_trade['exit_order_id'] = None
                    current_trade['initial_completed'] = False
                    current_trade['exit_completed'] = False
                    current_trade['exit_attempted'] = False
                    entry_time = datetime.now()
                    active_position = True
                    pending_trades[current_trade['order_id']] = current_trade
                    logger.info(f"Trade initiated: {current_trade}")
                else:
                    logger.error(f"Order failed: {order_result}")
                    current_trade = None
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
                        current_trade['initial_completed'] = True
                        current_trade['entry_price'] = float(initial_order_status.get('avgprc', current_price))
                        logger.info(f"✅ Initial {current_trade['direction']} Order COMPLETE for {current_trade['symbol']} @ ₹{current_trade['entry_price']:.2f}")

                        # Adjust target if needed
                        if current_trade['direction'] == 'BUY' and current_trade['target'] <= current_trade['entry_price']:
                            current_trade['target'] = round(current_trade['entry_price'] * 1.002 / 0.05) * 0.05
                        elif current_trade['direction'] == 'SELL' and current_trade['target'] >= current_trade['entry_price']:
                            current_trade['target'] = round(current_trade['entry_price'] * 0.998 / 0.05) * 0.05

                        # Place limit exit order
                        exit_direction = 'SELL' if current_trade['direction'] == 'BUY' else 'BUY'
                        exit_result = place_order(
                            symbol=current_trade['symbol'],
                            token=current_trade['token'],
                            direction=exit_direction,
                            quantity=current_trade['quantity'],
                            price=current_trade['target']  # Limit order
                        )
                        
                        if exit_result and exit_result.get('stat') == 'Ok':
                            current_trade['exit_order_id'] = exit_result.get('norenordno')
                            logger.info(f"Placed Exit Order for {current_trade['symbol']}: {current_trade['exit_order_id']} @ ₹{current_trade['target']:.2f}")
                        else:
                            logger.error(f"Exit order failed: {exit_result}")

                if current_trade['initial_completed'] and not current_trade['exit_completed'] and current_trade['exit_order_id']:
                    exit_order_status = get_order_details(current_trade['exit_order_id'])
                    
                    if exit_order_status and exit_order_status.get('status') == 'COMPLETE':
                        current_trade['exit_completed'] = True
                        executed_exit_price = float(exit_order_status.get('avgprc', current_price))
                        
                        pnl = 0
                        if current_trade['direction'] == 'BUY':
                            pnl = (executed_exit_price - current_trade['entry_price']) * current_trade['quantity']
                        else:
                            pnl = (current_trade['entry_price'] - executed_exit_price) * current_trade['quantity']
                        
                        logger.info(f"Position closed. P&L: ₹{pnl:.2f}")
                        
                        # Reset position
                        del pending_trades[order_id]
                        current_trade = None
                        active_position = False
                        entry_time = None
                        continue

                    # Trailing logic
                    new_target = current_trade['target']
                    if current_trade['direction'] == 'BUY':
                        if current_price > current_trade['target'] * 1.003:
                            new_target = round(current_price * 0.998 / 0.05) * 0.05
                            if new_target > current_trade['target']:
                                try:
                                    api.modify_order(
                                        orderno=current_trade['exit_order_id'],
                                        price_type='LMT',
                                        price=str(new_target)
                                    )
                                    current_trade['target'] = new_target
                                    logger.info(f"🔼 Trailing exit order for {current_trade['symbol']} to ₹{new_target:.2f}")
                                except Exception as e:
                                    logger.error(f"Error modifying exit order: {e}")
                    else:
                        if current_price < current_trade['target'] * 0.997:
                            new_target = round(current_price * 1.002 / 0.05) * 0.05
                            if new_target < current_trade['target']:
                                try:
                                    api.modify_order(
                                        orderno=current_trade['exit_order_id'],
                                        price_type='LMT',
                                        price=str(new_target)
                                    )
                                    current_trade['target'] = new_target
                                    logger.info(f"🔽 Trailing exit order for {current_trade['symbol']} to ₹{new_target:.2f}")
                                except Exception as e:
                                    logger.error(f"Error modifying exit order: {e}")

                # Check timeout
                if position_duration > MAX_POSITION_DURATION and not current_trade['exit_attempted']:
                    logger.info(f"⏳ Trade Timeout for {current_trade['symbol']}. Initiating exit.")
                    current_trade['exit_attempted'] = True

                    if not current_trade['initial_completed']:
                        try:
                            api.cancel_order(orderno=current_trade['order_id'])
                            logger.info(f"Cancelled pending initial order {current_trade['order_id']}")
                        except Exception as e:
                            logger.warning(f"Could not cancel initial order: {e}")

                    if current_trade['initial_completed'] and current_trade['exit_order_id']:
                        try:
                            api.cancel_order(orderno=current_trade['exit_order_id'])
                            logger.info(f"Cancelled pending exit order {current_trade['exit_order_id']}")
                        except Exception as e:
                            logger.warning(f"Could not cancel exit order: {e}")
                    
                    # Force market exit if position open
                    positions = api.get_positions()
                    current_position_qty = 0
                    for pos in positions.get('net', []):
                        if pos.get('tsym') == current_trade['symbol'] and int(pos.get('netqty', 0)) != 0:
                            current_position_qty = int(pos['netqty'])
                            break
                    
                    if current_position_qty != 0:
                        force_exit_direction = 'SELL' if current_position_qty > 0 else 'BUY'
                        force_exit_result = place_order(
                            symbol=current_trade['symbol'],
                            token=current_trade['token'],
                            direction=force_exit_direction,
                            quantity=abs(current_position_qty),
                            price=0  # Market
                        )
                        if force_exit_result:
                            logger.info(f"Forced market exit placed for {current_trade['symbol']}")
                    
                    del pending_trades[order_id]
                    current_trade = None
                    active_position = False
                    entry_time = None
            
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error in position manager: {e}")
            time.sleep(5)

def place_order(symbol, token, direction, quantity, price=0):
    """Place order using Flattrade/Shoonya API with correct parameters"""
    try:
        # Prepare order parameters according to API
        order_params = {
            'buy_or_sell': direction[0].upper(),  # 'B' or 'S'
            'product_type': 'I',  # Intraday
            'exchange': 'NSE',
            'tradingsymbol': symbol+'-EQ',
            'quantity': str(int(quantity)),
            'discloseqty': '0',
            'price_type': 'MKT' if price == 0 else 'LMT',
            'price': '0' if price == 0 else str(round(float(price), 2)),
            'trigger_price': '0',
            'retention': 'DAY',
            'remarks': 'OSC_SCALPER'
        }
        
        # Place order
        response = api.place_order(**order_params)
        
        if response is None:
            logger.error("Order placement failed with None response.")
            return None
            
        logger.debug(f"Order response: {response}")
        
        if isinstance(response, dict) and response.get('stat') == 'Ok':
            return response
        else:
            logger.error(f"Order placement failed. Response: {response}")
            return None
            
    except Exception as e:
        logger.error(f"Order placement error: {str(e)}")
        return None

def main():
    """Main function to start all components"""
    try:
        global processing_data
        
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