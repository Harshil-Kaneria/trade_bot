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
from queue import Queue  # Added missing import
import pyotp

parser = argparse.ArgumentParser()
parser.add_argument("--broker", type=str,default="F", help="Value for broker is (SHOONYA/FLATTRADE) so S/F")
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
broker = args.broker

if(broker=='F'):
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
    print("FLATTRADE")
else:
    uid    = os.getenv("SHOONYA_UID")
    pwd     = os.getenv("SHOONYA_PASSWORD")
    factor2 = pyotp.TOTP(os.getenv("SHOONYA_TOTP")).now()
    vc      = os.getenv("SHOONYA_VENDOR_CODE")
    app_key = os.getenv("SHOONYA_API_KEY")
    imei    = os.getenv("SHOONYA_IMEI")
    api = ShoonyaApiPy()
    ret = api.login(userid=uid, password=pwd, twoFA=factor2, vendor_code=vc, api_secret=app_key, imei=imei)
    if ret is None:
        logger.error("Failed to set session. Check credentials.")
        exit()
    print("SHOONYA")


# Trading Configuration
CAPITAL = 90000  # ₹1,00,000
TARGET_PROFIT = 180  # ₹180 per trade
MAX_POSITION_DURATION = 300  # 5 minutes in seconds
RISK_REWARD_RATIO = 1.5  # 1:1.5 risk:reward
MIN_OSCILLATION_RANGE_PCT = 0.05  # Minimum 0.05% price range for oscillation
MAX_OSCILLATION_RANGE_PCT = 0.3  # Maximum 0.3% price range for oscillation
MIN_REVERSALS = 3  # Minimum number of reversals in observation window
OBSERVATION_WINDOW = 30  # Number of ticks to observe for oscillation patterns
MIN_LIQUIDITY = 10000  # Minimum average volume in last 5 minutes
MAX_CONCURRENT_SUBS = 20  # Maximum stocks to monitor simultaneously

# Global trackers
active_position = False
position_lock = Lock()
processing_data = True
trade_signal_queue = Queue()
price_history = {}  # {token: deque of prices)}
reversal_counts = {}  # {token: count of recent reversals)}

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
    price_history[str(token)] = deque(maxlen=OBSERVATION_WINDOW)
    reversal_counts[str(token)] = 0

token_to_index = {str(token): idx for idx, token in enumerate(df['instrument_token'])}
tokens = df['instrument_token'].astype(str).tolist()

def on_ticks(tick_data):
    """Process incoming ticks and update price history"""
    try:
        if tick_data.get('t') == 'tf' or tick_data.get('t') == 'tk':
            token = tick_data.get('tk')
            price = float(tick_data.get('lp', 0))
            volume = int(tick_data.get('v', 0))
            
            if token in price_history:
                # Detect reversals by comparing with previous price
                if len(price_history[token]) >= 1:
                    prev_price = price_history[token][-1]
                    current_trend = 1 if price > prev_price else (-1 if price < prev_price else 0)
                    
                    if len(price_history[token]) >= 2:
                        prev_trend = 1 if price_history[token][-1] > price_history[token][-2] else (
                            -1 if price_history[token][-1] < price_history[token][-2] else 0)
                        if current_trend != 0 and prev_trend != 0 and current_trend != prev_trend:
                            reversal_counts[token] += 1
                
                price_history[token].append(price)
    except Exception as e:
        logger.error(f"Error processing tick: {e}")

def on_connect():
    """WebSocket connected callback"""
    logger.info("✅ WebSocket Connected")
    try:
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

def calculate_oscillation_metrics(token):
    """Calculate oscillation metrics for a given stock"""
    prices = list(price_history.get(token, []))
    if len(prices) < OBSERVATION_WINDOW:
        return None
    
    prices = np.array(prices)
    price_range = prices.max() - prices.min()
    avg_price = prices.mean()
    
    if avg_price == 0:
        return None
    
    range_pct = (price_range / avg_price) * 100
    reversal_count = reversal_counts.get(token, 0)
    
    return {
        'range_pct': range_pct,
        'reversal_count': reversal_count,
        'avg_price': avg_price,
        'current_price': prices[-1],
        'support': prices.min(),
        'resistance': prices.max()
    }

def find_oscillation_opportunities():
    """Identify stocks with optimal oscillation patterns"""
    opportunities = []
    
    for token in price_history:
        metrics = calculate_oscillation_metrics(token)
        if not metrics:
            continue
        
        # Criteria for good oscillation candidate
        if (MIN_OSCILLATION_RANGE_PCT <= metrics['range_pct'] <= MAX_OSCILLATION_RANGE_PCT and
            metrics['reversal_count'] >= MIN_REVERSALS):
            
            idx = token_to_index.get(token)
            if idx is not None:
                symbol = df.at[idx, 'tradingsymbol']
                opportunities.append({
                    'symbol': symbol,
                    'token': token,
                    'current_price': metrics['current_price'],
                    'support': metrics['support'],
                    'resistance': metrics['resistance'],
                    'range_pct': metrics['range_pct'],
                    'reversal_count': metrics['reversal_count']
                })
    
    # Sort by most reversals in the tightest range
    opportunities.sort(key=lambda x: (x['reversal_count'], -x['range_pct']), reverse=True)
    return opportunities

def determine_trade_parameters(opportunity):
    """Calculate trade parameters for an oscillation opportunity"""
    price = opportunity['current_price']
    support = opportunity['support']
    resistance = opportunity['resistance']
    
    # Calculate quantity based on risk per trade
    risk_per_trade = CAPITAL * 0.001  # 0.1% of capital
    stop_loss = support if (price - support) < (resistance - price) else resistance
    risk_per_share = abs(price - stop_loss)
    
    if risk_per_share == 0:
        return None
    
    quantity = int(risk_per_trade / risk_per_share)
    quantity = max(1, min(quantity, int((CAPITAL * 0.1) / price)))  # Limit to 10% of capital
    
    # Determine direction based on position relative to range
    range_mid = (support + resistance) / 2
    if price < range_mid:
        direction = 'BUY'
        target = price + (TARGET_PROFIT / quantity)
        stop_loss = support - (0.1 * (resistance - support))  # Slightly below support
    else:
        direction = 'SELL'
        target = price - (TARGET_PROFIT / quantity)
        stop_loss = resistance + (0.1 * (resistance - support))  # Slightly above resistance
    
    return {
        'symbol': opportunity['symbol'],
        'token': opportunity['token'],
        'direction': direction,
        'price': price,
        'quantity': quantity,
        'target': target,
        'stop_loss': stop_loss,
        'range_pct': opportunity['range_pct'],
        'reversal_count': opportunity['reversal_count']
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
            
            # Take the top opportunity
            best_opportunity = opportunities[0]
            trade_params = determine_trade_parameters(best_opportunity)
            
            if trade_params:
                logger.info(f"Found oscillation opportunity: {trade_params['symbol']} | "
                           f"Range: {trade_params['range_pct']:.2f}% | "
                           f"Reversals: {trade_params['reversal_count']}")
                
                # Put trade signal in queue
                trade_signal_queue.put(trade_params)
                
                # Wait before next scan
                time.sleep(5)
            else:
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"Error in stock picker: {e}")
            time.sleep(5)

def position_manager():
    """Manage trade execution and monitoring"""
    global active_position
    
    current_trade = None
    entry_time = None
    
    while processing_data:
        try:
            # Check for new signals if no active position
            if not active_position and not trade_signal_queue.empty():
                current_trade = trade_signal_queue.get()
                logger.info(f"Executing trade: {current_trade}")
                
                # Place order
                order_result = place_order(
                    symbol=current_trade['symbol'],
                    token=current_trade['token'],
                    direction=current_trade['direction'],
                    quantity=current_trade['quantity'],
                    price=current_trade['price']
                )
                
                if order_result and order_result.get('stat') == 'Ok':
                    current_trade['order_id'] = order_result['norenordno']
                    current_trade['entry_price'] = float(order_result['avgprc'])
                    entry_time = datetime.now()
                    active_position = True
                    logger.info(f"Trade executed: {current_trade}")
                else:
                    logger.error(f"Order failed: {order_result}")
                    current_trade = None
                    continue
            
            # Monitor active position
            if active_position and current_trade:
                # Check if position should be closed
                current_time = datetime.now()
                position_duration = (current_time - entry_time).total_seconds()
                
                # Get current price
                idx = token_to_index.get(current_trade['token'])
                current_price = df.at[idx, 'last_price'] if idx is not None else None
                
                if current_price is None:
                    time.sleep(1)
                    continue
                
                # Check exit conditions
                exit_condition = None
                if current_trade['direction'] == 'BUY':
                    if current_price >= current_trade['target']:
                        exit_condition = 'TARGET_HIT'
                    elif current_price <= current_trade['stop_loss']:
                        exit_condition = 'STOP_LOSS'
                else:  # SELL
                    if current_price <= current_trade['target']:
                        exit_condition = 'TARGET_HIT'
                    elif current_price >= current_trade['stop_loss']:
                        exit_condition = 'STOP_LOSS'
                
                # Check timeout
                if position_duration > MAX_POSITION_DURATION:
                    exit_condition = 'TIMEOUT'
                
                # Exit if any condition met
                if exit_condition:
                    logger.info(f"Exit condition met: {exit_condition}")
                    
                    # Place exit order
                    exit_direction = 'SELL' if current_trade['direction'] == 'BUY' else 'BUY'
                    exit_result = place_order(
                        symbol=current_trade['symbol'],
                        token=current_trade['token'],
                        direction=exit_direction,
                        quantity=current_trade['quantity'],
                        price=0  # Market order
                    )
                    
                    if exit_result and exit_result.get('stat') == 'Ok':
                        # Calculate P&L
                        exit_price = float(exit_result['avgprc'])
                        if current_trade['direction'] == 'BUY':
                            pnl = (exit_price - current_trade['entry_price']) * current_trade['quantity']
                        else:
                            pnl = (current_trade['entry_price'] - exit_price) * current_trade['quantity']
                        
                        logger.info(f"Position closed. P&L: ₹{pnl:.2f}")
                        
                        # Reset position
                        current_trade = None
                        active_position = False
                        entry_time = None
                    else:
                        logger.error(f"Exit order failed: {exit_result}")
            
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error in position manager: {e}")
            time.sleep(5)

def place_order(symbol, token, direction, quantity, price=0):
    """Place order using Flattrade API with correct parameters"""
    try:
        # Convert numpy float to native Python float if needed
        if hasattr(price, 'item'):  # Check if it's numpy type
            price = price.item()
        
        # Prepare order parameters according to Flattrade API
        order_params = {
            'buy_or_sell': 'B' if direction == 'BUY' else 'S',
            'product_type': 'I',  # Margin Intraday Square-off
            'exchange': 'NSE',
            'tradingsymbol': symbol,
            'quantity': str(int(quantity)),  # Ensure quantity is integer string
            'price': str(float(price)) if price > 0 else '0',  # Price as string
            'trigger_price': '0',  # No trigger price for regular orders
            'discloseqty': '0',  # No disclosed quantity
            'ordertype': 'L' if price > 0 else 'MKT',  # LIMIT or MARKET
            'retention': 'DAY',  # Order valid for the day
            'remarks': 'OSC_SCALPER'  # Order tag
        }
        
        logger.debug(f"Placing order with params: {order_params}")
        response = api.place_order(order_params)
        
        if response and isinstance(response, dict) and response.get('stat') == 'Ok':
            return response
        else:
            logger.error(f"Order placement failed. Response: {response}")
            return None
            
    except Exception as e:
        logger.error(f"Order placement error: {str(e)}", exc_info=True)
        return None

def main():
    """Main function to start all components"""
    try:
        global processing_data
        # Start WebSocket
        api.start_websocket(
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
        unsubscribe_tokens = [f"NSE|{token}" for token in tokens]
        api.unsubscribe(unsubscribe_tokens)
        time.sleep(1)
        exit()

if __name__ == "__main__":
    main()