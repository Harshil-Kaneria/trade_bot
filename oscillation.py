import os
from dotenv import load_dotenv
import time
import numpy as np
from datetime import datetime
from kite_trade import *
from kiteconnect import KiteTicker

load_dotenv(override=True)

# Initialize Kite API
enctoken = os.getenv("ENCTOKEN")
kite = KiteApp(enctoken=enctoken)

# Configuration
CAPITAL = 100000  # ₹1,00,000 capital
TARGET_PROFIT = 180  # ₹180 profit per trade
MAX_ORDER_TIME = 300  # 5 minutes (exit trade no matter what)
RISK_PER_TRADE = 0.01  # 1% risk per trade (~₹1000)

# Trackers
active_order = None
trades_completed = 0
total_profit = 0
price_history = {}  # Stores last 20 prices for each symbol

# Get liquid Nifty 500 stocks
def get_nifty500_symbols():
    instruments = kite.instruments("NSE")
    return [inst['tradingsymbol'] for inst in instruments if inst['tradingsymbol'].endswith('EQ')][:500]

# Calculate quantity (uses full capital)
def calculate_quantity(price):
    quantity = int(CAPITAL / price)  # Uses full ₹1,00,000
    return max(1, quantity)  # At least 1 share

# Detect oscillation pattern (100→105→100→102→100→103)
def is_oscillating(prices):
    if len(prices) < 10:
        return False
    
    # Calculate peaks and troughs
    peaks = []
    troughs = []
    
    for i in range(1, len(prices)-1):
        if prices[i] > prices[i-1] and prices[i] > prices[i+1]:
            peaks.append(prices[i])
        elif prices[i] < prices[i-1] and prices[i] < prices[i+1]:
            troughs.append(prices[i])
    
    # Need at least 3 peaks and 3 troughs
    if len(peaks) < 3 or len(troughs) < 3:
        return False
    
    # Check if peaks are increasing and troughs are stable
    avg_trough = np.mean(troughs)
    trough_range = max(troughs) - min(troughs)
    
    # Good oscillation if:
    # 1) Troughs stay within 1.5% range
    # 2) Peaks are generally rising
    return (trough_range/avg_trough < 0.015 and 
            np.all(np.diff(peaks) > 0))

# Find best entry point in oscillation
def find_entry_point(prices):
    if len(prices) < 10:
        return False
    
    current_price = prices[-1]
    last_trough = min(prices[-5:-1])  # Last 4 prices
    
    # Buy when price drops near recent trough
    return current_price <= last_trough * 1.005  # Within 0.5% of trough

# Place BUY and SELL orders (LIMIT)
def place_scalp_order(symbol):
    global active_order
    
    ltp = kite.ltp(f"NSE:{symbol}")[f"NSE:{symbol}"]["last_price"]
    quantity = calculate_quantity(ltp)
    
    # Buy at 0.2% below current (better fill)
    buy_price = round(ltp * 0.998, 2)
    sell_price = round(buy_price + (TARGET_PROFIT / quantity), 2)
    
    print(f"📈 Placing Order: {symbol} | Buy {quantity} @ ₹{buy_price} | Sell @ ₹{sell_price}")
    
    try:
        # Place BUY order (LIMIT)
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
        
        # Place SELL order (LIMIT)
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
            "buy_order_id": buy_order["order_id"],
            "sell_order_id": sell_order["order_id"],
            "quantity": quantity,
            "buy_price": buy_price,
            "sell_price": sell_price,
            "order_time": datetime.now()
        }
        return True
    
    except Exception as e:
        print(f"❌ Order Failed: {e}")
        return False

# Manage active orders (force exit after 5 mins)
def manage_orders():
    global active_order, trades_completed, total_profit
    
    if not active_order:
        return
    
    current_time = datetime.now()
    order_age = (current_time - active_order["order_time"]).total_seconds()
    
    # Cancel if older than 5 minutes (force exit)
    if order_age > MAX_ORDER_TIME:
        print(f"⏳ Order Timeout (5 mins), Exiting Trade...")
        try:
            # Cancel pending orders
            kite.cancel_order(variety="regular", order_id=active_order["buy_order_id"])
            kite.cancel_order(variety="regular", order_id=active_order["sell_order_id"])
            
            # Get current position
            positions = kite.positions()
            for pos in positions["net"]:
                if pos["tradingsymbol"] == active_order["symbol"] and pos["quantity"] != 0:
                    # Square off at market price
                    squareoff_order = kite.place_order(
                        variety="regular",
                        exchange="NSE",
                        tradingsymbol=active_order["symbol"],
                        transaction_type="SELL",
                        quantity=abs(pos["quantity"]),
                        product="MIS",
                        order_type="MARKET",
                        validity="DAY"
                    )
                    print(f"🔴 Force Exited Trade (Market Order)")
                    break
        except Exception as e:
            print(f"⚠️ Error exiting trade: {e}")
        
        active_order = None
        return
    
    # Check if order completed
    try:
        buy_status = kite.order_history(active_order["buy_order_id"])[-1]["status"]
        sell_status = kite.order_history(active_order["sell_order_id"])[-1]["status"]
        
        if buy_status == "COMPLETE" and sell_status == "COMPLETE":
            profit = (active_order["sell_price"] - active_order["buy_price"]) * active_order["quantity"]
            total_profit += profit
            trades_completed += 1
            print(f"✅ Trade Successful! Profit: ₹{profit} | Total Profit: ₹{total_profit}")
            active_order = None
        
        elif buy_status == "COMPLETE" and sell_status != "COMPLETE":
            # Modify SELL order if price moves favorably
            current_ltp = kite.ltp(f"NSE:{active_order['symbol']}")[f"NSE:{active_order['symbol']}"]["last_price"]
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
        print(f"⚠️ Error checking order: {e}")

user_id = kite.profile()["user_id"]
# Main trading loop
def run_bot():
    global active_order, trades_completed, price_history
    
    symbols = get_nifty500_symbols()
    print(f"Loaded {len(symbols)} symbols from Nifty 500")
    
    # Initialize KiteTicker for streaming data
    kws = KiteTicker(api_key="your_api_key", access_token=enctoken + "&user_id=" + user_id)
    
    # Initialize price history for first 50 symbols
    initialized = 0
    instrument_tokens = []
    
    for symbol in symbols[:50]:  # Reduce initial monitoring set
        try:
            # Get instrument token for each symbol
            instruments = kite.instruments("NSE")
            instrument = next((i for i in instruments if i['tradingsymbol'] == symbol), None)
            
            if instrument:
                instrument_token = instrument['instrument_token']
                instrument_tokens.append(instrument_token)
                
                # Get initial LTP using quote API
                quote = kite.quote(f"NSE:{symbol}")
                ltp = quote[f"NSE:{symbol}"]["last_price"]
                price_history[symbol] = [ltp] * OSCILLATION_PERIOD
                initialized += 1
                print(f"Initialized {symbol} @ ₹{ltp}")
        except Exception as e:
            print(f"Failed to initialize {symbol}: {str(e)}")
            continue
    
    print(f"🚀 Bot Started | Tracking {initialized} symbols | Capital: ₹1,00,000")
    
    # WebSocket callbacks
    def on_ticks(ws, ticks):
        for tick in ticks:
            symbol = next((s for s in price_history.keys() 
                         if kite.instruments("NSE", s)[0]['instrument_token'] == tick['instrument_token']), None)
            if symbol:
                price_history[symbol].append(tick['last_price'])
                price_history[symbol] = price_history[symbol][-OSCILLATION_PERIOD:]
    
    def on_connect(ws, response):
        ws.subscribe(instrument_tokens)
        ws.set_mode(ws.MODE_LTP, instrument_tokens)
    
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.connect(threaded=True)
    
    iteration = 0
    while True:
        iteration += 1
        print(f"\n=== Iteration {iteration} ===")
        
        # Trading logic
        if active_order is None:
            print("Checking for trading opportunities...")
            for symbol, prices in price_history.items():
                try:
                    if len(prices) >= OSCILLATION_PERIOD and is_oscillating(prices) and find_entry_point(prices):
                        print(f"Found opportunity in {symbol}")
                        if place_scalp_order(symbol):
                            break
                except Exception as e:
                    print(f"Error evaluating {symbol}: {str(e)}")
                    continue
        
        # Order management
        manage_orders()
        
        # Status update
        print(f"Active orders: {1 if active_order else 0}")
        print(f"Total trades: {trades_completed} | Profit: ₹{total_profit}")
        
        time.sleep(5)  # 5 second delay between iterations

if __name__ == "__main__":
    run_bot()