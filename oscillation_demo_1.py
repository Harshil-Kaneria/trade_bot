import os
import time
import pandas as pd
from kite_trade import *
from kiteconnect import KiteTicker
from datetime import datetime

# Zerodha API Key and Token Setup
enctoken = os.getenv("ENCTOKEN")
kite = KiteApp(enctoken=enctoken)

# Global variables
capital = 100000  # Initial capital
target_profit = 3000  # Target profit in INR after 3 hours
quantity = 0  # Number of stocks per trade (calculated dynamically based on stock price)
holding_stock = False
buy_price = 0
sell_price = 0
total_profit = 0
last_order_time = None  # Track last order time
last_trade_time = None  # Track the time of the last trade
order_time_interval = 6 * 60  # 6 minutes in seconds (time between orders)
max_trade_time = 5 * 60  # 5 minutes maximum holding time for each trade
total_trades = 0  # Counter for total trades in 3 hours

# Function to calculate the number of stocks to buy based on capital
def calculate_quantity(stock_price):
    return int(capital // stock_price)  # Number of shares you can buy with ₹1,00,000

# Function to place buy/sell orders
def place_order(isBuy=True, price=None, quantity=1):
    global holding_stock, buy_price, sell_price, total_profit, last_trade_time, total_trades
    
    if isBuy:
        print(f"Placing Buy Order: {quantity} stocks at {price}")
        order_data = kite.place_order(variety=kite.VARIETY_REGULAR,
                                      exchange=kite.EXCHANGE_NSE,
                                      tradingsymbol=trading_symbol,
                                      transaction_type=kite.TRANSACTION_TYPE_BUY,
                                      quantity=quantity,
                                      product=kite.PRODUCT_CNC,
                                      order_type=kite.ORDER_TYPE_LIMIT,
                                      price=price,
                                      tag="Oscillation_Trade_Buy")
        holding_stock = True
        buy_price = price
    else:
        print(f"Placing Sell Order: {quantity} stocks at {price}")
        order_data = kite.place_order(variety=kite.VARIETY_REGULAR,
                                      exchange=kite.EXCHANGE_NSE,
                                      tradingsymbol=trading_symbol,
                                      transaction_type=kite.TRANSACTION_TYPE_SELL,
                                      quantity=quantity,
                                      product=kite.PRODUCT_CNC,
                                      order_type=kite.ORDER_TYPE_LIMIT,
                                      price=price,
                                      tag="Oscillation_Trade_Sell")
        holding_stock = False
        sell_price = price
        profit = (sell_price - buy_price) * quantity
        total_profit += profit
        total_trades += 1
        print(f"Trade completed. Profit: ₹{profit}, Total Profit: ₹{total_profit}")
        last_trade_time = datetime.now()

    return True

# Price Oscillation Detection and Buy/Sell Logic
def detect_oscillation_and_trade(stock_prices):
    """ Detect oscillation in stock prices and decide to buy or sell """
    global holding_stock, buy_price, sell_price

    # Detect price oscillation (using min and max within the last 10 prices)
    min_price = min(stock_prices)
    max_price = max(stock_prices)

    # If no position is held, buy when the price hits the lower bound
    if not holding_stock and stock_prices[-1] == min_price:  # Buy signal at the bottom
        buy_price = min_price
        print(f"Oscillation Buy Signal: Buying at {buy_price}")
        quantity = calculate_quantity(buy_price)  # Calculate number of stocks for ₹1,00,000
        place_order(isBuy=True, price=buy_price, quantity=quantity)
    
    # If a position is held, sell when the price hits the upper bound
    elif holding_stock and stock_prices[-1] == max_price:  # Sell signal at the top
        sell_price = max_price
        print(f"Oscillation Sell Signal: Selling at {sell_price}")
        quantity = calculate_quantity(sell_price)  # Calculate number of stocks for ₹1,00,000
        place_order(isBuy=False, price=sell_price, quantity=quantity)

# WebSocket for real-time price monitoring
def on_ticks(ws, ticks):
    """ Handle live stock price updates """
    stock_prices = []

    # Collect recent stock prices (last 10 prices)
    for tick in ticks:
        price = tick['last_price']
        stock_prices.append(price)
        
        if len(stock_prices) > 10:  # Keep only last 10 prices for detection
            stock_prices.pop(0)

    # Check for oscillation and trade based on strategy
    if len(stock_prices) > 2:  # Ensure we have enough data for oscillation
        detect_oscillation_and_trade(stock_prices)

    # If no profit after 5 minutes, close the position
    if holding_stock and (datetime.now() - last_trade_time).seconds > max_trade_time:
        print("No profit after 5 minutes. Closing position.")
        quantity = calculate_quantity(buy_price)  # Calculate number of stocks
        place_order(isBuy=False, price=stock_prices[-1], quantity=quantity)

    # Check if total profit reached target and stop trading if necessary
    if total_profit >= target_profit or total_trades >= 30:
        print("Target profit or trade limit reached. Stopping trading.")
        ws.close()  # Close the WebSocket to stop trading

def on_connect(ws, response):
    """ WebSocket connection """
    print("WebSocket: Connected")
    ws.subscribe([instrument_token])
    ws.set_mode(ws.MODE_FULL, [instrument_token])

def on_close(ws, code, reason):
    """ WebSocket closed """
    print(f"WebSocket closed with code: {code}, reason: {reason}")
    time.sleep(5)

# Start WebSocket to monitor live stock prices
def start_trading():
    if not kws.is_connected():
        kws.connect(threaded=True)

# Set up WebSocket for real-time data monitoring
kws = KiteTicker(api_key="your_api_key", access_token=enctoken + "&user_id=" + user_id)
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

# Initialize trading by connecting to WebSocket
kws.connect(threaded=True)
