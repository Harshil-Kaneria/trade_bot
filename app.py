import time
import datetime
import mysql.connector
from kite_trade import *
from kiteconnect import KiteTicker
import talib
import numpy as np
from flask import Flask, render_template
from flask_socketio import SocketIO, emit

# Flask app for displaying trades and profits
app = Flask(__name__)
socketio = SocketIO(app)

# Database connection
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='',
    database='trading_bot'
)
cursor = conn.cursor()

# Initialize Kite API
enctoken = "57JyY5GV7YH8QkJwrPW8YiizYIaZqdWDEXrX+PkoDYju3CcAzFUk/R9E6ZqwU6gBPtfxcrBm1P2YwgG1zxLwdi6BVqzvEJXkQEJVSLBa4JBPocmyYlKDVg=="
kite = KiteApp(enctoken=enctoken)

# Create tables if they don't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS trades (
        id INT AUTO_INCREMENT PRIMARY KEY,
        stock_symbol VARCHAR(20),
        buy_price FLOAT,
        sell_price FLOAT,
        quantity INT,
        buy_time DATETIME,
        sell_time DATETIME,
        profit FLOAT
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS portfolio (
        id INT PRIMARY KEY,
        capital FLOAT,
        invested_amount FLOAT
    )
''')

# Initialize portfolio if not exists
cursor.execute('SELECT * FROM portfolio WHERE id = 1')
if cursor.fetchone() is None:
    cursor.execute('INSERT INTO portfolio (id, capital, invested_amount) VALUES (1, 100000, 0)')
    conn.commit()

# Fetch capital and invested amount from DB
cursor.execute('SELECT capital, invested_amount FROM portfolio WHERE id = 1')
capital, invested_amount = cursor.fetchone()
current_profit = 0
quantity_data = 0
book_profit = 2
reenter_threshold = 1
last_sell_price = None
waiting_for_reentry = False

# User input for stock token
instrument_token = 291849

def update_invested_amount(amount):
    cursor.execute('UPDATE portfolio SET invested_amount = %s WHERE id = 1', (amount,))
    conn.commit()

def check_open_positions():
    global holding_stock, current_trade_id, buy_price, quantity_data, invested_amount

    cursor.execute('SELECT id, stock_symbol, buy_price, quantity FROM trades WHERE sell_price IS NULL')
    open_trades = cursor.fetchall()

    if open_trades:
        for trade in open_trades:
            trade_id, stock_symbol, buy_price_db, quantity = trade
            current_trade_id = trade_id
            buy_price = buy_price_db
            quantity_data = quantity
            holding_stock = True
            invested_amount += buy_price * quantity  # Update invested amount
            print(f"Resuming open position for {stock_symbol}: Bought at ₹{buy_price_db}, Quantity: {quantity}")
    else:
        print("No open positions found.")

# Simulate buying stocks
def buy_stock(stock, price, quantity):
    global invested_amount, quantity_data
    cursor.execute('''INSERT INTO trades (stock_symbol, buy_price, quantity, buy_time) VALUES (%s, %s, %s, NOW())''',
                   (stock, price, quantity))
    conn.commit()
    trade_id = cursor.lastrowid
    invested_amount += price * quantity
    update_invested_amount(invested_amount)
    quantity_data = quantity
    socketio.emit('trade_event', {'event': 'Buy', 'message': f'Bought {stock} at ₹{price}'})
    return trade_id

# Simulate selling stocks
def sell_stock(stock_id, price):
    global current_profit, invested_amount, quantity_data
    cursor.execute('SELECT buy_price, quantity FROM trades WHERE id = %s AND sell_price IS NULL', (stock_id,))
    trade = cursor.fetchone()
    if trade:
        buy_price, quantity = trade
        profit = (price - buy_price) * quantity
        current_profit += profit
        invested_amount -= buy_price * quantity
        update_invested_amount(invested_amount)
        cursor.execute('''UPDATE trades SET sell_price = %s, sell_time = NOW(), profit = %s WHERE id = %s''', (price, profit, stock_id))
        conn.commit()
        socketio.emit('trade_event', {'event': 'Sell', 'message': f'Sold stock ID {stock_id} for profit of ₹{profit}'})

# WebSocket for live data
user_id = kite.profile()["user_id"]
kws = KiteTicker(api_key="your_api_key", access_token=enctoken + "&user_id=" + user_id)

buy_price = None
holding_stock = False
current_trade_id = None

# Handle WebSocket ticks
def on_ticks(ws, ticks):
    global buy_price, holding_stock, current_profit, current_trade_id, quantity_data, last_sell_price, waiting_for_reentry, invested_amount
    for tick in ticks:
        socketio.emit('price_tick', {
            'event': 'tick',
            'data': tick
        })

        price = tick['last_price']
        symbol = tick['instrument_token']

        print(f"Live Data: {price} for Token: {symbol}")

        if not holding_stock and not waiting_for_reentry:
            quantity = int((capital - invested_amount) // price) or 1  # Prevent division by zero
            current_trade_id = buy_stock(symbol, price, quantity)
            buy_price = price
            holding_stock = True

        elif holding_stock:
            # Check if overall profit has reached the target
            if (price * quantity_data) >= (book_profit + (quantity_data * buy_price)):
                sell_stock(current_trade_id, price)
                holding_stock = False
                last_sell_price = price  # Set the last sell price
                waiting_for_reentry = True  # Start waiting for a drop
                socketio.emit('trade_event', {
                    'event': 'Profit',
                    'message': f'Total profit of ₹{current_profit} booked.'
                })
                ws.close()

        elif waiting_for_reentry:
            if (price * quantity_data) <= ((last_sell_price * quantity_data) - reenter_threshold):
                quantity = int((capital - invested_amount) // price) or 1
                current_trade_id = buy_stock(symbol, price, quantity)
                buy_price = price
                holding_stock = True
                waiting_for_reentry = False  # Reset waiting flag

def on_connect(ws, response):
    print("WebSocket: Connected")
    ws.subscribe([instrument_token])
    ws.set_mode(ws.MODE_QUOTE, [instrument_token])

def on_close(ws, code, reason):
    print(f"WebSocket closed with code: {code}, reason: {reason}")

# Attach events
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

# Start WebSocket in a background thread
@socketio.on('connect')
def start_trading():
    if not kws.is_connected():
        kws.connect(threaded=True)

# Flask Routes
@app.route('/')
def index():
    cursor.execute('SELECT * FROM trades')
    trades = cursor.fetchall()
    return render_template('index.html', trades=trades)

if __name__ == '__main__':
    check_open_positions()
    socketio.run(app, port=5000)

# Close connection on shutdown
conn.close()
print("Profit target achieved, trading session ended.")
