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
enctoken = "WyuFDOWBQHm5GIm0VbVXs0cOfTEsjeWGWStpLomSxMFdOLqzHXkfkLN/5kTvtz9FqLTDxV8teF4T6bk8DtKE6X9mvNbAsotl33hCrh26opprWVchlkFRhA=="
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
        sell_time DATETIME
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS profits (
        id INT AUTO_INCREMENT PRIMARY KEY,
        profit FLOAT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
''')

# Portfolio
capital = 100000
invest_amount = 90000
current_profit = 0
quantity_data = 0
book_profit = 2
reenter_threshold = 1
last_sell_price = None
waiting_for_reentry = False

# User input for stock token
instrument_token = 291849

# Simulate buying stocks
def buy_stock(stock, price, quantity):
    global invest_amount
    global quantity_data
    cursor.execute('''INSERT INTO trades (stock_symbol, buy_price, quantity, buy_time) VALUES (%s, %s, %s, NOW())''',
                   (stock, price, quantity))
    conn.commit()
    trade_id = cursor.lastrowid
    invest_amount -= price * quantity
    quantity_data = quantity
    socketio.emit('trade_event', {'event': 'Buy', 'message': f'Bought {stock} at ₹{price}'})
    return trade_id

# Simulate selling stocks
def sell_stock(stock_id, price):
    global current_profit
    global invest_amount
    global quantity_data
    cursor.execute('SELECT buy_price, quantity FROM trades WHERE id = %s AND sell_price IS NULL', (stock_id,))
    trade = cursor.fetchone()
    if trade:
        buy_price, quantity = trade
        profit = (price - buy_price) * quantity
        current_profit += profit
        invest_amount = invest_amount+(quantity*price)
        cursor.execute('''UPDATE trades SET sell_price = %s, sell_time = NOW() WHERE id = %s''', (price, stock_id))
        cursor.execute('''INSERT INTO profits (profit) VALUES (%s)''', (profit,))
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
    global buy_price, holding_stock, current_profit, current_trade_id, quantity_data, last_sell_price, waiting_for_reentry
    for tick in ticks:
        # print(f"tick {tick}")

        socketio.emit('buy_event', {
            'event': 'tick',
            'data': tick
        })

        price = tick['last_price']
        symbol = tick['instrument_token']

        print(f"Live Data: {price} for Token: {symbol}")

        if not holding_stock and not waiting_for_reentry:
            quantity = int(invest_amount // price) or 1  # Prevent division by zero
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

        # Wait for price to drop by the threshold before buying again
        elif waiting_for_reentry:
            if (price*quantity_data) <= ((last_sell_price*quantity_data) - reenter_threshold):
                quantity = int(invest_amount // price) or 1
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
    cursor.execute('SELECT * FROM profits')
    profits = cursor.fetchall()
    return render_template('index.html', trades=trades, profits=profits)

if __name__ == '__main__':
    socketio.run(app, port=5000)

# Close connection on shutdown
conn.close()
print("Profit target achieved, trading session ended.")