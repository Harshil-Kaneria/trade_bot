import time
import datetime
import mysql.connector
from kite_trade import *
from kiteconnect import KiteTicker
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
enctoken = ""
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
book_profit = 250
reenter_threshold = 100
last_sell_price = None
waiting_for_reentry = False
holding_stock = False
current_trade_id = None
buy_price = None

# User input for stock token
instrument_token = 1304833   

def rec_db():
    """ Fetch trade history and capital, return JSON-friendly data """
    cursor.execute('SELECT * FROM trades ORDER BY id DESC')
    trades = cursor.fetchall()

    cursor.execute('SELECT capital, invested_amount FROM portfolio WHERE id = 1')
    capital, invested_amount = cursor.fetchone()

    # Convert trades data into a list of dictionaries
    trades_list = []
    for trade in trades:
        trades_list.append({
            "id": trade[0],
            "stock_symbol": trade[1],
            "buy_price": trade[2],
            "sell_price": trade[3],
            "quantity": trade[4],
            "buy_time": str(trade[5]),  # Convert datetime to string
            "sell_time": str(trade[6]) if trade[6] else None,
            "profit": trade[7]
        })

    return {"trades": trades_list, "capital": capital,"invested_amount":invested_amount}

def update_invested_amount(amount):
    """ Update invested amount in DB """
    cursor.execute('UPDATE portfolio SET invested_amount = %s WHERE id = 1', (amount,))
    conn.commit()


def update_capital(amount):
    """ Update capital in DB """
    cursor.execute('UPDATE portfolio SET capital = %s WHERE id = 1', (amount,))
    conn.commit()


def recalculate_invested_amount():
    """ Recalculate invested amount based on open trades in DB """
    global invested_amount
    cursor.execute('SELECT SUM(buy_price * quantity) FROM trades WHERE sell_price IS NULL')
    result = cursor.fetchone()
    invested_amount = result[0] if result[0] is not None else 0
    update_invested_amount(invested_amount)


def check_open_positions():
    """ Fetch open trades from DB and restore state """
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
    """ Execute a buy order and update database """
    global invested_amount, quantity_data, capital, holding_stock

    # Fetch updated capital before buying
    cursor.execute('SELECT capital FROM portfolio WHERE id = 1')
    capital = cursor.fetchone()[0]

    cursor.execute('''INSERT INTO trades (stock_symbol, buy_price, quantity, buy_time) VALUES (%s, %s, %s, NOW())''',
                   (stock, price, quantity))
    conn.commit()
    trade_id = cursor.lastrowid
    invested_amount += price * quantity
    update_invested_amount(invested_amount)
    quantity_data = quantity
    holding_stock = True

    socketio.emit('trade_event', {'event': 'Buy', 'message': f'Bought {stock} at ₹{price}','data':rec_db()})
    return trade_id


# Simulate selling stocks
def sell_stock(stock_id, price):
    """ Execute a sell order and update database """
    global current_profit, invested_amount, capital, holding_stock, last_sell_price, waiting_for_reentry

    cursor.execute('SELECT buy_price, quantity FROM trades WHERE id = %s AND sell_price IS NULL', (stock_id,))
    trade = cursor.fetchone()
    if trade:
        buy_price, quantity = trade
        profit = (price - buy_price) * quantity
        current_profit += profit
        invested_amount -= buy_price * quantity

        # Increase capital after selling
        capital += profit
        update_capital(capital)
        update_invested_amount(invested_amount)

        cursor.execute('''UPDATE trades SET sell_price = %s, sell_time = NOW(), profit = %s WHERE id = %s''',
                       (price, profit, stock_id))
        conn.commit()

        holding_stock = False
        last_sell_price = price
        waiting_for_reentry = True

        socketio.emit('trade_event', {'event': 'Sell', 'message': f'Sold stock ID {stock_id} for profit of ₹{profit}','data':rec_db()})


# WebSocket for live data
user_id = kite.profile()["user_id"]
kws = KiteTicker(api_key="your_api_key", access_token=enctoken + "&user_id=" + user_id)


def on_ticks(ws, ticks):
    """ Handle live stock price updates """
    global buy_price, holding_stock, current_trade_id, quantity_data, last_sell_price, waiting_for_reentry, invested_amount

    for tick in ticks:
        socketio.emit('price_tick', {'event': 'tick', 'data': tick})
        price = tick['last_price']
        symbol = tick['instrument_token']
        print(f"Live Data: {price} for Token: {symbol}")

        if not holding_stock and not waiting_for_reentry:
            quantity = int((capital - invested_amount) // price) or 1
            current_trade_id = buy_stock(symbol, price, quantity)
            buy_price = price

        elif holding_stock and (price * quantity_data) >= (book_profit + (quantity_data * buy_price)):
            sell_stock(current_trade_id, price)
            ws.close()

        elif waiting_for_reentry and (price * quantity_data) <= ((last_sell_price * quantity_data) - reenter_threshold):
            quantity = int((capital - invested_amount) // price) or 1
            current_trade_id = buy_stock(symbol, price, quantity)
            buy_price = price
            waiting_for_reentry = False


def on_connect(ws, response):
    print("WebSocket: Connected")
    ws.subscribe([instrument_token])
    ws.set_mode(ws.MODE_QUOTE, [instrument_token])


def on_close(ws, code, reason):
    print(f"WebSocket closed with code: {code}, reason: {reason}")


kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close


@socketio.on('connect')
def start_trading():
    if not kws.is_connected():
        kws.connect(threaded=True)


@app.route('/')
def index():
    cursor.execute('SELECT * FROM trades ORDER BY id DESC')
    trades = cursor.fetchall()
    cursor.execute('SELECT capital, invested_amount FROM portfolio WHERE id = 1')
    capital, invested_amount = cursor.fetchone()
    return render_template('index.html', trades=trades,capital=capital,invested_amount=invested_amount)


if __name__ == '__main__':
    check_open_positions()
    recalculate_invested_amount()
    socketio.run(app, port=5000)

conn.close()
