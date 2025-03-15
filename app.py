import os
from dotenv import load_dotenv
import time
import datetime
import mysql.connector
from kite_trade import *
from kiteconnect import KiteTicker
import numpy as np
import logging
from flask import Flask, render_template,request, jsonify
from flask_socketio import SocketIO, emit
import json
import subprocess

app = Flask(__name__)
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)
socketio = SocketIO(app,logger=False, engineio_logger=False)
load_dotenv(override=True)

conn = mysql.connector.connect(
    host=os.getenv("HOST"),
    user=os.getenv("USER"),
    password=os.getenv("PASSWORD"),
    database=os.getenv("DATABASE"),
)
cursor = conn.cursor()

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

cursor.execute('SELECT * FROM portfolio WHERE id = 1')
if cursor.fetchone() is None:
    cursor.execute('INSERT INTO portfolio (id, capital, invested_amount) VALUES (1, 50000, 0)')
    conn.commit()
cursor.execute('SELECT capital, invested_amount FROM portfolio WHERE id = 1')

capital, invested_amount = cursor.fetchone()
current_profit = 0
current_price = None
quantity_data = 0
book_profit = 250
book_loss = 500
reenter_threshold = 100
last_sell_price = None
waiting_for_reentry = False
holding_stock = False
current_trade_id = None
buy_price = None
user_buy_signal = False
user_sell_signal = False
user_auto_buy = False
user_stop_reentry = False
instrument_token = 3918849
trading_symbol = 'SEPC'
order_data = None

enctoken = os.getenv("ENCTOKEN")
kite = KiteApp(enctoken=enctoken)

def run_analyze():
    print("Running analyze.py...")
    subprocess.run(["py", "analyze.py"])

def get_analyze_data():
    try:
        run_analyze()
        with open("analyze_output.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print("No analysis file found. Run `run_analyze()` first.")
        return None

def auto_place_order(quantity=1,isBuy=True):
    global trading_symbol,order_data
    if isBuy:
        order_data = kite.place_order(variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=trading_symbol,
            transaction_type=kite.TRANSACTION_TYPE_BUY,
            quantity=quantity,
            product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_MARKET,
            price=None,
            validity=None,
            disclosed_quantity=None,
            trigger_price=None,
            squareoff=None,
            stoploss=None,
            trailing_stoploss=None,
            tag="HK"
        )
    elif order_data is not None:
        kite.place_order(variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=trading_symbol,
            transaction_type=kite.TRANSACTION_TYPE_SELL,
            quantity=quantity,
            product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_MARKET,
            price=None,
            validity=None,
            disclosed_quantity=None,
            trigger_price=None,
            squareoff=None,
            stoploss=None,
            trailing_stoploss=None,
            tag="HK"
        )
        order_data = None

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

def buy_stock(stock, price):
    """ Execute a buy order and update database """
    global invested_amount, quantity_data, capital, holding_stock,waiting_for_reentry,user_buy_signal,user_sell_signal,buy_price
    quantity = int((capital - invested_amount) // price) or 1
    
    auto_place_order(quantity,True)
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
    user_buy_signal = False
    user_sell_signal = False
    buy_price = price

    socketio.emit('trade_event', {'event': 'Buy', 'message': f'Bought {stock} at ₹{price}','data':rec_db()})
    return trade_id

def sell_stock(stock_id, price):
    """ Execute a sell order and update database """
    global current_profit, invested_amount, capital, holding_stock, last_sell_price, waiting_for_reentry,user_buy_signal,user_sell_signal
    cursor.execute('SELECT buy_price, quantity FROM trades WHERE id = %s AND sell_price IS NULL', (stock_id,))
    trade = cursor.fetchone()
    if trade:
        buy_price, quantity = trade
        auto_place_order(quantity,False)
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
        user_buy_signal = False
        user_sell_signal = False

        socketio.emit('trade_event', {'event': 'Sell', 'message': f'Sold stock ID {stock_id} for profit of ₹{profit}','data':rec_db()})

user_id = kite.profile()["user_id"]
kws = KiteTicker(api_key="your_api_key", access_token=enctoken + "&user_id=" + user_id)

def on_ticks(ws, ticks):
    """ Handle live stock price updates """
    global buy_price, holding_stock, current_trade_id, quantity_data, last_sell_price, waiting_for_reentry, invested_amount,user_sell_signal,user_buy_signal,user_auto_buy,user_stop_reentry,current_price

    for tick in ticks:
        price = tick['last_price']
        symbol = tick['instrument_token']

        if user_auto_buy and not holding_stock and (not waiting_for_reentry or user_buy_signal):
            current_trade_id = buy_stock(symbol, price)

        elif holding_stock and (((price * quantity_data) >= (book_profit + (quantity_data * buy_price))) or user_sell_signal or ((quantity_data * (buy_price - price)) >= book_loss)):
            sell_stock(current_trade_id, price)

        elif user_stop_reentry and waiting_for_reentry and (price * quantity_data) <= ((last_sell_price * quantity_data) - reenter_threshold):
            current_trade_id = buy_stock(symbol, price)
            waiting_for_reentry = False

        current_price = price
        print(f"Live price - {price} for this {symbol} - {trading_symbol}")
        socketio.emit('price_tick', {'event': 'tick', 'data': tick})


def on_connect(ws, response):
    print("WebSocket: Connected")
    ws.subscribe([instrument_token])
    ws.set_mode(ws.MODE_QUOTE, [instrument_token])

def on_close(ws, code, reason):
    print(f"WebSocket closed with code: {code}, reason: {reason}")
    time.sleep(5)
    # ws.connect(threaded=True)

if instrument_token is None:
    instrument_token = input("Enter Instrument Token: ").strip()
    instrument_token = int(instrument_token)
    trading_symbol = input("Enter Trading Symbol: ").strip()
    if not instrument_token or not trading_symbol:
        print("Invalid input. Exiting.")
        exit()


kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

def restart_websocket():
    global kws, instrument_token
    
    if kws.is_connected():
        print(f"Unsubscribing from {instrument_token}...")
        kws.unsubscribe([instrument_token])
        time.sleep(1)

        print("Closing WebSocket...")
        kws.close()
        time.sleep(2)

    kws = None
    time.sleep(1)

    kws = KiteTicker(api_key="your_api_key", access_token=enctoken + "&user_id=" + user_id)
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close

    print(f"Reconnecting to new instrument {instrument_token}...")
    kws.connect(threaded=True)

@socketio.on('connect')
def start_trading():
    if not kws.is_connected():
        kws.connect(threaded=True)

@app.route('/api/user-event', methods=['POST'])
def handle_user_event():
    global buy_price,user_sell_signal, user_buy_signal,user_auto_buy,user_stop_reentry,instrument_token,trading_symbol,current_trade_id,current_price
    data = request.get_json()
    event_type = data.get('event')
    if event_type == 'sell':
        # user_buy_signal = False
        # user_sell_signal = True
        sell_stock(current_trade_id, current_price)
        print("User sell signal set to True")
    elif event_type == 'buy':
        # user_sell_signal = False
        # user_buy_signal = True
        current_trade_id = buy_stock(instrument_token, current_price)
        buy_price = current_price
        print("User buy signal set to True")
    elif event_type == 'auto_buy':
        user_auto_buy = True
        socketio.emit('trade_event', {'event': 'Auto buy', 'message': f'Auto buy activated'})
    elif event_type == 'stop_auto_reentry':
        user_stop_reentry = False
        socketio.emit('trade_event', {'event': 'Stop auto reentry', 'message': f'Stop auto re-entry functionality'})
    elif event_type == 'start_auto_reentry':
        user_stop_reentry = True
        socketio.emit('trade_event', {'event': 'Start auto reentry', 'message': f'Start auto re-entry functionality'})
    elif event_type == 'scan_market':
        data = get_analyze_data()
        if data:  
            socketio.emit('trade_event', {'event': 'Scanned', 'message': f'Scanning market and new stock picker','data':data})
    elif event_type == "set_token":
        instrument_token = data.get('instrument_token')
        trading_symbol = data.get('trading_symbol')
        restart_websocket()
        socketio.emit('trade_event', {'event': 'Loaded', 'message': f'New setup for stocks'})

    return jsonify({"message": f"{event_type} event received"}), 200

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