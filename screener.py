import os
from dotenv import load_dotenv
import pandas as pd
from flask import Flask, jsonify
from kite_trade import *
import datetime
import talib
import numpy as np

# Load environment variables
load_dotenv(override=True)
enctoken = os.getenv("ENCTOKEN")
kite = KiteApp(enctoken=enctoken)

app = Flask(__name__)

symbols_list = list(pd.read_csv('data_500.csv')['SYMBOL'])
df = pd.DataFrame(kite.instruments("NSE"))
df = df[df["tradingsymbol"].isin(symbols_list)].reset_index(drop=True)
df = df[df["segment"]!="INDICES"].reset_index()

def calculate_supertrend(high, low, close, period=10, multiplier=3):
    # Calculate ATR
    atr = talib.ATR(high, low, close, timeperiod=period)
    
    # Calculate basic upper and lower bands
    median_price = (high + low) / 2
    upper_band = median_price + (multiplier * atr)
    lower_band = median_price - (multiplier * atr)
    
    # Initialize SuperTrend
    supertrend = [0] * len(close)
    direction = [1] * len(close)  # 1 for uptrend, -1 for downtrend
    
    for i in range(1, len(close)):
        if close[i] > upper_band[i-1]:
            direction[i] = 1
        elif close[i] < lower_band[i-1]:
            direction[i] = -1
        else:
            direction[i] = direction[i-1]
            
        if direction[i] == 1:
            supertrend[i] = lower_band[i]
        else:
            supertrend[i] = upper_band[i]
    
    return supertrend, direction

def analyze_instrument(instrument_token):
    try:
        from_datetime = datetime.datetime.now() - datetime.timedelta(days=30)
        to_datetime = datetime.datetime.now()
        interval = "5minute"
        
        data = kite.historical_data(instrument_token, from_datetime, to_datetime, interval, continuous=False, oi=False)
        df = pd.DataFrame(data)
        
        if len(df) < 21:  # Not enough data points
            return None
            
        # Calculate indicators
        df['ema9'] = talib.EMA(df['close'], timeperiod=9)
        df['ema21'] = talib.EMA(df['close'], timeperiod=21)
        df['volume_sma20'] = talib.SMA(df['volume'], timeperiod=20)
        df['supertrend'], df['supertrend_direction'] = calculate_supertrend(
            df['high'].values, df['low'].values, df['close'].values
        )
        
        # Get latest values
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        # Check conditions
        ema_cross_up = (prev['ema9'] <= prev['ema21']) and (latest['ema9'] > latest['ema21'])
        ema_cross_down = (prev['ema9'] >= prev['ema21']) and (latest['ema9'] < latest['ema21'])
        volume_condition = latest['volume'] > latest['volume_sma20']
        
        # Determine trend
        if ema_cross_up and volume_condition and latest['supertrend_direction'] == 1:
            return "up"
        elif ema_cross_down and volume_condition and latest['supertrend_direction'] == -1:
            return "down"
        else:
            return None
            
    except Exception as e:
        print(f"Error analyzing instrument {instrument_token}: {e}")
        return None

@app.route("/api/screener", methods=["GET"])
def run_screener():
    try:
        upSidePosition = []
        shortSidePosition = []
        
        for _, row in df.iterrows():
            result = analyze_instrument(row['instrument_token'])
            
            if result == "up":
                upSidePosition.append(row.to_dict())
            elif result == "down":
                shortSidePosition.append(row.to_dict())
        
        response = {
            "upSidePosition": upSidePosition,
            "shortSidePosition": shortSidePosition,
            "last_updated": datetime.datetime.now().isoformat()
        }
        
        return jsonify(response)
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)