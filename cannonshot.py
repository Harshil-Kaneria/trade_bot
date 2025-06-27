import os
from dotenv import load_dotenv
import pandas as pd
from kite_trade import *
import datetime
import talib
import numpy as np
import json
import time
import requests
import urllib.parse
from bs4 import BeautifulSoup

# Load environment variables
load_dotenv(override=True)
enctoken = os.getenv("ENCTOKEN")
kite = KiteApp(enctoken=enctoken)

# Cannonshot Screener Parameters
CANNONSHOT_PARAMS = {
    'ISSCOREFILTER':True,
    'SCORE':60,  
    "data_period_days": 30,
    "daily_candle_interval": "day",
    "weekly_candle_interval": "week",
    "rsi_period": 14,
    "rsi_overbought": 70,
    "min_volume_multiplier": 2.0,
    "min_delivery_multiplier": 2.0,
    "min_data_points": 21,
    "batch_size": 5,  # Number of stocks to scan before delay
    "api_delay": 0.5,  # Delay between batches in seconds
    "only_up": False,  # If True, filter out stocks showing downward movement
    "downward_threshold": -0.5,  # Percentage threshold for downward movement (negative means down)
    "lookback_period": 3  # Number of recent candles to check for downward movement
}

# symbols_list = list(pd.read_csv('data_500.csv')['SYMBOL'])
symbols_list = list(pd.read_csv('data_test.csv')['SYMBOL'])
df = pd.DataFrame(kite.instruments("NSE"))
df = df[df["tradingsymbol"].isin(symbols_list)].reset_index(drop=True)
df = df[df["segment"]!="INDICES"].reset_index()


def get_moneycontrol_link(name: str):
    # Clean and encode query
    query = urllib.parse.quote(name.strip())
    
    # Correct API URL
    url = f"https://www.moneycontrol.com/mccode/common/autosuggestion_solr.php?classic=true&query={query}&type=1&format=json"
    
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return None

        data = resp.json()
        if not data:
            return None

        # Return the first match's link
        return data[0].get("link_src")
    except Exception as e:
        print(f"Error: {e}")
        return None

def scrape_score_from_page(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code != 200:
            return None
        soup = BeautifulSoup(res.text, "html.parser")
        score_div = soup.select_one("div.score")
        if score_div:
            raw_text = score_div.text.strip()
            score = raw_text.split("/")[0]  # "71/100" -> "71"
            return int(score)
        return None
    except Exception as e:
        return None

def convert_numpy_types(obj):
    """Convert numpy types to native Python types for JSON serialization"""
    if isinstance(obj, (np.int32, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.float32, np.float64)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    return obj

def check_downward_movement(daily_df):
    """Check if the stock is showing downward movement in recent candles"""
    if len(daily_df) < CANNONSHOT_PARAMS['lookback_period'] + 1:
        return False
        
    recent_candles = daily_df.iloc[-CANNONSHOT_PARAMS['lookback_period']-1:-1]
    price_changes = []
    
    for i in range(len(recent_candles)-1):
        prev_close = recent_candles.iloc[i]['close']
        current_close = recent_candles.iloc[i+1]['close']
        change_pct = ((current_close - prev_close) / prev_close) * 100
        price_changes.append(change_pct)
    
    # Calculate average percentage change
    if not price_changes:
        return False
        
    avg_change = sum(price_changes) / len(price_changes)
    return avg_change <= CANNONSHOT_PARAMS['downward_threshold']

def analyze_cannonshot(instrument_token, instrument_data):
    try:
        # Get daily data
        daily_from = datetime.datetime.now() - datetime.timedelta(days=CANNONSHOT_PARAMS['data_period_days'])
        daily_data = kite.historical_data(instrument_token, daily_from, datetime.datetime.now(), 
                                       CANNONSHOT_PARAMS['daily_candle_interval'], continuous=False, oi=False)
        daily_df = pd.DataFrame(daily_data)
        
        # Get weekly data
        weekly_from = datetime.datetime.now() - datetime.timedelta(weeks=8)
        weekly_data = kite.historical_data(instrument_token, weekly_from, datetime.datetime.now(), 
                                         CANNONSHOT_PARAMS['weekly_candle_interval'], continuous=False, oi=False)
        weekly_df = pd.DataFrame(weekly_data)
        
        if len(daily_df) < CANNONSHOT_PARAMS['min_data_points'] or len(weekly_df) < 4:
            return None
            
        # Check for downward movement if ONLY_UP is True
        if CANNONSHOT_PARAMS['only_up'] and check_downward_movement(daily_df):
            return None
            
        # Calculate indicators for daily data
        daily_df['rsi'] = talib.RSI(daily_df['close'], timeperiod=CANNONSHOT_PARAMS['rsi_period'])
        daily_df['volume_ma'] = daily_df['volume'].rolling(window=20).mean()
        daily_df['delivery_ma'] = daily_df['volume'].rolling(window=20).mean()
        daily_df['prev_high'] = daily_df['high'].shift(1)
        
        # Calculate weekly high
        weekly_df['prev_week_high'] = weekly_df['high'].shift(1)
        
        # Get latest values
        latest_daily = daily_df.iloc[-1]
        prev_daily = daily_df.iloc[-2]
        latest_weekly = weekly_df.iloc[-1]
        prev_weekly = weekly_df.iloc[-2]
        
        # Check conditions
        volume_condition = (latest_daily['volume'] > CANNONSHOT_PARAMS['min_volume_multiplier'] * latest_daily['volume_ma'])
        delivery_condition = (latest_daily['volume'] > CANNONSHOT_PARAMS['min_delivery_multiplier'] * latest_daily['delivery_ma'])
        close_above_prev_high = latest_daily['close'] > latest_daily['prev_high']
        close_above_week_high = latest_daily['close'] > prev_weekly['high']
        rsi_crossed = (prev_daily['rsi'] < CANNONSHOT_PARAMS['rsi_overbought']) and (latest_daily['rsi'] >= CANNONSHOT_PARAMS['rsi_overbought'])
        rsi_overbought = latest_daily['rsi'] >= CANNONSHOT_PARAMS['rsi_overbought']
        
        if (volume_condition and delivery_condition and close_above_prev_high and 
            close_above_week_high and rsi_crossed and rsi_overbought):
            
            symbol = instrument_data['name']
            print(f"Fetching Moneycontrol link for {symbol}...")
            link = get_moneycontrol_link(symbol)
            score = None
            time.sleep(2)
            if link:
                print(f"  Got link: {link}")
                score = scrape_score_from_page(link)
                print(f"  Score: {score}")
                time.sleep(2)
            else:
                print("No link found")

            if CANNONSHOT_PARAMS['ISSCOREFILTER'] and score is not None and score < CANNONSHOT_PARAMS['SCORE']:
                print(f"  Score below {CANNONSHOT_PARAMS['SCORE']} ({score}), filtering out...")
                return None  # Skip this instrument
                    
            result = {
                "instrument": convert_numpy_types(instrument_data.to_dict()),
                "current_price": convert_numpy_types(latest_daily['close']),
                "volume": convert_numpy_types(latest_daily['volume']),
                "volume_ma": convert_numpy_types(latest_daily['volume_ma']),
                "volume_ratio": convert_numpy_types(round(latest_daily['volume'] / latest_daily['volume_ma'], 2)),
                "delivery_ratio": convert_numpy_types(round(latest_daily['volume'] / latest_daily['delivery_ma'], 2)),
                "rsi": convert_numpy_types(round(latest_daily['rsi'], 2)),
                "prev_high": convert_numpy_types(latest_daily['prev_high']),
                "prev_week_high": convert_numpy_types(prev_weekly['high']),
                "date": latest_daily['date'].strftime('%Y-%m-%d') if hasattr(latest_daily['date'], 'strftime') else str(latest_daily['date']),
                "score":score
            }
            return result
            
        return None
            
    except Exception as e:
        print(f"Error analyzing instrument {instrument_token}: {e}")
        return None

def run_cannonshot_screener():
    try:
        start_time = datetime.datetime.now()
        cannonshot_stocks = []
        total_symbols = len(df)
        
        for i, row in df.iterrows():
            symbol = row['tradingsymbol']
            print(f"Processing {symbol} ({i+1}/{total_symbols})...")
            
            result = analyze_cannonshot(row['instrument_token'], row)
            
            if result:
                cannonshot_stocks.append(result)
                print(f"  → Cannonshot signal found for {symbol}")
            
            # Add delay after every batch of 10 stocks
            if (i + 1) % CANNONSHOT_PARAMS['batch_size'] == 0:
                print(f"Completed batch of {CANNONSHOT_PARAMS['batch_size']} stocks. Waiting {CANNONSHOT_PARAMS['api_delay']} seconds...")
                time.sleep(CANNONSHOT_PARAMS['api_delay'])
        
        # Prepare response
        response = {
            "cannonshot_stocks": convert_numpy_types(cannonshot_stocks),
            "analysis_parameters": CANNONSHOT_PARAMS,
            "last_updated": datetime.datetime.now().isoformat(),
            "total_signals": len(cannonshot_stocks),
            "cannonshot_stocks_csv": ",".join([f"NSE:{item['instrument']['tradingsymbol']}" for item in cannonshot_stocks]),
        }
        
        with open("cannonshot_screener.json", "w") as f:
            json.dump(response, f, indent=4)

        end_time = datetime.datetime.now()
        duration = (end_time - start_time).total_seconds()
        print(f"\nScanning completed in {duration:.2f} seconds")
        print(f"Found {len(cannonshot_stocks)} Cannonshot signals")
        print("Results saved to cannonshot_screener.json")
 
    except Exception as e:
        print(f"\nError: {str(e)}")

if __name__ == "__main__":
    run_cannonshot_screener()