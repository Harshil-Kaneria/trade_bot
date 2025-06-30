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

# Global Analysis Parameters - Can be modified as needed
ANALYSIS_PARAMS = {
    'MONEY_CONTROL':True,
    'ISSCOREFILTER':True,
    'SCORE':60,
    "data_period_days": 30,
    "candle_interval": "5minute",
    "ema": {
        "short_period": 9,
        "long_period": 21
    },
    "supertrend": {
        "period": 10,
        "multiplier": 3
    },
    "volume": {
        "sma_period": 20
    },
    "strength_weights": {
        "volume_ratio": 0.3,
        "ema_distance": 0.3,
        "price_distance": 0.4
    },
    "min_data_points": 21,  # Minimum data points required for analysis
    "batch_size": 5,       # Number of stocks to process before delay
    "api_delay": 0.5        # Delay between batches in seconds
}

symbols_list = list(pd.read_csv('data_500.csv')['SYMBOL'])
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

def calculate_supertrend(high, low, close, period=ANALYSIS_PARAMS['supertrend']['period'], 
                         multiplier=ANALYSIS_PARAMS['supertrend']['multiplier']):
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
    
    return supertrend, direction, atr

def calculate_support_resistance(df, lookback_period=30):
    """
    Calculate support and resistance levels using recent pivot points
    Args:
        df: DataFrame with OHLC data
        lookback_period: Number of candles to consider for finding pivot points
    Returns:
        dict with support and resistance levels
    """
    try:
        # Get the most recent data
        recent_data = df.tail(lookback_period).copy()
        
        # Find pivot points (local maxima/minima)
        recent_data['pivot'] = 0
        for i in range(1, len(recent_data)-1):
            prev_close = recent_data.iloc[i-1]['close']
            curr_close = recent_data.iloc[i]['close']
            next_close = recent_data.iloc[i+1]['close']
            
            if curr_close > prev_close and curr_close > next_close:
                recent_data.iloc[i, recent_data.columns.get_loc('pivot')] = 1  # Resistance
            elif curr_close < prev_close and curr_close < next_close:
                recent_data.iloc[i, recent_data.columns.get_loc('pivot')] = -1  # Support
        
        # Get resistance levels (top 3 pivot highs)
        resistance = recent_data[recent_data['pivot'] == 1]['high'].sort_values(ascending=False).head(3).values
        resistance = sorted(resistance)  # Sort from nearest to farthest
        
        # Get support levels (top 3 pivot lows)
        support = recent_data[recent_data['pivot'] == -1]['low'].sort_values().head(3).values
        support = sorted(support, reverse=True)  # Sort from nearest to farthest
        
        # Get current price
        current_price = recent_data.iloc[-1]['close']
        
        # Find nearest support/resistance
        nearest_resistance = next((r for r in resistance if r > current_price), None)
        nearest_support = next((s for s in support if s < current_price), None)
        
        return {
            'support_levels': support.tolist() if hasattr(support, 'tolist') else list(support),
            'resistance_levels': resistance.tolist() if hasattr(resistance, 'tolist') else list(resistance),
            'nearest_support': float(nearest_support) if nearest_support else None,
            'nearest_resistance': float(nearest_resistance) if nearest_resistance else None,
            'method': f'Pivot points from last {lookback_period} candles'
        }
    except Exception as e:
        print(f"Error calculating support/resistance: {e}")
        return {
            'support_levels': [],
            'resistance_levels': [],
            'nearest_support': None,
            'nearest_resistance': None,
            'method': 'Error occurred'
        }

def analyze_instrument(instrument_token, instrument_data):
    try:
        from_datetime = datetime.datetime.now() - datetime.timedelta(days=ANALYSIS_PARAMS['data_period_days'])
        to_datetime = datetime.datetime.now()
        interval = ANALYSIS_PARAMS['candle_interval']
        
        data = kite.historical_data(instrument_token, from_datetime, to_datetime, interval, continuous=False, oi=False)
        df = pd.DataFrame(data)
        
        if len(df) < ANALYSIS_PARAMS['min_data_points']:  # Not enough data points
            return None
            
        # Calculate indicators
        df['ema_short_period'] = talib.EMA(df['close'], timeperiod=ANALYSIS_PARAMS['ema']['short_period'])
        df['ema_long_period'] = talib.EMA(df['close'], timeperiod=ANALYSIS_PARAMS['ema']['long_period'])
        df['volume_sma'] = talib.SMA(df['volume'], timeperiod=ANALYSIS_PARAMS['volume']['sma_period'])
        df['supertrend'], df['supertrend_direction'], df['atr'] = calculate_supertrend(
            df['high'].values, df['low'].values, df['close'].values
        )
        
        # Calculate support and resistance
        sr_levels = calculate_support_resistance(df)
        
        # Get latest values
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        # Calculate additional metrics for ranking
        price_distance_from_ema = (latest['close'] - latest['ema_short_period']) / latest['ema_short_period'] * 100
        volume_ratio = latest['volume'] / latest['volume_sma']
        ema_distance = (latest['ema_short_period'] - latest['ema_long_period']) / latest['ema_long_period'] * 100
        
        # Check conditions
        ema_cross_up = (prev['ema_short_period'] <= prev['ema_long_period']) and (latest['ema_short_period'] > latest['ema_long_period'])
        ema_cross_down = (prev['ema_short_period'] >= prev['ema_long_period']) and (latest['ema_short_period'] < latest['ema_long_period'])
        volume_condition = latest['volume'] > latest['volume_sma']
        
        # Prepare result with all calculated values
        result = {
            "instrument": convert_numpy_types(instrument_data.to_dict()),
            "strength_score": 0,
            "support_resistance": convert_numpy_types(sr_levels)
        }
        
        result["instrument"]["indicators"] = {
            "current_price": convert_numpy_types(latest['close']),
            "ema_short_period": convert_numpy_types(latest['ema_short_period']),
            "ema_long_period": convert_numpy_types(latest['ema_long_period']),
            "supertrend_value": convert_numpy_types(latest['supertrend']),
            "supertrend_direction": "up" if latest['supertrend_direction'] == 1 else "down",
            "atr": convert_numpy_types(latest['atr']),
            "volume": convert_numpy_types(latest['volume']),
            "volume_sma": convert_numpy_types(latest['volume_sma']),
            "volume_ratio": convert_numpy_types(round(volume_ratio, 2)),
            "price_distance_from_ema": convert_numpy_types(round(price_distance_from_ema, 2)),
            "ema_distance": convert_numpy_types(round(ema_distance, 2)),
            "date": latest['date'].strftime('%Y-%m-%d') if hasattr(latest['date'], 'strftime') else str(latest['date'])
        }
        
        # Determine trend and calculate strength score
        if ema_cross_up and volume_condition and latest['supertrend_direction'] == 1:

            symbol = instrument_data['name']
            score = None
            if(ANALYSIS_PARAMS['MONEY_CONTROL']==True):
                print(f"Fetching Moneycontrol link for {symbol}...")
                time.sleep(2)
                link = get_moneycontrol_link(symbol)
                if link:
                    print(f"  Got link: {link}")
                    time.sleep(2)
                    score = scrape_score_from_page(link)
                    print(f"  Score: {score}")
                else:
                    print("No link found")

                if ANALYSIS_PARAMS['ISSCOREFILTER'] and score is not None and score < ANALYSIS_PARAMS['SCORE']:
                    print(f"  Score below {ANALYSIS_PARAMS['SCORE']} ({score}), filtering out...")
                    return None  # Skip this instrument

            # Calculate strength score for upside positions
            strength = (volume_ratio * ANALYSIS_PARAMS['strength_weights']['volume_ratio'] + 
                       (ema_distance * ANALYSIS_PARAMS['strength_weights']['ema_distance']) + 
                       (price_distance_from_ema * ANALYSIS_PARAMS['strength_weights']['price_distance']))
            result["signal"] = "up"
            result["strength_score"] = round(strength, 2)
            result["score"] = score
            
            # Add target and stoploss suggestions for long positions
            if sr_levels['nearest_resistance']:
                atr = latest['atr']
                result["target_suggestion"] = {
                    "conservative": round(sr_levels['nearest_resistance'], 2),
                    "moderate": round(latest['close'] + 2*atr, 2),
                    "aggressive": round(latest['close'] + 3*atr, 2)
                }
                result["stoploss_suggestion"] = {
                    "conservative": round(sr_levels['nearest_support'] if sr_levels['nearest_support'] else latest['close'] - 2*atr, 2),
                    "moderate": round(latest['close'] - atr, 2),
                    "aggressive": round(latest['close'] - 0.5*atr, 2)
                }
            return result
            
        elif ema_cross_down and volume_condition and latest['supertrend_direction'] == -1:

            symbol = instrument_data['name']
            score = None
            if(ANALYSIS_PARAMS['MONEY_CONTROL']==True):
                print(f"Fetching Moneycontrol link for {symbol}...")
                time.sleep(2)
                link = get_moneycontrol_link(symbol)
                if link:
                    print(f"  Got link: {link}")
                    time.sleep(2)
                    score = scrape_score_from_page(link)
                    print(f"  Score: {score}")
                else:
                    print("No link found")

                if ANALYSIS_PARAMS['ISSCOREFILTER'] and score is not None and score < ANALYSIS_PARAMS['SCORE']:
                    print(f"  Score below {ANALYSIS_PARAMS['SCORE']} ({score}), filtering out...")
                    return None  # Skip this instrument

            # Calculate strength score for downside positions
            strength = (volume_ratio * ANALYSIS_PARAMS['strength_weights']['volume_ratio'] + 
                       (abs(ema_distance) * ANALYSIS_PARAMS['strength_weights']['ema_distance']) + 
                       (abs(price_distance_from_ema) * ANALYSIS_PARAMS['strength_weights']['price_distance']))
            result["signal"] = "down"
            result["strength_score"] = round(strength, 2)
            result["score"] = score
            
            # Add target and stoploss suggestions for short positions
            if sr_levels['nearest_support']:
                atr = latest['atr']
                result["target_suggestion"] = {
                    "conservative": round(sr_levels['nearest_support'], 2),
                    "moderate": round(latest['close'] - 2*atr, 2),
                    "aggressive": round(latest['close'] - 3*atr, 2)
                }
                result["stoploss_suggestion"] = {
                    "conservative": round(sr_levels['nearest_resistance'] if sr_levels['nearest_resistance'] else latest['close'] + 2*atr, 2),
                    "moderate": round(latest['close'] + atr, 2),
                    "aggressive": round(latest['close'] + 0.5*atr, 2)
                }
            return result
            
        return None
            
    except Exception as e:
        print(f"Error analyzing instrument {instrument_token}: {e}")
        return None

def run_screener():
    try:
        start_time = datetime.datetime.now()

        upSidePosition = []
        shortSidePosition = []
        total_symbols = len(df)
        
        for i, row in df.iterrows():
            symbol = row['tradingsymbol']
            print(f"Processing {symbol} ({i+1}/{total_symbols})...")
            
            result = analyze_instrument(row['instrument_token'], row)
            
            if result and result["signal"] == "up":
                upSidePosition.append(convert_numpy_types(result))
                print(f"  → Buy Signal found for {symbol}")
            elif result and result["signal"] == "down":
                shortSidePosition.append(convert_numpy_types(result))
                print(f"  → Sell Signal found for {symbol}")
            
            # Add delay after every 10 stocks
            if (i + 1) % ANALYSIS_PARAMS['batch_size'] == 0 and (i + 1) < total_symbols:
                print(f"Completed {i+1} stocks. Waiting {ANALYSIS_PARAMS['api_delay']} seconds...")
                time.sleep(ANALYSIS_PARAMS['api_delay'])
        
        # Sort by strength score (descending)
        # upSidePosition.sort(key=lambda x: x['strength_score'], reverse=True)
        # shortSidePosition.sort(key=lambda x: x['strength_score'], reverse=True)
        upSidePosition.sort(key=lambda x: (x.get('score') is None, -(x.get('score') or 0)))
        shortSidePosition.sort(key=lambda x: (x.get('score') is None, -(x.get('score') or 0)))
        
        # Prepare response with all calculated values
        response = {
            "upSidePosition": convert_numpy_types(upSidePosition),
            "shortSidePosition": convert_numpy_types(shortSidePosition),
            "analysis_parameters": ANALYSIS_PARAMS,
            "last_updated": datetime.datetime.now().isoformat(),
            "total_up_signals": convert_numpy_types(len(upSidePosition)),
            "total_down_signals": convert_numpy_types(len(shortSidePosition)),
            "upSidePositionCsv": ",".join([f"NSE:{item['instrument']['tradingsymbol']}" for item in upSidePosition]),
            "shortSidePositionCsv": ",".join([f"NSE:{item['instrument']['tradingsymbol']}" for item in shortSidePosition]),
        }
        
        with open("screener.json", "w") as f:
            json.dump(response, f, indent=4)

        end_time = datetime.datetime.now()
        duration = (end_time - start_time).total_seconds()
        print(f"\nScanning completed in {duration:.2f} seconds")
        print(f"Found {len(upSidePosition)} buy signals and {len(shortSidePosition)} sell signals")
        print("Results saved to screener.json")
 
    except Exception as e:
        print(f"\nError: {str(e)}")

if __name__ == "__main__":
    run_screener()