import os
from dotenv import load_dotenv
import pandas as pd
from kite_trade import *
import datetime
import talib
import numpy as np
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables
load_dotenv(override=True)
enctoken = os.getenv("ENCTOKEN")
kite = KiteApp(enctoken=enctoken)

# Global Analysis Parameters - Can be modified as needed
ANALYSIS_PARAMS = {
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
    "min_data_points": 21  # Minimum data points required for analysis
}

symbols_list = list(pd.read_csv('data_500.csv')['SYMBOL'])
df = pd.DataFrame(kite.instruments("NSE"))
df = df[df["tradingsymbol"].isin(symbols_list)].reset_index(drop=True)
df = df[df["segment"]!="INDICES"].reset_index()

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
            "analysis_parameters": ANALYSIS_PARAMS,
            "strength_score": 0
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
            # Calculate strength score for upside positions
            strength = (volume_ratio * ANALYSIS_PARAMS['strength_weights']['volume_ratio'] + 
                       (ema_distance * ANALYSIS_PARAMS['strength_weights']['ema_distance']) + 
                       (price_distance_from_ema * ANALYSIS_PARAMS['strength_weights']['price_distance']))
            result["signal"] = "up"
            result["strength_score"] = round(strength, 2)
            return result
            
        elif ema_cross_down and volume_condition and latest['supertrend_direction'] == -1:
            # Calculate strength score for downside positions
            strength = (volume_ratio * ANALYSIS_PARAMS['strength_weights']['volume_ratio'] + 
                       (abs(ema_distance) * ANALYSIS_PARAMS['strength_weights']['ema_distance']) + 
                       (abs(price_distance_from_ema) * ANALYSIS_PARAMS['strength_weights']['price_distance']))
            result["signal"] = "down"
            result["strength_score"] = round(strength, 2)
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
        
        # Create thread-safe lists
        upSidePosition_lock = threading.Lock()
        shortSidePosition_lock = threading.Lock()
        
        def process_row(row):
            result = analyze_instrument(row['instrument_token'], row)
            
            if result and result["signal"] == "up":
                with upSidePosition_lock:
                    upSidePosition.append(convert_numpy_types(result))
                print(f"Scanned {row['tradingsymbol']} + Buy Signal")
            elif result and result["signal"] == "down":
                with shortSidePosition_lock:
                    shortSidePosition.append(convert_numpy_types(result))
                print(f"Scanned {row['tradingsymbol']} - Sell Signal")
            else:
                print(f"Scanned {row['tradingsymbol']}")
        
        # Using ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=(os.cpu_count() * 5)) as executor:
            # Submit all tasks to the executor
            futures = [executor.submit(process_row, row) for _, row in df.iterrows()]
            
            # Wait for all tasks to complete
            for future in as_completed(futures):
                future.result()  # This will raise exceptions if any occurred
        
        # Sort by strength score (descending)
        upSidePosition.sort(key=lambda x: x['strength_score'], reverse=True)
        shortSidePosition.sort(key=lambda x: x['strength_score'], reverse=True)
        
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
        print("\nScan saved to screener.json")
 
    except Exception as e:
        print(f"\nError {str(e)}")

if __name__ == "__main__":
    run_screener()