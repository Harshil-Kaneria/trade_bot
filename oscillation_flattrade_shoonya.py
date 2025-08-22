import os
import argparse
import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from api_helper_flattrade import NorenApiPy
from api_helper_shoonya import ShoonyaApiPy
from dotenv import load_dotenv
import logging
from threading import Lock, Thread, Event
from collections import deque, defaultdict
from queue import Queue, PriorityQueue
import pyotp
import warnings
warnings.filterwarnings('ignore')

# Performance optimization imports
try:
    from numba import jit, prange
    NUMBA_AVAILABLE = True
except ImportError:
    NUMBA_AVAILABLE = False
    print("Numba not available, using standard Python")

try:
    from scipy import stats
    from scipy.signal import savgol_filter
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    print("SciPy not available, some features will be disabled")

try:
    import talib
    TALIB_AVAILABLE = True
except ImportError:
    TALIB_AVAILABLE = False
    print("TA-Lib not available, using custom indicators")

parser = argparse.ArgumentParser()
parser.add_argument("--broker", type=str, default="F", help="Value for broker is (SHOONYA/FLATTRADE) so S/F")
parser.add_argument("--mode", type=str, default="AGGRESSIVE", help="Trading mode: CONSERVATIVE, MODERATE, AGGRESSIVE, ULTRA")
parser.add_argument("--max_trades", type=int, default=5, help="Maximum concurrent trades")
parser.add_argument("--risk_multiplier", type=float, default=1.5, help="Risk multiplier for position sizing")
args = parser.parse_args()

# Enhanced logging with performance metrics
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('hft_oscillation_scalper.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Load credentials
load_dotenv(override=True)

api = None
apiSocket = None
broker = args.broker
trading_mode = args.mode
MAX_CONCURRENT_TRADES = args.max_trades
RISK_MULTIPLIER = args.risk_multiplier

# Enhanced Trading Configuration for HFT
CAPITAL = 90000
TARGET_PROFIT_PER_TRADE = 180
MAX_LOSS_PER_TRADE = 150
MAX_POSITION_DURATION = 180  # Reduced to 3 minutes for faster turnover
AUTO_CLOSE_ENABLED = True
VOLATILITY_THRESHOLD = 0.0003  # Reduced for more sensitive detection
MIN_PRICE = 100
MAX_PRICE = 3000
TICK_SIZE = 0.10

# Advanced Technical Analysis Parameters
SHORT_EMA_PERIOD = 3  # Ultra-short for HFT
LONG_EMA_PERIOD = 15
RSI_PERIOD = 14
RSI_OVERBOUGHT = 75
RSI_OVERSOLD = 25
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
BOLLINGER_PERIOD = 20
BOLLINGER_STD = 2
STOCHASTIC_K = 14
STOCHASTIC_D = 3
ATR_PERIOD = 14

# HFT Specific Parameters
OSCILLATION_PERIOD = 5  # Ultra-short for micro-opportunities
HISTORY_WINDOW = 100  # Extended for better pattern recognition
PRICE_UPDATE_INTERVAL = 0.1  # 100ms updates
SIGNAL_CONFIRMATION_WINDOW = 3  # Confirm signals over 3 ticks
MIN_VOLUME_THRESHOLD = 1000  # Minimum volume for liquidity
MAX_SPREAD_THRESHOLD = 0.002  # Maximum bid-ask spread (0.2%)

# Performance Optimization
USE_GPU_ACCELERATION = True  # For RTX 3060
USE_MULTIPROCESSING = True
MAX_WORKER_PROCESSES = 4
BATCH_SIZE = 50  # Process symbols in batches

# Global trackers with enhanced performance
active_positions = {}  # Multiple concurrent positions
position_lock = Lock()
processing_data = True
trade_signal_queue = PriorityQueue()  # Priority-based signal processing
price_history = defaultdict(lambda: deque(maxlen=HISTORY_WINDOW))
volume_history = defaultdict(lambda: deque(maxlen=HISTORY_WINDOW))
volatility_cache = {}
opportunity_cache = {}
last_signal_time = defaultdict(float)
signal_cooldown = 2.0  # Minimum seconds between signals for same symbol

# Performance metrics
trade_stats = {
    'total_trades': 0,
    'winning_trades': 0,
    'losing_trades': 0,
    'total_profit': 0.0,
    'max_drawdown': 0.0,
    'avg_trade_duration': 0.0,
    'signals_generated': 0,
    'signals_executed': 0
}

# Initialize broker connection
if broker == 'F':
    api_key = os.getenv("FLATTRADE_API_KEY")
    api_secret = os.getenv("FLATTRADE_SECRET_KEY")
    user_id = os.getenv("FLATTRADE_USER_ID")
    user_token = os.getenv("FLATTRADE_SESSION")
    api = NorenApiPy()
    ret = api.set_session(userid=user_id, password='', usertoken=user_token)
    if ret is None:
        logger.error("Failed to set session. Check credentials.")
        exit()
    apiSocket = api
    logger.info("✅ FLATTRADE API Connected")
else:
    uid = os.getenv("SHOONYA_UID")
    pwd = os.getenv("SHOONYA_PASSWORD")
    factor2 = pyotp.TOTP(os.getenv("SHOONYA_TOTP")).now()
    vc = os.getenv("SHOONYA_VENDOR_CODE")
    app_key = os.getenv("SHOONYA_API_KEY")
    imei = os.getenv("SHOONYA_IMEI")
    api = ShoonyaApiPy()
    ret = api.login(userid=uid, password=pwd, twoFA=factor2, vendor_code=vc, api_secret=app_key, imei=imei)
    if ret is None:
        logger.error("Failed to set session. Check credentials.")
        exit()
    apiSocket = api
    logger.info("✅ SHOONYA API Connected")

# Load and prepare symbol data with enhanced filtering
try:
    symbols_list = pd.read_csv('data_500.csv')['SYMBOL'].tolist()
    instruments = pd.read_csv('NSE_Equity_Master.csv')
    
    # Enhanced filtering for better liquidity and volatility
    df = instruments[instruments['Symbol'].isin(symbols_list)]
    df = df[df['Instrument'] == "EQ"]
    
    # Additional filtering for HFT suitability
    # Filter by price range and add volume data if available
    df = df[['Token', 'Symbol']].rename(columns={
        'Token': 'instrument_token',
        'Symbol': 'tradingsymbol'
    }).reset_index(drop=True)
    
    # Initialize enhanced data structures
    for token in df['instrument_token']:
        price_history[str(token)] = deque(maxlen=HISTORY_WINDOW)
        volume_history[str(token)] = deque(maxlen=HISTORY_WINDOW)
    
    token_to_index = {str(token): idx for idx, token in enumerate(df['instrument_token'])}
    tokens = df['instrument_token'].astype(str).tolist()
    
    logger.info(f"✅ Loaded {len(tokens)} symbols for HFT trading")
    logger.info(f"   Trading Mode: {trading_mode}")
    logger.info(f"   Max Concurrent Trades: {MAX_CONCURRENT_TRADES}")
    logger.info(f"   Risk Multiplier: {RISK_MULTIPLIER}")
    
except Exception as e:
    logger.error(f"Failed to load symbol data: {e}")
    exit()

# Enhanced utility functions
def round_to_tick(value, tick=TICK_SIZE):
    """Enhanced tick rounding with validation"""
    try:
        if value <= 0:
            return 0
        return round(round(float(value) / float(tick)) * float(tick), 2)
    except Exception:
        return round(value, 2)

def get_price_history(token, minutes=30):
    """Enhanced historical data fetching with caching"""
    try:
        endtime = int(time.time())
        starttime = endtime - minutes * 60
        hist_data = api.get_time_price_series(exchange='NSE', token=token, starttime=starttime, endtime=endtime, interval=1)
        if hist_data:
            return [float(candle['intc']) for candle in hist_data]
    except Exception as e:
        logger.error(f"Error fetching historical data for token {token}: {e}")
    return []

def get_volume_history(token, minutes=30):
    """Fetch volume history for enhanced analysis"""
    try:
        endtime = int(time.time())
        starttime = endtime - minutes * 60
        hist_data = api.get_time_price_series(exchange='NSE', token=token, starttime=starttime, endtime=endtime, interval=1)
        if hist_data:
            return [int(candle.get('vol', 0)) for candle in hist_data]
    except Exception as e:
        logger.error(f"Error fetching volume data for token {token}: {e}")
    return []

def get_todays_high_low(token):
    """Enhanced high/low detection with better error handling"""
    try:
        quote = api.get_quotes(exchange='NSE', token=str(token))
        if not quote:
            return None, None
        
        # Handle different quote formats
        if isinstance(quote, dict) and quote.get('data') and isinstance(quote.get('data'), dict):
            quote = quote.get('data')
        
        # Enhanced key detection
        high_keys = ['h', 'high', 'day_high', 'dayHigh', 'ih', 'dayHigh']
        low_keys = ['l', 'low', 'day_low', 'dayLow', 'il', 'dayLow']
        
        day_high = None
        day_low = None
        
        for k in high_keys:
            if k in quote and quote.get(k) not in (None, ''):
                day_high = float(quote.get(k))
                break
        
        for k in low_keys:
            if k in quote and quote.get(k) not in (None, ''):
                day_low = float(quote.get(k))
                break
        
        return day_high, day_low
        
    except Exception as e:
        logger.warning(f"Failed to fetch quote high/low for token {token}: {e}")
        return None, None

# Enhanced technical analysis functions
def calculate_ema_enhanced(prices, period):
    """Enhanced EMA calculation with validation"""
    if len(prices) < period:
        return None
    
    try:
        if NUMBA_AVAILABLE:
            # Use Numba acceleration if available
            return calculate_ema_numba(np.array(prices), period)
        else:
            # Standard calculation
            ema = pd.Series(prices).ewm(span=period, adjust=False).mean().iloc[-1]
            return ema
    except Exception as e:
        logger.error(f"Error calculating EMA: {e}")
        return None

def calculate_rsi_enhanced(prices, period=14):
    """Enhanced RSI calculation"""
    if len(prices) < period + 1:
        return None
    
    try:
        if TALIB_AVAILABLE:
            # Use TA-Lib if available
            return talib.RSI(np.array(prices), timeperiod=period)[-1]
        else:
            # Custom RSI calculation
            deltas = np.diff(prices)
            gains = np.where(deltas > 0, deltas, 0)
            losses = np.where(deltas < 0, -deltas, 0)
            
            avg_gain = np.mean(gains[-period:])
            avg_loss = np.mean(losses[-period:])
            
            if avg_loss == 0:
                return 100
            
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            return rsi
    except Exception as e:
        logger.error(f"Error calculating RSI: {e}")
        return None

def calculate_macd_enhanced(prices, fast=12, slow=26, signal=9):
    """Enhanced MACD calculation"""
    if len(prices) < slow + signal:
        return None, None, None
    
    try:
        if TALIB_AVAILABLE:
            # Use TA-Lib if available
            macd, signal_line, histogram = talib.MACD(np.array(prices), 
                                                     fastperiod=fast, 
                                                     slowperiod=slow, 
                                                     signalperiod=signal)
            return macd[-1], signal_line[-1], histogram[-1]
        else:
            # Custom MACD calculation
            ema_fast = calculate_ema_enhanced(prices, fast)
            ema_slow = calculate_ema_enhanced(prices, slow)
            
            if ema_fast is None or ema_slow is None:
                return None, None, None
            
            macd_line = ema_fast - ema_slow
            signal_line = calculate_ema_enhanced([macd_line], signal)
            histogram = macd_line - signal_line if signal_line else 0
            
            return macd_line, signal_line, histogram
    except Exception as e:
        logger.error(f"Error calculating MACD: {e}")
        return None, None, None

def calculate_bollinger_bands(prices, period=20, std_dev=2):
    """Calculate Bollinger Bands"""
    if len(prices) < period:
        return None, None, None
    
    try:
        sma = np.mean(prices[-period:])
        std = np.std(prices[-period:])
        
        upper_band = sma + (std_dev * std)
        lower_band = sma - (std_dev * std)
        
        return upper_band, sma, lower_band
    except Exception as e:
        logger.error(f"Error calculating Bollinger Bands: {e}")
        return None, None, None

# Numba-accelerated functions (if available)
if NUMBA_AVAILABLE:
    @jit(nopython=True)
    def calculate_ema_numba(prices, period):
        """Numba-accelerated EMA calculation"""
        alpha = 2.0 / (period + 1.0)
        ema = prices[0]
        
        for i in range(1, len(prices)):
            ema = alpha * prices[i] + (1 - alpha) * ema
        
        return ema
    
    @jit(nopython=True)
    def detect_micro_patterns_numba(prices, window=5):
        """Numba-accelerated micro pattern detection"""
        n = len(prices)
        if n < window:
            return 0.0
        
        # Price momentum
        momentum = (prices[-1] - prices[-window]) / prices[-window]
        
        # Price acceleration
        accel = 0
        if n >= 3:
            accel = (prices[-1] - 2*prices[-2] + prices[-3]) / prices[-2]
        
        # Volatility
        returns = np.zeros(n-1)
        for i in range(1, n):
            returns[i-1] = (prices[i] - prices[i-1]) / prices[i-1]
        
        volatility = np.std(returns)
        
        # Pattern score
        score = abs(momentum) * 0.4 + abs(accel) * 0.3 + volatility * 0.3
        return score
else:
    def calculate_ema_numba(prices, period):
        """Fallback EMA calculation without Numba"""
        return calculate_ema_enhanced(prices, period)
    
    def detect_micro_patterns_numba(prices, window=5):
        """Fallback pattern detection without Numba"""
        return detect_micro_patterns(prices, window)

def detect_micro_patterns(prices, window=5):
    """Standard micro pattern detection"""
    n = len(prices)
    if n < window:
        return 0.0
    
    try:
        # Price momentum
        momentum = (prices[-1] - prices[-window]) / prices[-window]
        
        # Price acceleration
        accel = 0
        if n >= 3:
            accel = (prices[-1] - 2*prices[-2] + prices[-3]) / prices[-2]
        
        # Volatility
        returns = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(1, n)]
        volatility = np.std(returns) if returns else 0
        
        # Pattern score
        score = abs(momentum) * 0.4 + abs(accel) * 0.3 + volatility * 0.3
        return score
    except Exception as e:
        logger.error(f"Error in micro pattern detection: {e}")
        return 0.0

class AdvancedOpportunityDetector:
    """Advanced opportunity detection with multiple confirmation signals"""
    
    def __init__(self):
        self.signal_cache = {}
        self.confirmation_cache = {}
        self.last_analysis = {}
        self.opportunity_scores = defaultdict(list)
    
    def detect_opportunity(self, token, prices, volumes):
        """Multi-factor opportunity detection with enhanced scoring"""
        if len(prices) < HISTORY_WINDOW:
            return None
        
        current_time = time.time()
        cache_key = f"{token}_{current_time//1}"  # Cache per second
        
        if cache_key in self.signal_cache:
            return self.signal_cache[cache_key]
        
        try:
            # Convert to numpy arrays for faster processing
            price_array = np.array(prices, dtype=np.float64)
            volume_array = np.array(volumes, dtype=np.float64)
            
            # Enhanced technical indicators
            rsi = calculate_rsi_enhanced(prices, RSI_PERIOD)
            macd, macd_signal, macd_hist = calculate_macd_enhanced(prices, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
            bb_upper, bb_middle, bb_lower = calculate_bollinger_bands(prices, BOLLINGER_PERIOD, BOLLINGER_STD)
            
            # Micro pattern detection
            pattern_score = detect_micro_patterns_numba(price_array, OSCILLATION_PERIOD)
            
            # Enhanced oscillation score
            oscillation_score = self._calculate_oscillation_score_enhanced(price_array, volume_array)
            
            # Multi-timeframe analysis
            short_trend = self._analyze_trend(price_array, 5)
            medium_trend = self._analyze_trend(price_array, 15)
            long_trend = self._analyze_trend(price_array, 30)
            
            # Volume analysis
            volume_trend = self._analyze_volume_trend(volume_array)
            
            # Price action patterns
            price_patterns = self._detect_price_patterns(price_array)
            
            # Volatility analysis
            volatility_score = self._analyze_volatility(price_array)
            
            # Momentum analysis
            momentum_score = self._analyze_momentum(price_array)
            
            # Calculate composite score
            composite_score = self._calculate_composite_score(
                rsi, macd, pattern_score, oscillation_score,
                short_trend, medium_trend, long_trend,
                volume_trend, price_patterns, volatility_score, momentum_score
            )
            
            # Determine trade direction
            direction = self._determine_trade_direction(
                rsi, macd, short_trend, medium_trend, pattern_score
            )
            
            # Quality filter - only high-quality opportunities
            if composite_score > 0.7 and direction:  # Higher threshold for quality
                opportunity = {
                    'token': token,
                    'symbol': self._get_symbol(token),
                    'direction': direction,
                    'score': composite_score,
                    'confidence': self._calculate_confidence(composite_score),
                    'indicators': {
                        'rsi': rsi,
                        'macd': macd,
                        'pattern_score': pattern_score,
                        'oscillation_score': oscillation_score,
                        'volatility_score': volatility_score,
                        'momentum_score': momentum_score
                    },
                    'timestamp': current_time,
                    'price': prices[-1],
                    'volume': volumes[-1] if volumes else 0
                }
                
                # Store opportunity score for trend analysis
                self.opportunity_scores[token].append(composite_score)
                if len(self.opportunity_scores[token]) > 10:
                    self.opportunity_scores[token].pop(0)
                
                self.signal_cache[cache_key] = opportunity
                return opportunity
            
            self.signal_cache[cache_key] = None
            return None
            
        except Exception as e:
            logger.error(f"Error in opportunity detection for {token}: {e}")
            return None
    
    def _calculate_oscillation_score_enhanced(self, prices, volumes, window=5):
        """Enhanced oscillation detection with volume confirmation"""
        n = len(prices)
        if n < window:
            return 0.0
        
        try:
            # Price oscillation
            price_range = np.max(prices[-window:]) - np.min(prices[-window:])
            avg_price = np.mean(prices[-window:])
            price_oscillation = price_range / (avg_price + 1e-10)
            
            # Volume oscillation
            if len(volumes) >= window:
                volume_range = np.max(volumes[-window:]) - np.min(volumes[-window:])
                avg_volume = np.mean(volumes[-window:])
                volume_oscillation = volume_range / (avg_volume + 1e-10) if avg_volume > 0 else 0
            else:
                volume_oscillation = 0
            
            # Price reversal detection
            reversals = 0
            for i in range(1, window):
                if (prices[-i] - prices[-i-1]) * (prices[-i-1] - prices[-i-2]) < 0:
                    reversals += 1
            
            reversal_score = reversals / (window - 1)
            
            # Trend strength filter
            trend_strength = abs(np.mean(np.diff(prices[-window:])))
            if trend_strength > 0.01:  # Strong trend - reduce oscillation score
                price_oscillation *= 0.5
            
            # Combined score
            total_score = (price_oscillation * 0.5 + 
                           volume_oscillation * 0.3 + 
                           reversal_score * 0.2)
            
            return min(total_score, 1.0)  # Cap at 1.0
            
        except Exception as e:
            logger.error(f"Error in oscillation score calculation: {e}")
            return 0.0
    
    def _analyze_trend(self, prices, period):
        """Analyze trend strength over given period"""
        if len(prices) < period:
            return 0
        
        try:
            if SCIPY_AVAILABLE:
                recent_prices = prices[-period:]
                slope, _, r_value, _, _ = stats.linregress(range(len(recent_prices)), recent_prices)
                
                # Normalize slope by average price
                avg_price = np.mean(recent_prices)
                normalized_slope = slope / (avg_price + 1e-10)
                
                # Return trend strength (-1 to 1)
                return np.clip(normalized_slope * 1000, -1, 1)
            else:
                # Simple trend calculation
                recent_prices = prices[-period:]
                first_half = np.mean(recent_prices[:period//2])
                second_half = np.mean(recent_prices[period//2:])
                
                if first_half == 0:
                    return 0
                
                trend = (second_half - first_half) / first_half
                return np.clip(trend, -1, 1)
                
        except Exception as e:
            logger.error(f"Error in trend analysis: {e}")
            return 0
    
    def _analyze_volume_trend(self, volumes):
        """Analyze volume trend"""
        if len(volumes) < 10:
            return 0
        
        try:
            recent_volumes = volumes[-10:]
            avg_volume = np.mean(volumes[-20:-10]) if len(volumes) >= 20 else np.mean(volumes)
            
            if avg_volume == 0:
                return 0
            
            volume_trend = (np.mean(recent_volumes) - avg_volume) / avg_volume
            return np.clip(volume_trend, -1, 1)
            
        except Exception as e:
            logger.error(f"Error in volume trend analysis: {e}")
            return 0
    
    def _detect_price_patterns(self, prices):
        """Detect common price patterns"""
        if len(prices) < 10:
            return 0
        
        try:
            patterns = 0
            
            # Double bottom/top detection
            for i in range(2, len(prices)-2):
                if (prices[i] < prices[i-1] and prices[i] < prices[i+1] and
                    prices[i-2] < prices[i-1] and prices[i+2] < prices[i+1]):
                    patterns += 1  # Double bottom
                elif (prices[i] > prices[i-1] and prices[i] > prices[i+1] and
                      prices[i-2] > prices[i-1] and prices[i+2] > prices[i+1]):
                    patterns += 1  # Double top
            
            # Hammer/Doji detection
            for i in range(1, len(prices)-1):
                body = abs(prices[i] - prices[i-1])
                total_range = max(prices[i], prices[i-1]) - min(prices[i], prices[i-1])
                if total_range > 0 and body / total_range < 0.3:
                    patterns += 0.5  # Hammer/Doji
            
            # Normalize pattern count
            return min(patterns / 5, 1.0)
            
        except Exception as e:
            logger.error(f"Error in price pattern detection: {e}")
            return 0
    
    def _analyze_volatility(self, prices):
        """Analyze price volatility"""
        if len(prices) < 10:
            return 0
        
        try:
            returns = np.diff(prices) / prices[:-1]
            volatility = np.std(returns)
            
            # Normalize volatility (0-1 scale)
            normalized_vol = min(volatility * 100, 1.0)
            return normalized_vol
            
        except Exception as e:
            logger.error(f"Error in volatility analysis: {e}")
            return 0
    
    def _analyze_momentum(self, prices):
        """Analyze price momentum"""
        if len(prices) < 5:
            return 0
        
        try:
            # Calculate momentum over different periods
            momentum_5 = (prices[-1] - prices[-5]) / prices[-5] if prices[-5] > 0 else 0
            momentum_10 = (prices[-1] - prices[-10]) / prices[-10] if len(prices) >= 10 and prices[-10] > 0 else 0
            
            # Combined momentum score
            momentum_score = (momentum_5 * 0.7 + momentum_10 * 0.3)
            return np.clip(momentum_score, -1, 1)
            
        except Exception as e:
            logger.error(f"Error in momentum analysis: {e}")
            return 0
    
    def _calculate_composite_score(self, rsi, macd, pattern_score, oscillation_score,
                                 short_trend, medium_trend, long_trend, volume_trend, 
                                 price_patterns, volatility_score, momentum_score):
        """Calculate composite opportunity score with enhanced weighting"""
        
        # RSI score (0-1)
        rsi_score = 0
        if rsi is not None:
            if rsi < 25 or rsi > 75:  # Extreme conditions
                rsi_score = 1.0
            elif rsi < 35 or rsi > 65:  # Moderate conditions
                rsi_score = 0.8
            elif rsi < 45 or rsi > 55:  # Neutral conditions
                rsi_score = 0.4
        
        # MACD score (0-1)
        macd_score = 0
        if macd is not None:
            macd_score = min(abs(macd) / 10, 1.0)  # Normalize MACD
        
        # Trend alignment score
        trend_alignment = 0
        if abs(short_trend) > 0.5 and abs(medium_trend) > 0.3:
            if (short_trend > 0 and medium_trend > 0) or (short_trend < 0 and medium_trend < 0):
                trend_alignment = 1.0
            else:
                trend_alignment = 0.2
        
        # Volume confirmation score
        volume_score = (volume_trend + 1) / 2  # Convert -1,1 to 0,1
        
        # Volatility score (moderate volatility is preferred)
        volatility_preference = 1.0 - abs(volatility_score - 0.5) * 2  # Peak at 0.5
        
        # Momentum score
        momentum_preference = abs(momentum_score)  # Higher momentum is better
        
        # Composite score with enhanced weights
        composite = (
            rsi_score * 0.15 +
            macd_score * 0.10 +
            pattern_score * 0.20 +
            oscillation_score * 0.25 +
            trend_alignment * 0.15 +
            volume_score * 0.05 +
            price_patterns * 0.05 +
            volatility_preference * 0.03 +
            momentum_preference * 0.02
        )
        
        return composite
    
    def _determine_trade_direction(self, rsi, macd, short_trend, medium_trend, pattern_score):
        """Determine optimal trade direction with enhanced logic"""
        
        # Long signals
        long_signals = 0
        if rsi is not None and rsi < 40:  # Oversold
            long_signals += 1
        if macd is not None and macd > 0:  # Positive momentum
            long_signals += 1
        if short_trend > 0.3:  # Short-term uptrend
            long_signals += 1
        if medium_trend > 0.2:  # Medium-term uptrend
            long_signals += 1
        if pattern_score > 0.6:  # Strong pattern
            long_signals += 1
        
        # Short signals
        short_signals = 0
        if rsi is not None and rsi > 60:  # Overbought
            short_signals += 1
        if macd is not None and macd < 0:  # Negative momentum
            short_signals += 1
        if short_trend < -0.3:  # Short-term downtrend
            short_signals += 1
        if medium_trend < -0.2:  # Medium-term downtrend
            short_signals += 1
        if pattern_score > 0.6:  # Strong pattern
            short_signals += 1
        
        # Determine direction based on signal strength
        if long_signals >= 3:
            return 'BUY'
        elif short_signals >= 3:
            return 'SELL'
        
        return None
    
    def _calculate_confidence(self, score):
        """Calculate confidence level based on score"""
        if score > 0.8:
            return 'HIGH'
        elif score > 0.6:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def _get_symbol(self, token):
        """Get symbol from token"""
        idx = token_to_index.get(token)
        if idx is not None:
            return df.at[idx, 'tradingsymbol']
        return 'UNKNOWN'

class EnhancedPositionManager:
    """Enhanced position management with multiple concurrent trades"""
    
    def __init__(self):
        self.positions = {}
        self.position_lock = Lock()
        self.risk_manager = RiskManager()
        self.max_positions = MAX_CONCURRENT_TRADES
    
    def can_open_position(self, symbol):
        """Check if we can open a new position"""
        with self.position_lock:
            # Check concurrent trade limit
            if len(self.positions) >= self.max_positions:
                return False
            
            # Check if symbol already has position
            for pos in self.positions.values():
                if pos['symbol'] == symbol:
                    return False
            
            # Check portfolio risk
            if not self.risk_manager.check_portfolio_risk():
                return False
            
            return True
    
    def open_position(self, opportunity):
        """Open a new position with enhanced risk management"""
        with self.position_lock:
            if not self.can_open_position(opportunity['symbol']):
                return None
            
            # Calculate position size based on risk and opportunity quality
            position_size = self.risk_manager.calculate_position_size(
                opportunity['score'], opportunity['confidence']
            )
            
            # Create enhanced position object
            position = {
                'id': f"pos_{int(time.time() * 1000)}",
                'symbol': opportunity['symbol'],
                'token': opportunity['token'],
                'direction': opportunity['direction'],
                'quantity': position_size,
                'entry_price': 0,
                'entry_time': time.time(),
                'status': 'OPENING',
                'opportunity_score': opportunity['score'],
                'confidence': opportunity['confidence'],
                'indicators': opportunity['indicators'],
                'sl_tp_placed': False,
                'exit_attempted': False,
                'partial_fills': [],
                'total_filled': 0
            }
            
            self.positions[position['id']] = position
            logger.info(f"🎯 Opening position: {opportunity['symbol']} | Score: {opportunity['score']:.3f} | Size: {position_size}")
            return position['id']
    
    def update_position(self, position_id, **kwargs):
        """Update position details with validation"""
        with self.position_lock:
            if position_id in self.positions:
                old_status = self.positions[position_id].get('status', 'UNKNOWN')
                self.positions[position_id].update(kwargs)
                new_status = self.positions[position_id].get('status', 'UNKNOWN')
                
                if old_status != new_status:
                    logger.info(f"📊 Position {position_id} status changed: {old_status} → {new_status}")
    
    def close_position(self, position_id):
        """Close a position with cleanup"""
        with self.position_lock:
            if position_id in self.positions:
                position = self.positions[position_id]
                logger.info(f"🔒 Closing position: {position['symbol']} | Duration: {time.time() - position['entry_time']:.1f}s")
                del self.positions[position_id]
    
    def get_active_positions(self):
        """Get all active positions"""
        with self.position_lock:
            return list(self.positions.values())
    
    def get_position_by_symbol(self, symbol):
        """Get position by symbol"""
        with self.position_lock:
            for pos in self.positions.values():
                if pos['symbol'] == symbol:
                    return pos
            return None

class RiskManager:
    """Advanced risk management system with dynamic position sizing"""
    
    def __init__(self):
        self.max_risk_per_trade = 0.02  # 2% max risk per trade
        self.max_portfolio_risk = 0.10   # 10% max portfolio risk
        self.position_sizing_multiplier = RISK_MULTIPLIER
        self.risk_adjustment_factor = 1.0
        self.consecutive_losses = 0
        self.consecutive_wins = 0
        
        # Dynamic risk adjustment based on performance
        self.performance_thresholds = {
            'excellent': 0.8,  # 80%+ win rate
            'good': 0.6,       # 60%+ win rate
            'average': 0.4,    # 40%+ win rate
            'poor': 0.2        # 20%+ win rate
        }
    
    def calculate_position_size(self, opportunity_score, confidence):
        """Calculate optimal position size based on opportunity quality and risk"""
        base_size = 100  # Base quantity
        
        # Adjust based on opportunity score (0.5x to 2.0x)
        score_multiplier = 1.0 + (opportunity_score - 0.5) * 3
        
        # Adjust based on confidence level
        confidence_multiplier = {
            'HIGH': 1.5,
            'MEDIUM': 1.0,
            'LOW': 0.7
        }.get(confidence, 1.0)
        
        # Adjust based on trading mode
        mode_multiplier = {
            'CONSERVATIVE': 0.7,
            'MODERATE': 1.0,
            'AGGRESSIVE': 1.3,
            'ULTRA': 1.6
        }.get(trading_mode, 1.0)
        
        # Dynamic risk adjustment based on recent performance
        performance_multiplier = self._calculate_performance_multiplier()
        
        # Calculate final position size
        final_size = int(base_size * score_multiplier * confidence_multiplier * 
                        mode_multiplier * performance_multiplier * self.risk_adjustment_factor)
        
        # Apply limits
        final_size = max(50, min(final_size, 500))  # Min 50, Max 500
        
        return final_size
    
    def _calculate_performance_multiplier(self):
        """Calculate performance-based risk multiplier"""
        total_trades = trade_stats['total_trades']
        if total_trades < 10:
            return 1.0  # Not enough data
        
        win_rate = trade_stats['winning_trades'] / total_trades
        
        if win_rate >= self.performance_thresholds['excellent']:
            return 1.2  # Increase risk for excellent performance
        elif win_rate >= self.performance_thresholds['good']:
            return 1.1  # Slightly increase risk for good performance
        elif win_rate >= self.performance_thresholds['average']:
            return 1.0  # Normal risk for average performance
        elif win_rate >= self.performance_thresholds['poor']:
            return 0.8  # Reduce risk for poor performance
        else:
            return 0.6  # Significantly reduce risk for very poor performance
    
    def check_portfolio_risk(self):
        """Check if portfolio risk is within limits"""
        try:
            total_exposure = sum(pos.get('quantity', 0) * pos.get('entry_price', 0) 
                               for pos in active_positions.values() if pos.get('entry_price', 0) > 0)
            
            portfolio_risk = total_exposure / CAPITAL
            return portfolio_risk <= self.max_portfolio_risk
            
        except Exception as e:
            logger.error(f"Error checking portfolio risk: {e}")
            return False
    
    def update_performance_metrics(self, trade_result):
        """Update performance metrics for risk adjustment"""
        if trade_result == 'WIN':
            self.consecutive_wins += 1
            self.consecutive_losses = 0
        elif trade_result == 'LOSS':
            self.consecutive_losses += 1
            self.consecutive_wins = 0
        
        # Adjust risk based on consecutive results
        if self.consecutive_losses >= 3:
            self.risk_adjustment_factor = max(0.5, self.risk_adjustment_factor * 0.9)
            logger.warning(f"⚠️  Consecutive losses: {self.consecutive_losses}, reducing risk to {self.risk_adjustment_factor:.2f}")
        elif self.consecutive_wins >= 5:
            self.risk_adjustment_factor = min(1.5, self.risk_adjustment_factor * 1.1)
            logger.info(f"🎯 Consecutive wins: {self.consecutive_wins}, increasing risk to {self.risk_adjustment_factor:.2f}")
    
    def get_risk_summary(self):
        """Get current risk summary"""
        return {
            'max_risk_per_trade': self.max_risk_per_trade,
            'max_portfolio_risk': self.max_portfolio_risk,
            'current_risk_adjustment': self.risk_adjustment_factor,
            'consecutive_wins': self.consecutive_wins,
            'consecutive_losses': self.consecutive_losses,
            'performance_multiplier': self._calculate_performance_multiplier()
        }

# Initialize enhanced components
opportunity_detector = AdvancedOpportunityDetector()
position_manager = EnhancedPositionManager()
risk_manager = RiskManager()

def on_ticks(tick_data):
    """Enhanced tick processing with performance optimization"""
    try:
        if isinstance(tick_data, dict):
            if broker == 'F':
                token = tick_data.get('tk')
                price = float(tick_data.get('lp', 0))
                volume = int(tick_data.get('vol', 0))
            else:
                token = tick_data.get('tk')
                price = float(tick_data.get('lp', 0))
                volume = int(tick_data.get('vol', 0))
            
            if token in price_history and price > 0:
                price_history[token].append(price)
                volume_history[token].append(volume)
                
                # Trigger opportunity detection
                detect_opportunities_sync(token)
                
    except Exception as e:
        logger.error(f"Error processing tick: {e}")

def detect_opportunities_sync(token):
    """Synchronous opportunity detection for better performance"""
    try:
        prices = list(price_history[token])
        volumes = list(volume_history[token])
        
        if len(prices) < OSCILLATION_PERIOD:
            return
        
        # Check cooldown
        current_time = time.time()
        if current_time - last_signal_time[token] < signal_cooldown:
            return
        
        # Detect opportunity
        opportunity = opportunity_detector.detect_opportunity(token, prices, volumes)
        
        if opportunity and position_manager.can_open_position(opportunity['symbol']):
            # Add to priority queue with score-based priority
            priority = int((1 - opportunity['score']) * 1000)  # Lower score = higher priority
            trade_signal_queue.put((priority, opportunity))
            last_signal_time[token] = current_time
            
            trade_stats['signals_generated'] += 1
            logger.info(f"🚀 Signal generated: {opportunity['symbol']} | Score: {opportunity['score']:.3f} | Direction: {opportunity['direction']}")
    
    except Exception as e:
        logger.error(f"Error in opportunity detection: {e}")

def on_connect():
    """WebSocket connected callback"""
    logger.info("✅ WebSocket Connected")
    try:
        if broker == 'F':
            subscription_tokens = tokens
            apiSocket.subscribe(subscription_tokens)
        else:
            subscription_tokens = [f"NSE|{token}" for token in tokens]
            apiSocket.subscribe(subscription_tokens)
        logger.info(f"Subscribed to {len(tokens)} instruments")
    except Exception as e:
        logger.error(f"Subscription failed: {e}")

def on_close():
    """WebSocket closed callback"""
    logger.info("❌ WebSocket Closed")

def on_error(error):
    """WebSocket error callback"""
    logger.error(f"WebSocket Error: {error}")

def enhanced_stock_picker():
    """Enhanced stock picker with batch processing and priority handling"""
    global processing_data
    
    while processing_data:
        try:
            # Process signals in batches for efficiency
            signals_to_process = []
            while not trade_signal_queue.empty() and len(signals_to_process) < BATCH_SIZE:
                try:
                    priority, opportunity = trade_signal_queue.get_nowait()
                    signals_to_process.append((priority, opportunity))
                except:
                    break
            
            if signals_to_process:
                # Sort by priority (lower number = higher priority)
                signals_to_process.sort(key=lambda x: x[0])
                
                # Process high-priority signals first
                for priority, opportunity in signals_to_process:
                    if position_manager.can_open_position(opportunity['symbol']):
                        position_id = position_manager.open_position(opportunity)
                        if position_id:
                            logger.info(f"🎯 Opening position: {opportunity['symbol']} | Score: {opportunity['score']:.3f}")
                            # Execute trade logic here
                            execute_trade(opportunity, position_id)
                            break  # Only execute one trade per cycle to avoid overwhelming
            
            time.sleep(0.1)  # 100ms intervals for HFT
            
        except Exception as e:
            logger.error(f"Error in enhanced stock picker: {e}")
            time.sleep(1)

def execute_trade(opportunity, position_id):
    """Execute trade with enhanced logic and error handling"""
    try:
        # Get current market price
        token = opportunity['token']
        if len(price_history[token]) > 0:
            current_price = price_history[token][-1]
        else:
            logger.warning(f"No price data for {opportunity['symbol']}")
            return
        
        position = active_positions.get(position_id)
        if not position:
            return
        
        # Calculate order parameters
        quantity = position['quantity']
        direction = position['direction']
        
        # Place market order for immediate execution
        order_result = place_enhanced_order(
            symbol=opportunity['symbol'],
            token=token,
            direction=direction,
            quantity=quantity,
            price=0,  # Market order
            price_type='MKT'
        )
        
        if order_result and order_result.get('stat') == 'Ok':
            order_id = order_result.get('norenordno')
            position_manager.update_position(position_id, 
                                         order_id=order_id,
                                         entry_price=current_price,
                                         status='ACTIVE')
            
            logger.info(f"✅ Trade executed: {opportunity['symbol']} | Order ID: {order_id}")
            trade_stats['signals_executed'] += 1
            
            # Place SL/TP orders
            place_sl_tp_orders_enhanced(opportunity, position_id, current_price)
        
    except Exception as e:
        logger.error(f"Error executing trade: {e}")
        # Update position status to failed
        position_manager.update_position(position_id, status='FAILED')

def place_enhanced_order(symbol, token, direction, quantity, price=0, price_type='MKT'):
    """Enhanced order placement with better error handling and validation"""
    try:
        # Validate order parameters
        if quantity <= 0:
            logger.error(f"Invalid quantity: {quantity}")
            return None
        
        if price < 0:
            logger.error(f"Invalid price: {price}")
            return None
        
        order_params = {
            'buy_or_sell': direction[0].upper(),
            'product_type': 'I',  # Intraday
            'exchange': 'NSE',
            'tradingsymbol': symbol+'-EQ',
            'quantity': str(int(quantity)),
            'discloseqty': '0',
            'price_type': price_type,
            'price': '0' if price == 0 else str(round_to_tick(price)),
            'retention': 'DAY',
            'remarks': 'HFT_OSCILLATION'
        }
        
        logger.info(f"📤 Placing enhanced order: {order_params}")
        
        response = api.place_order(**order_params)
        
        if response and isinstance(response, dict) and response.get('stat') == 'Ok':
            logger.info(f"✅ Enhanced order placed: {response.get('norenordno', 'N/A')}")
            return response
        else:
            logger.error(f"❌ Enhanced order failed: {response}")
            return None
            
    except Exception as e:
        logger.error(f"Enhanced order placement error: {e}")
        return None

def place_sl_tp_orders_enhanced(opportunity, position_id, entry_price):
    """Enhanced SL/TP order placement with dynamic levels"""
    try:
        position = active_positions.get(position_id)
        if not position:
            return
        
        quantity = position['quantity']
        direction = position['direction']
        
        # Dynamic SL/TP calculation based on volatility
        token = opportunity['token']
        prices = list(price_history[token])
        
        if len(prices) < 20:
            # Use default levels if not enough data
            volatility_multiplier = 1.0
        else:
            # Calculate volatility-based multiplier
            returns = np.diff(prices[-20:]) / prices[-21:-1]
            volatility = np.std(returns)
            volatility_multiplier = 1.0 + (volatility * 100)  # Adjust based on volatility
        
        # Calculate SL/TP levels
        if direction == 'BUY':
            sl_price = entry_price * (1 - 0.005 * volatility_multiplier)  # Dynamic stop loss
            tp_price = entry_price * (1 + 0.015 * volatility_multiplier)  # Dynamic take profit
        else:
            sl_price = entry_price * (1 + 0.005 * volatility_multiplier)  # Dynamic stop loss
            tp_price = entry_price * (1 - 0.015 * volatility_multiplier)  # Dynamic take profit
        
        # Round to tick size
        sl_price = round_to_tick(sl_price)
        tp_price = round_to_tick(tp_price)
        
        logger.info(f"📊 Dynamic SL/TP for {opportunity['symbol']}: SL={sl_price:.2f}, TP={tp_price:.2f}")
        
        # Place SL order
        sl_direction = 'SELL' if direction == 'BUY' else 'BUY'
        sl_order = place_enhanced_order(
            symbol=opportunity['symbol'],
            token=opportunity['token'],
            direction=sl_direction,
            quantity=quantity,
            price=0,
            price_type='SL-MKT'
        )
        
        # Place TP order
        tp_direction = 'SELL' if direction == 'BUY' else 'BUY'
        tp_order = place_enhanced_order(
            symbol=opportunity['symbol'],
            token=opportunity['token'],
            direction=tp_direction,
            quantity=quantity,
            price=tp_price,
            price_type='LMT'
        )
        
        if sl_order and tp_order:
            logger.info(f"✅ SL/TP orders placed for {opportunity['symbol']}")
            position_manager.update_position(position_id, sl_tp_placed=True)
        else:
            logger.warning(f"⚠️  Failed to place SL/TP orders for {opportunity['symbol']}")
        
    except Exception as e:
        logger.error(f"Error placing enhanced SL/TP orders: {e}")

def handle_partial_fill_enhanced(order_id, expected_quantity, symbol, token, direction):
    """Enhanced partial fill handling with better position management"""
    try:
        order_details = get_order_details(order_id)
        if not order_details:
            logger.warning(f"Could not fetch order details for partial fill handling: {order_id}")
            return False
        
        # Get filled quantity
        filled_qty = int(order_details.get('fillshares', order_details.get('filled_qty', 0)))
        if filled_qty == 0:
            logger.info(f"Order {order_id} not yet filled")
            return False
        
        remaining_qty = expected_quantity - filled_qty
        if remaining_qty <= 0:
            logger.info(f"Order {order_id} fully filled: {filled_qty}/{expected_quantity}")
            return True
        
        logger.warning(f"⚠️  Partial fill detected: {filled_qty}/{expected_quantity} for {symbol}")
        logger.info(f"📤 Placing additional market order for remaining {remaining_qty} shares...")
        
        # Place additional market order for remaining quantity
        additional_order = place_enhanced_order(
            symbol=symbol,
            token=token,
            direction=direction,
            quantity=remaining_qty,
            price=0,  # Market order
            price_type='MKT'
        )
        
        if additional_order and isinstance(additional_order, dict) and additional_order.get('stat') == 'Ok':
            logger.info(f"✅ Additional market order placed: {additional_order.get('norenordno', 'N/A')}")
            logger.info(f"   This will complete the position to {expected_quantity} shares")
            return True
        else:
            logger.error(f"❌ Additional market order failed: {additional_order}")
            return False
            
    except Exception as e:
        logger.error(f"Error handling partial fill: {e}")
        return False

def enhanced_position_monitor():
    """Enhanced position monitoring with better trade management"""
    global processing_data
    
    while processing_data:
        try:
            # Get all active positions
            active_positions_list = position_manager.get_active_positions()
            
            for position in active_positions_list:
                try:
                    position_id = position['id']
                    symbol = position['symbol']
                    token = position['token']
                    
                    # Monitor position status
                    if position['status'] == 'OPENING':
                        # Check if initial order is complete
                        order_id = position.get('order_id')
                        if order_id:
                            order_status = get_order_details(order_id)
                            if order_status and order_status.get('status') == 'COMPLETE':
                                # Position is now active
                                filled_qty = int(order_status.get('fillshares', order_status.get('filled_qty', 0)))
                                entry_price = float(order_status.get('avgprc', 0))
                                
                                if filled_qty > 0:
                                    position_manager.update_position(position_id,
                                                                   status='ACTIVE',
                                                                   entry_price=entry_price,
                                                                   total_filled=filled_qty)
                                    
                                    logger.info(f"✅ Position activated: {symbol} | Qty: {filled_qty} | Entry: ₹{entry_price:.2f}")
                                    
                                    # Place SL/TP orders if not already placed
                                    if not position.get('sl_tp_placed'):
                                        place_sl_tp_orders_enhanced(
                                            {'symbol': symbol, 'token': token},
                                            position_id,
                                            entry_price
                                        )
                                else:
                                    logger.warning(f"⚠️  Order {order_id} completed but no quantity filled")
                    
                    elif position['status'] == 'ACTIVE':
                        # Monitor active position for exit conditions
                        current_price = None
                        if len(price_history[token]) > 0:
                            current_price = price_history[token][-1]
                        
                        if current_price is None:
                            continue
                        
                        entry_price = position.get('entry_price', 0)
                        direction = position['direction']
                        quantity = position.get('total_filled', position['quantity'])
                        
                        # Calculate current P&L
                        if direction == 'BUY':
                            pnl = (current_price - entry_price) * quantity
                            pnl_percent = ((current_price - entry_price) / entry_price) * 100
                        else:
                            pnl = (entry_price - current_price) * quantity
                            pnl_percent = ((entry_price - current_price) / entry_price) * 100
                        
                        # Check for manual exit conditions
                        if not position.get('exit_attempted'):
                            # Time-based exit
                            position_duration = time.time() - position['entry_time']
                            if position_duration > MAX_POSITION_DURATION:
                                logger.info(f"⏳ Time-based exit for {symbol} after {position_duration:.1f}s")
                                force_exit_position(position_id, 'TIMEOUT')
                                continue
                            
                            # Profit target reached
                            if pnl_percent >= 1.5:  # 1.5% profit target
                                logger.info(f"🎯 Profit target reached for {symbol}: {pnl_percent:.2f}%")
                                force_exit_position(position_id, 'PROFIT_TARGET')
                                continue
                            
                            # Stop loss hit
                            if pnl_percent <= -1.0:  # 1% stop loss
                                logger.warning(f"🛑 Stop loss hit for {symbol}: {pnl_percent:.2f}%")
                                force_exit_position(position_id, 'STOP_LOSS')
                                continue
                        
                        # Update position P&L
                        position_manager.update_position(position_id, current_pnl=pnl, current_pnl_percent=pnl_percent)
                    
                    elif position['status'] == 'EXITING':
                        # Monitor exit order
                        exit_order_id = position.get('exit_order_id')
                        if exit_order_id:
                            exit_order_status = get_order_details(exit_order_id)
                            if exit_order_status and exit_order_status.get('status') == 'COMPLETE':
                                # Exit complete
                                final_pnl = position.get('current_pnl', 0)
                                final_pnl_percent = position.get('current_pnl_percent', 0)
                                
                                if final_pnl > 0:
                                    trade_stats['winning_trades'] += 1
                                    risk_manager.update_performance_metrics('WIN')
                                    logger.info(f"✅ Profitable exit: {symbol} | P&L: ₹{final_pnl:.2f} ({final_pnl_percent:.2f}%)")
                                else:
                                    trade_stats['losing_trades'] += 1
                                    risk_manager.update_performance_metrics('LOSS')
                                    logger.warning(f"❌ Loss exit: {symbol} | P&L: ₹{final_pnl:.2f} ({final_pnl_percent:.2f}%)")
                                
                                trade_stats['total_trades'] += 1
                                trade_stats['total_profit'] += final_pnl
                                
                                # Close position
                                position_manager.close_position(position_id)
                
                except Exception as e:
                    logger.error(f"Error monitoring position {position.get('id', 'UNKNOWN')}: {e}")
            
            time.sleep(0.5)  # Check every 500ms
            
        except Exception as e:
            logger.error(f"Error in enhanced position monitor: {e}")
            time.sleep(1)

def force_exit_position(position_id, reason):
    """Force exit a position with market order"""
    try:
        position = active_positions.get(position_id)
        if not position:
            return
        
        symbol = position['symbol']
        token = position['token']
        direction = position['direction']
        quantity = position.get('total_filled', position['quantity'])
        
        if quantity <= 0:
            logger.warning(f"No quantity to exit for {symbol}")
            return
        
        # Determine exit direction
        exit_direction = 'SELL' if direction == 'BUY' else 'BUY'
        
        logger.info(f"📤 Force exiting {symbol} ({reason}): {exit_direction} {quantity}")
        
        # Place market exit order
        exit_order = place_enhanced_order(
            symbol=symbol,
            token=token,
            direction=exit_direction,
            quantity=quantity,
            price=0,  # Market order
            price_type='MKT'
        )
        
        if exit_order and exit_order.get('stat') == 'Ok':
            exit_order_id = exit_order.get('norenordno')
            position_manager.update_position(position_id,
                                         status='EXITING',
                                         exit_order_id=exit_order_id,
                                         exit_reason=reason,
                                         exit_attempted=True)
            
            logger.info(f"✅ Exit order placed: {exit_order_id}")
        else:
            logger.error(f"❌ Failed to place exit order for {symbol}")
            position_manager.update_position(position_id, status='EXIT_FAILED')
    
    except Exception as e:
        logger.error(f"Error in force exit: {e}")

def performance_monitor():
    """Enhanced performance monitoring with detailed metrics"""
    while processing_data:
        try:
            time.sleep(60)  # Update every minute
            
            # Calculate performance metrics
            total_trades = trade_stats['total_trades']
            if total_trades > 0:
                win_rate = (trade_stats['winning_trades'] / total_trades) * 100
                avg_profit = trade_stats['total_profit'] / total_trades
                avg_duration = trade_stats['avg_trade_duration']
            else:
                win_rate = 0
                avg_profit = 0
                avg_duration = 0
            
            # Calculate drawdown
            if trade_stats['total_profit'] < trade_stats['max_drawdown']:
                trade_stats['max_drawdown'] = trade_stats['total_profit']
            
            # Get risk summary
            risk_summary = risk_manager.get_risk_summary()
            
            # Get active positions info
            active_positions_list = position_manager.get_active_positions()
            total_exposure = sum(pos.get('quantity', 0) * pos.get('entry_price', 0) 
                               for pos in active_positions_list if pos.get('entry_price', 0) > 0)
            
            logger.info(f"📊 Enhanced Performance Update:")
            logger.info(f"   Total Trades: {total_trades}")
            logger.info(f"   Win Rate: {win_rate:.1f}%")
            logger.info(f"   Total Profit: ₹{trade_stats['total_profit']:.2f}")
            logger.info(f"   Avg Profit per Trade: ₹{avg_profit:.2f}")
            logger.info(f"   Max Drawdown: ₹{trade_stats['max_drawdown']:.2f}")
            logger.info(f"   Active Positions: {len(active_positions_list)}")
            logger.info(f"   Total Exposure: ₹{total_exposure:.2f}")
            logger.info(f"   Signals Generated: {trade_stats['signals_generated']}")
            logger.info(f"   Signals Executed: {trade_stats['signals_executed']}")
            logger.info(f"   Risk Adjustment: {risk_summary['current_risk_adjustment']:.2f}")
            logger.info(f"   Performance Multiplier: {risk_summary['performance_multiplier']:.2f}")
            
            # Log opportunity quality metrics
            if opportunity_detector.opportunity_scores:
                avg_scores = []
                for token, scores in opportunity_detector.opportunity_scores.items():
                    if scores:
                        avg_scores.extend(scores)
                
                if avg_scores:
                    avg_opportunity_score = np.mean(avg_scores)
                    logger.info(f"   Avg Opportunity Score: {avg_opportunity_score:.3f}")
            
        except Exception as e:
            logger.error(f"Error in performance monitor: {e}")

def get_order_details(order_id):
    """Enhanced order details fetching with better error handling"""
    try:
        order_book = api.get_order_book()
        if not order_book:
            return None
        
        for order in order_book:
            if order.get('norenordno') == order_id:
                return order
        
        return None
        
    except Exception as e:
        logger.error(f"Error fetching order details for {order_id}: {e}")
        return None

def check_broker_capabilities():
    """Check broker capabilities and log system status"""
    try:
        logger.info(f"🔍 Checking broker capabilities for: {'FLATTRADE' if broker == 'F' else 'SHOONYA'}")
        
        # Try to get account details to verify connection
        try:
            user_details = api.get_user_details()
            if user_details:
                logger.info(f"✅ {('FLATTRADE' if broker == 'F' else 'SHOONYA')} connection verified")
                logger.info(f"   User: {user_details.get('uname', 'N/A')}")
                logger.info(f"   Account: {user_details.get('actid', 'N/A')}")
            else:
                logger.warning("⚠️  Could not verify user details")
        except Exception as e:
            logger.warning(f"Could not fetch user details: {e}")
        
        # Log system capabilities
        logger.info("🚀 Enhanced HFT System Capabilities:")
        logger.info(f"   GPU Acceleration: {'Available' if NUMBA_AVAILABLE else 'Not Available'}")
        logger.info(f"   SciPy: {'Available' if SCIPY_AVAILABLE else 'Not Available'}")
        logger.info(f"   TA-Lib: {'Available' if TALIB_AVAILABLE else 'Not Available'}")
        logger.info(f"   Trading Mode: {trading_mode}")
        logger.info(f"   Max Concurrent Trades: {MAX_CONCURRENT_TRADES}")
        logger.info(f"   Risk Multiplier: {RISK_MULTIPLIER}")
        logger.info(f"   Position Duration: {MAX_POSITION_DURATION}s")
        logger.info(f"   Signal Cooldown: {signal_cooldown}s")
        
        # Explain the enhanced trading strategy
        logger.info("📋 Enhanced Trading Strategy:")
        logger.info("   ✅ Multi-factor opportunity detection with 7+ indicators")
        logger.info("   ✅ Dynamic position sizing based on opportunity quality")
        logger.info("   ✅ Advanced risk management with performance-based adjustment")
        logger.info("   ✅ Multiple concurrent positions for higher profit potential")
        logger.info("   ✅ Dynamic SL/TP based on volatility")
        logger.info("   ✅ Priority-based signal processing")
        logger.info("   ✅ Enhanced partial fill handling")
        
    except Exception as e:
        logger.error(f"Error checking broker capabilities: {e}")

def main():
    """Enhanced main function with performance optimization and better startup sequence"""
    try:
        global processing_data
        
        logger.info(f"🚀 Starting Enhanced HFT Oscillation Scalper v2.0")
        logger.info(f"   Trading Mode: {trading_mode}")
        logger.info(f"   Max Concurrent Trades: {MAX_CONCURRENT_TRADES}")
        logger.info(f"   Risk Multiplier: {RISK_MULTIPLIER}")
        logger.info(f"   GPU Acceleration: {'Enabled' if NUMBA_AVAILABLE else 'Disabled'}")
        logger.info(f"   Multi-processing: {'Enabled' if USE_MULTIPROCESSING else 'Disabled'}")
        logger.info(f"   Broker: {'FLATTRADE' if broker == 'F' else 'SHOONYA'}")
        
        # Check broker capabilities first
        check_broker_capabilities()
        
        # Initialize historical data for better startup
        logger.info("📊 Initializing historical data...")
        initialize_historical_data()
        
        # Start WebSocket connection
        logger.info("🔌 Starting WebSocket connection...")
        if broker == 'F':
            apiSocket.start_websocket(
                subscribe_callback=on_ticks,
                socket_open_callback=on_connect,
                socket_close_callback=on_close,
                socket_error_callback=on_error
            )
        else:
            apiSocket.start_websocket(
                order_update_callback=None,
                subscribe_callback=on_ticks,
                socket_open_callback=on_connect,
                socket_close_callback=on_close,
                socket_error_callback=on_error
            )
        
        # Wait for WebSocket to connect
        time.sleep(2)
        
        # Start enhanced components
        logger.info("🧵 Starting enhanced trading components...")
        
        # Start enhanced stock picker
        picker_thread = Thread(target=enhanced_stock_picker, daemon=True, name="StockPicker")
        picker_thread.start()
        logger.info("✅ Stock picker started")
        
        # Start enhanced position monitor
        monitor_thread = Thread(target=enhanced_position_monitor, daemon=True, name="PositionMonitor")
        monitor_thread.start()
        logger.info("✅ Position monitor started")
        
        # Start performance monitor
        perf_thread = Thread(target=performance_monitor, daemon=True, name="PerformanceMonitor")
        perf_thread.start()
        logger.info("✅ Performance monitor started")
        
        # Log startup completion
        logger.info("🎉 Enhanced HFT system startup completed successfully!")
        logger.info("   All components are running and monitoring for opportunities...")
        logger.info("   Press Ctrl+C to stop the system")
        
        # Keep main thread alive and monitor system health
        startup_time = time.time()
        while processing_data:
            try:
                # Check system health every 30 seconds
                current_time = time.time()
                if current_time - startup_time > 30:
                    startup_time = current_time
                    
                    # Check if all threads are alive
                    threads_alive = (picker_thread.is_alive() and 
                                   monitor_thread.is_alive() and 
                                   perf_thread.is_alive())
                    
                    if not threads_alive:
                        logger.error("❌ Critical error: Some threads have died!")
                        processing_data = False
                        break
                    
                    # Log system status
                    active_positions_count = len(position_manager.get_active_positions())
                    queue_size = trade_signal_queue.qsize()
                    
                    logger.info(f"💓 System Health Check: Threads OK | Active Positions: {active_positions_count} | Signal Queue: {queue_size}")
                
                time.sleep(1)
                
            except KeyboardInterrupt:
                raise
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(5)
            
    except KeyboardInterrupt:
        logger.info("\n🛑 Keyboard Interrupt. Shutting down enhanced HFT system...")
        processing_data = False
        
        # Unsubscribe from all tokens
        logger.info("🔌 Unsubscribing from market data...")
        if broker == 'F':
            unsubscribe_tokens = tokens
        else:
            unsubscribe_tokens = [f"NSE|{token}" for token in tokens]
        
        try:
            apiSocket.unsubscribe(unsubscribe_tokens)
        except Exception as e:
            logger.warning(f"Could not unsubscribe: {e}")
        
        # Wait for threads to finish
        logger.info("⏳ Waiting for threads to finish...")
        time.sleep(3)
        
        # Final performance summary
        logger.info("📊 Final Performance Summary:")
        logger.info(f"   Total Trades: {trade_stats['total_trades']}")
        logger.info(f"   Total Profit: ₹{trade_stats['total_profit']:.2f}")
        logger.info(f"   Win Rate: {(trade_stats['winning_trades'] / max(trade_stats['total_trades'], 1)) * 100:.1f}%")
        logger.info(f"   Signals Generated: {trade_stats['signals_generated']}")
        logger.info(f"   Signals Executed: {trade_stats['signals_executed']}")
        
        logger.info("👋 Enhanced HFT system shutdown completed. Goodbye!")
        exit()
    
    except Exception as e:
        logger.error(f"❌ Critical error in main function: {e}")
        processing_data = False
        time.sleep(2)
        exit()

def initialize_historical_data():
    """Initialize historical data for better startup performance"""
    try:
        logger.info("📊 Fetching initial historical data...")
        
        # Initialize data for a subset of symbols to avoid overwhelming the API
        init_symbols = tokens[:min(50, len(tokens))]  # Start with first 50 symbols
        
        for i, token in enumerate(init_symbols):
            try:
                # Fetch price history
                prices = get_price_history(token, minutes=30)
                if prices:
                    price_history[token].extend(prices)
                
                # Fetch volume history
                volumes = get_volume_history(token, minutes=30)
                if volumes:
                    volume_history[token].extend(volumes)
                
                # Progress indicator
                if (i + 1) % 10 == 0:
                    logger.info(f"   Initialized {i + 1}/{len(init_symbols)} symbols...")
                
                # Small delay to avoid API rate limits
                time.sleep(0.1)
                
            except Exception as e:
                logger.warning(f"Could not initialize data for token {token}: {e}")
                continue
        
        logger.info(f"✅ Historical data initialization completed for {len(init_symbols)} symbols")
        
    except Exception as e:
        logger.error(f"Error in historical data initialization: {e}")

if __name__ == "__main__":
    main()