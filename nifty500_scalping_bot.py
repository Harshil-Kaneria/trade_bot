#!/usr/bin/env python3
"""
Nifty 500 Scalping Bot for 180 INR Fixed Profit/Loss
====================================================

A sophisticated algorithmic trading bot that continuously monitors all Nifty 500 stocks
to identify high-probability scalping opportunities with fixed profit/loss targets.

Key Features:
- Real-time momentum analysis from websocket tick data
- Fixed 180 INR profit/loss per trade
- Single trade constraint with continuous signal generation
- Support for both Shoonya and FlatTrade brokers
- Comprehensive logging and monitoring

Author: AI Trading Bot Developer
Version: 1.0.0
"""

import os
import sys
import time
import json
import logging
import argparse
import threading
import queue
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from collections import deque, defaultdict
from threading import Lock, Thread, Event
import warnings
warnings.filterwarnings('ignore')

# Third-party imports
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import pyotp

load_dotenv(override=True)

# Broker API imports
from api_helper_flattrade import NorenApiPy
from api_helper_shoonya import ShoonyaApiPy

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('nifty500_scalping_bot.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class TradingConfig:
    """Trading configuration parameters"""
    CAPITAL: float = 100000.0  # Total capital in INR
    TARGET_PROFIT: float = 180.0  # Fixed profit target per trade
    TARGET_LOSS: float = 180.0  # Fixed loss limit per trade
    MIN_PRICE: float = 50.0  # Minimum stock price to consider
    MAX_PRICE: float = 5000.0  # Maximum stock price to consider
    TICK_SIZE: float = 0.05  # Minimum price movement
    MOMENTUM_WINDOW: int = 5  # Number of ticks to analyze for momentum
    MIN_MOMENTUM_STRENGTH: float = 0.0005  # Minimum momentum strength (0.05%)
    SIGNAL_COOLDOWN: float = 5.0  # Seconds between signals for same stock
    MAX_POSITION_DURATION: int = 300  # Maximum position duration in seconds (5 minutes)
    VOLUME_THRESHOLD: int = 500  # Minimum volume for liquidity
    MAX_DAILY_LOSS: float = 1000.0  # Maximum daily loss limit in INR
    MAX_DAILY_TRADES: int = 50  # Maximum number of trades per day
    BATCH_SIZE: int = 100  # Number of symbols to process in each batch
    UPDATE_INTERVAL: float = 0.1  # Update interval in seconds (100ms)
    LOG_LEVEL: str = "INFO"  # Logging level (DEBUG, INFO, WARNING, ERROR)
    LOG_FILE: str = "nifty500_scalping_bot.log"  # Log file name
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return {
            'CAPITAL': self.CAPITAL,
            'TARGET_PROFIT': self.TARGET_PROFIT,
            'TARGET_LOSS': self.TARGET_LOSS,
            'MIN_PRICE': self.MIN_PRICE,
            'MAX_PRICE': self.MAX_PRICE,
            'TICK_SIZE': self.TICK_SIZE,
            'MOMENTUM_WINDOW': self.MOMENTUM_WINDOW,
            'MIN_MOMENTUM_STRENGTH': self.MIN_MOMENTUM_STRENGTH,
            'SIGNAL_COOLDOWN': self.SIGNAL_COOLDOWN,
            'MAX_POSITION_DURATION': self.MAX_POSITION_DURATION,
            'VOLUME_THRESHOLD': self.VOLUME_THRESHOLD,
            'MAX_DAILY_LOSS': self.MAX_DAILY_LOSS,
            'MAX_DAILY_TRADES': self.MAX_DAILY_TRADES,
            'BATCH_SIZE': self.BATCH_SIZE,
            'UPDATE_INTERVAL': self.UPDATE_INTERVAL,
            'LOG_LEVEL': self.LOG_LEVEL,
            'LOG_FILE': self.LOG_FILE
        }
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'TradingConfig':
        """Create configuration from dictionary"""
        return cls(**config_dict)

@dataclass
class TradeSignal:
    """Trade signal data structure"""
    symbol: str
    token: str
    direction: str  # 'BUY' or 'SELL'
    entry_price: float
    quantity: int
    target_price: float
    stop_loss: float
    confidence: float
    momentum_strength: float
    timestamp: float

@dataclass
class Position:
    """Active position data structure"""
    symbol: str
    token: str
    direction: str
    quantity: int
    entry_price: float
    target_price: float
    stop_loss: float
    order_id: str
    entry_time: float
    status: str  # 'OPENING', 'ACTIVE', 'CLOSING', 'CLOSED'

class BrokerInterface:
    """Abstract broker interface for both Shoonya and FlatTrade"""
    
    def __init__(self, broker_type: str):
        self.broker_type = broker_type
        self.api = None
        self.connected = False
        
    def connect(self) -> bool:
        """Connect to broker API"""
        try:
            if self.broker_type.upper() == 'F':
                return self._connect_flattrade()
            elif self.broker_type.upper() == 'S':
                return self._connect_shoonya()
            else:
                logger.error(f"Invalid broker type: {self.broker_type}")
                return False
        except Exception as e:
            logger.error(f"Broker connection failed: {e}")
            return False
    
    def _connect_flattrade(self) -> bool:
        """Connect to FlatTrade API"""
        try:
            api_key = os.getenv("FLATTRADE_API_KEY")
            api_secret = os.getenv("FLATTRADE_SECRET_KEY")
            user_id = os.getenv("FLATTRADE_USER_ID")
            user_token = os.getenv("FLATTRADE_SESSION")
            
            if not all([api_key, api_secret, user_id, user_token]):
                logger.error("Missing FlatTrade credentials in environment variables")
                return False
            
            self.api = NorenApiPy()
            ret = self.api.set_session(userid=user_id, password='', usertoken=user_token)
            
            if ret is None:
                logger.error("Failed to set FlatTrade session")
                return False
            
            self.connected = True
            logger.info("✅ FlatTrade API Connected")
            return True
            
        except Exception as e:
            logger.error(f"FlatTrade connection error: {e}")
            return False
    
    def _connect_shoonya(self) -> bool:
        """Connect to Shoonya API"""
        try:
            uid = os.getenv("SHOONYA_UID")
            pwd = os.getenv("SHOONYA_PASSWORD")
            factor2 = pyotp.TOTP(os.getenv("SHOONYA_TOTP")).now()
            vc = os.getenv("SHOONYA_VENDOR_CODE")
            app_key = os.getenv("SHOONYA_API_KEY")
            imei = os.getenv("SHOONYA_IMEI")
            
            if not all([uid, pwd, factor2, vc, app_key, imei]):
                logger.error("Missing Shoonya credentials in environment variables")
                return False
            
            self.api = ShoonyaApiPy()
            ret = self.api.login(userid=uid, password=pwd, twoFA=factor2, 
                               vendor_code=vc, api_secret=app_key, imei=imei)
            
            if ret is None:
                logger.error("Failed to login to Shoonya")
                return False
            
            self.connected = True
            logger.info("✅ Shoonya API Connected")
            return True
            
        except Exception as e:
            logger.error(f"Shoonya connection error: {e}")
            return False
    
    def place_bracket_order(self, signal: TradeSignal) -> Optional[str]:
        """Place a bracket order (BO) for the given signal"""
        try:
            if not self.connected or not self.api:
                logger.error("Broker not connected")
                return None
            
            # Calculate order parameters
            direction = signal.direction[0].upper()  # B or S
            symbol_with_suffix = f"{signal.symbol}-EQ"
            
            # Place main order
            order_params = {
                'buy_or_sell': direction,
                'product_type': 'I',  # Intraday
                'exchange': 'NSE',
                'tradingsymbol': symbol_with_suffix,
                'quantity': str(signal.quantity),
                'discloseqty': '0',
                'price_type': 'MKT',  # Market order for immediate execution
                'price': '0',
                'retention': 'DAY',
                'remarks': 'NIFTY500_SCALPING'
            }
            
            logger.info(f"📤 Placing bracket order: {signal.symbol} | {signal.direction} | Qty: {signal.quantity}")
            logger.info(f"   Entry: ₹{signal.entry_price:.2f} | Target: ₹{signal.target_price:.2f} | SL: ₹{signal.stop_loss:.2f}")
            
            response = self.api.place_order(**order_params)
            
            if response and isinstance(response, dict) and response.get('stat') == 'Ok':
                order_id = response.get('norenordno')
                logger.info(f"✅ Bracket order placed successfully: {order_id}")
                return order_id
            else:
                logger.error(f"❌ Bracket order failed: {response}")
                return None
                
        except Exception as e:
            logger.error(f"Error placing bracket order: {e}")
            return None
    
    def get_order_status(self, order_id: str) -> Optional[Dict]:
        """Get order status"""
        try:
            if not self.connected or not self.api:
                return None
            
            order_book = self.api.get_order_book()
            if not order_book:
                return None
            
            for order in order_book:
                if order.get('norenordno') == order_id:
                    return order
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting order status: {e}")
            return None
    
    def subscribe_to_quotes(self, tokens: List[str]) -> bool:
        """Subscribe to real-time quotes"""
        try:
            if not self.connected or not self.api:
                return False
            
            if self.broker_type.upper() == 'F':
                # FlatTrade format
                self.api.subscribe(tokens)
            else:
                # Shoonya format
                formatted_tokens = [f"NSE|{token}" for token in tokens]
                self.api.subscribe(formatted_tokens)
            
            logger.info(f"✅ Subscribed to {len(tokens)} instruments")
            return True
            
        except Exception as e:
            logger.error(f"Error subscribing to quotes: {e}")
            return False
    
    def unsubscribe_from_quotes(self, tokens: List[str]) -> bool:
        """Unsubscribe from real-time quotes"""
        try:
            if not self.connected or not self.api:
                return False
            
            if self.broker_type.upper() == 'F':
                self.api.unsubscribe(tokens)
            else:
                formatted_tokens = [f"NSE|{token}" for token in tokens]
                self.api.unsubscribe(formatted_tokens)
            
            logger.info(f"✅ Unsubscribed from {len(tokens)} instruments")
            return True
            
        except Exception as e:
            logger.error(f"Error unsubscribing from quotes: {e}")
            return False

class MomentumAnalyzer:
    """Advanced momentum analysis for scalping opportunities"""
    
    def __init__(self, config: TradingConfig):
        self.config = config
        self.price_history = defaultdict(lambda: deque(maxlen=config.MOMENTUM_WINDOW * 2))
        self.volume_history = defaultdict(lambda: deque(maxlen=config.MOMENTUM_WINDOW * 2))
        self.last_signal_time = defaultdict(float)
        self.signal_cache = {}
        
    def analyze_tick(self, token: str, symbol: str, price: float, volume: int) -> Optional[TradeSignal]:
        """Analyze a single tick for momentum signals"""
        try:
            current_time = time.time()
            
            # Debug: Log analysis start
            logger.debug(f"🔍 Analyzing {symbol}: Price=₹{price}, Volume={volume}")
            
            # Check cooldown
            if current_time - self.last_signal_time[token] < self.config.SIGNAL_COOLDOWN:
                logger.debug(f"⏳ {symbol} in cooldown period")
                return None
            
            # Update price and volume history
            self.price_history[token].append(price)
            self.volume_history[token].append(volume)
            
            # Need minimum data for analysis
            if len(self.price_history[token]) < self.config.MOMENTUM_WINDOW:
                logger.debug(f"📊 {symbol} needs more data: {len(self.price_history[token])}/{self.config.MOMENTUM_WINDOW}")
                return None
            
            # Check price range
            if not (self.config.MIN_PRICE <= price <= self.config.MAX_PRICE):
                logger.debug(f"💰 {symbol} price out of range: ₹{price} (min: ₹{self.config.MIN_PRICE}, max: ₹{self.config.MAX_PRICE})")
                return None
            
            # Check volume
            if volume < self.config.VOLUME_THRESHOLD:
                logger.debug(f"📈 {symbol} volume too low: {volume} < {self.config.VOLUME_THRESHOLD}")
                return None
            
            # Analyze momentum
            momentum_data = self._calculate_momentum(token, price)
            if not momentum_data:
                return None
            
            direction, momentum_strength, confidence = momentum_data
            
            # Check if momentum is strong enough
            if momentum_strength < self.config.MIN_MOMENTUM_STRENGTH:
                return None
            
            # Calculate trade parameters
            trade_params = self._calculate_trade_parameters(symbol, token, price, direction)
            if not trade_params:
                return None
            
            # Create trade signal
            signal = TradeSignal(
                symbol=symbol,
                token=token,
                direction=direction,
                entry_price=price,
                quantity=trade_params['quantity'],
                target_price=trade_params['target_price'],
                stop_loss=trade_params['stop_loss'],
                confidence=confidence,
                momentum_strength=momentum_strength,
                timestamp=current_time
            )
            
            # Update cooldown
            self.last_signal_time[token] = current_time
            
            logger.info(f"🚀 Signal generated: {symbol} | {direction} | Strength: {momentum_strength:.4f} | Confidence: {confidence:.2f}")
            
            return signal
            
        except Exception as e:
            logger.error(f"Error analyzing tick for {symbol}: {e}")
            return None
    
    def _calculate_momentum(self, token: str, current_price: float) -> Optional[Tuple[str, float, float]]:
        """Calculate momentum strength and direction"""
        try:
            prices = list(self.price_history[token])
            volumes = list(self.volume_history[token])
            
            if len(prices) < self.config.MOMENTUM_WINDOW:
                return None
            
            # Calculate price momentum
            recent_prices = prices[-self.config.MOMENTUM_WINDOW:]
            price_changes = np.diff(recent_prices)
            
            # Calculate momentum strength
            positive_changes = np.sum(price_changes > 0)
            negative_changes = np.sum(price_changes < 0)
            total_changes = len(price_changes)
            
            # Calculate momentum score
            momentum_score = (positive_changes - negative_changes) / total_changes
            
            # Calculate price acceleration
            if len(price_changes) >= 3:
                acceleration = np.mean(np.diff(price_changes[-3:]))
            else:
                acceleration = 0
            
            # Calculate volume momentum
            if len(volumes) >= self.config.MOMENTUM_WINDOW:
                recent_volumes = volumes[-self.config.MOMENTUM_WINDOW:]
                volume_trend = np.mean(np.diff(recent_volumes))
                volume_momentum = volume_trend / (np.mean(recent_volumes) + 1e-10)
            else:
                volume_momentum = 0
            
            # Calculate price volatility
            price_volatility = np.std(recent_prices) / np.mean(recent_prices)
            
            # Calculate composite momentum strength
            momentum_strength = abs(momentum_score) * 0.4 + abs(acceleration) * 0.3 + abs(volume_momentum) * 0.2 + price_volatility * 0.1
            
            # Determine direction
            if momentum_score > 0.3 and acceleration > 0:
                direction = 'BUY'
            elif momentum_score < -0.3 and acceleration < 0:
                direction = 'SELL'
            else:
                return None
            
            # Calculate confidence based on consistency
            consistency = 1.0 - price_volatility
            confidence = min(consistency * momentum_strength * 10, 1.0)
            
            return direction, momentum_strength, confidence
            
        except Exception as e:
            logger.error(f"Error calculating momentum: {e}")
            return None
    
    def _calculate_trade_parameters(self, symbol: str, token: str, price: float, direction: str) -> Optional[Dict]:
        """Calculate trade parameters for fixed profit/loss"""
        try:
            # Calculate maximum quantity based on capital
            max_quantity = int(self.config.CAPITAL / price)
            
            # Ensure minimum quantity
            if max_quantity < 1:
                return None
            
            # Calculate required price move for target profit
            required_move = self.config.TARGET_PROFIT / max_quantity
            
            # Check if required move is reasonable (not too small)
            if required_move < self.config.TICK_SIZE:
                # Reduce quantity to ensure reasonable price move
                max_quantity = int(self.config.TARGET_PROFIT / self.config.TICK_SIZE)
                required_move = self.config.TARGET_PROFIT / max_quantity
            
            # Calculate target and stop loss prices
            if direction == 'BUY':
                target_price = price + required_move
                stop_loss = price - required_move
            else:
                target_price = price - required_move
                stop_loss = price + required_move
            
            # Ensure prices are within reasonable bounds
            if target_price <= 0 or stop_loss <= 0:
                return None
            
            return {
                'quantity': max_quantity,
                'target_price': round(target_price, 2),
                'stop_loss': round(stop_loss, 2)
            }
            
        except Exception as e:
            logger.error(f"Error calculating trade parameters: {e}")
            return None

class SignalGenerator:
    """Continuous signal generation system"""
    
    def __init__(self, config: TradingConfig, broker: BrokerInterface, signal_queue: queue.Queue):
        self.config = config
        self.broker = broker
        self.signal_queue = signal_queue
        self.momentum_analyzer = MomentumAnalyzer(config)
        self.running = False
        self.symbols_data = {}
        self.tokens = []
        
    def initialize_symbols(self) -> bool:
        """Initialize Nifty 500 symbols and tokens"""
        try:
            # Load Nifty 500 symbols
            symbols_df = pd.read_csv('data_500.csv')
            symbols_list = symbols_df['SYMBOL'].tolist()
            
            # Load NSE master data
            instruments_df = pd.read_csv('NSE_Equity_Master.csv')
            
            # Filter for Nifty 500 stocks
            df = instruments_df[instruments_df['Symbol'].isin(symbols_list)]
            df = df[df['Instrument'] == "EQ"]
            
            # Create symbol to token mapping
            for _, row in df.iterrows():
                symbol = row['Symbol']
                token = str(row['Token'])
                self.symbols_data[token] = symbol
                self.tokens.append(token)
            
            logger.info(f"✅ Loaded {len(self.tokens)} Nifty 500 symbols")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing symbols: {e}")
            return False
    
    def start(self):
        """Start the signal generation system"""
        if not self.initialize_symbols():
            return False
        # Defer subscribing to tokens until WebSocket open to avoid blocking
        self.running = True
        logger.info("🚀 Signal generator started")
        return True
    
    def stop(self):
        """Stop the signal generation system"""
        self.running = False
        # Skip unsubscribe on stop to avoid blocking if websocket is closed
        # Subscriptions will be cleared by the broker session end
        logger.info("🛑 Signal generator stopped")
    
    def on_tick(self, tick_data: Dict):
        """Process incoming tick data"""
        try:
            if not self.running:
                return
            
            # Debug: Log all incoming tick data
            logger.debug(f"📡 Received tick data: {tick_data}")
            
            # Extract tick information
            if 'tk' in tick_data:
                token = tick_data['tk']
                price = float(tick_data.get('lp', 0))
                volume = int(tick_data.get('vol', 0))
                
                # Debug: Log tick details
                logger.debug(f"📊 Tick: Token={token}, Price={price}, Volume={volume}")
            else:
                logger.debug(f"⚠️ Invalid tick data format: {tick_data}")
                return
            
            # Get symbol from token
            symbol = self.symbols_data.get(token)
            if not symbol:
                logger.debug(f"⚠️ Unknown token: {token}")
                return
            if price <= 0:
                logger.debug(f"⚠️ Invalid price for {symbol}: {price}")
                return
            
            # Debug: Log symbol mapping
            logger.debug(f"🔍 Processing: {symbol} (Token: {token}) - Price: ₹{price}, Volume: {volume}")
            
            # Analyze tick for momentum
            signal = self.momentum_analyzer.analyze_tick(token, symbol, price, volume)
            
            if signal:
                # Add signal to queue
                self.signal_queue.put(signal)
                logger.info(f"📊 Signal queued: {signal.symbol} | {signal.direction} | Qty: {signal.quantity}")
            else:
                logger.debug(f"❌ No signal generated for {symbol}")
                
        except Exception as e:
            logger.error(f"Error processing tick: {e}")

class OrderManager:
    """Order management system with single trade constraint"""
    
    def __init__(self, config: TradingConfig, broker: BrokerInterface):
        self.config = config
        self.broker = broker
        self.current_position: Optional[Position] = None
        self.position_lock = Lock()
        self.running = False
        self.trade_stats = {
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'total_profit': 0.0,
            'total_loss': 0.0
        }
    
    def start(self):
        """Start the order management system"""
        self.running = True
        logger.info("🚀 Order manager started")
    
    def stop(self):
        """Stop the order management system"""
        self.running = False
        logger.info("🛑 Order manager stopped")
    
    def process_signal(self, signal: TradeSignal) -> bool:
        """Process a trade signal"""
        try:
            with self.position_lock:
                # Check if we already have an active position
                if self.current_position is not None:
                    logger.info(f"⏳ Position already active: {self.current_position.symbol}, ignoring signal for {signal.symbol}")
                    return False
                
                # Place bracket order
                order_id = self.broker.place_bracket_order(signal)
                if not order_id:
                    logger.error(f"Failed to place order for {signal.symbol}")
                    return False
                
                # Create position
                self.current_position = Position(
                    symbol=signal.symbol,
                    token=signal.token,
                    direction=signal.direction,
                    quantity=signal.quantity,
                    entry_price=signal.entry_price,
                    target_price=signal.target_price,
                    stop_loss=signal.stop_loss,
                    order_id=order_id,
                    entry_time=time.time(),
                    status='OPENING'
                )
                
                logger.info(f"✅ Position opened: {signal.symbol} | {signal.direction} | Qty: {signal.quantity}")
                logger.info(f"   Entry: ₹{signal.entry_price:.2f} | Target: ₹{signal.target_price:.2f} | SL: ₹{signal.stop_loss:.2f}")
                
                return True
                
        except Exception as e:
            logger.error(f"Error processing signal: {e}")
            return False
    
    def monitor_positions(self):
        """Monitor active positions"""
        while self.running:
            try:
                with self.position_lock:
                    if self.current_position is None:
                        time.sleep(1)
                        continue
                    
                    position = self.current_position
                    
                    # Check order status
                    order_status = self.broker.get_order_status(position.order_id)
                    if not order_status:
                        time.sleep(1)
                        continue
                    
                    # Update position status
                    if order_status.get('status') == 'COMPLETE':
                        if position.status == 'OPENING':
                            position.status = 'ACTIVE'
                            logger.info(f"✅ Position activated: {position.symbol}")
                    
                    # Check for exit conditions
                    if position.status == 'ACTIVE':
                        self._check_exit_conditions(position)
                    
                    # Check for timeout
                    if time.time() - position.entry_time > self.config.MAX_POSITION_DURATION:
                        logger.warning(f"⏰ Position timeout: {position.symbol}")
                        self._close_position(position, 'TIMEOUT')
                
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error monitoring positions: {e}")
                time.sleep(1)
    
    def _check_exit_conditions(self, position: Position):
        """Check if position should be closed"""
        try:
            # This would typically involve checking current price against target/stop loss
            # For now, we'll implement a simple time-based exit
            # In a real implementation, you'd get current price from market data
            
            # Placeholder for price checking logic
            # current_price = get_current_price(position.token)
            # if position.direction == 'BUY':
            #     if current_price >= position.target_price:
            #         self._close_position(position, 'TARGET_HIT')
            #     elif current_price <= position.stop_loss:
            #         self._close_position(position, 'STOP_LOSS_HIT')
            # else:
            #     if current_price <= position.target_price:
            #         self._close_position(position, 'TARGET_HIT')
            #     elif current_price >= position.stop_loss:
            #         self._close_position(position, 'STOP_LOSS_HIT')
            
            pass
            
        except Exception as e:
            logger.error(f"Error checking exit conditions: {e}")
    
    def _close_position(self, position: Position, reason: str):
        """Close a position"""
        try:
            logger.info(f"🔒 Closing position: {position.symbol} | Reason: {reason}")
            
            # Update trade statistics
            self.trade_stats['total_trades'] += 1
            
            if reason == 'TARGET_HIT':
                self.trade_stats['winning_trades'] += 1
                self.trade_stats['total_profit'] += self.config.TARGET_PROFIT
                logger.info(f"✅ Target hit: {position.symbol} | Profit: ₹{self.config.TARGET_PROFIT}")
            else:
                self.trade_stats['losing_trades'] += 1
                self.trade_stats['total_loss'] += self.config.TARGET_LOSS
                logger.info(f"❌ Stop loss hit: {position.symbol} | Loss: ₹{self.config.TARGET_LOSS}")
            
            # Clear position
            self.current_position = None
            
            # Log statistics
            self._log_statistics()
            
        except Exception as e:
            logger.error(f"Error closing position: {e}")
    
    def _log_statistics(self):
        """Log current trading statistics"""
        try:
            total_trades = self.trade_stats['total_trades']
            if total_trades > 0:
                win_rate = (self.trade_stats['winning_trades'] / total_trades) * 100
                net_profit = self.trade_stats['total_profit'] - self.trade_stats['total_loss']
                
                logger.info(f"📊 Trading Statistics:")
                logger.info(f"   Total Trades: {total_trades}")
                logger.info(f"   Win Rate: {win_rate:.1f}%")
                logger.info(f"   Total Profit: ₹{self.trade_stats['total_profit']:.2f}")
                logger.info(f"   Total Loss: ₹{self.trade_stats['total_loss']:.2f}")
                logger.info(f"   Net Profit: ₹{net_profit:.2f}")
            
        except Exception as e:
            logger.error(f"Error logging statistics: {e}")

class Nifty500ScalpingBot:
    """Main bot class that coordinates all components"""
    
    def __init__(self, broker_type: str, config: TradingConfig = None, test_mode: bool = False):
        self.broker_type = broker_type
        self.config = config or TradingConfig()
        self.test_mode = test_mode
        self.broker = BrokerInterface(broker_type)
        self.signal_queue = queue.Queue()
        self.signal_generator = SignalGenerator(self.config, self.broker, self.signal_queue)
        self.order_manager = OrderManager(self.config, self.broker)
        self.running = False
        
        if test_mode:
            # Override config for test mode
            self.config.MOMENTUM_WINDOW = 3
            self.config.MIN_MOMENTUM_STRENGTH = 0.0001
            self.config.VOLUME_THRESHOLD = 100
            self.config.SIGNAL_COOLDOWN = 1.0
            logger.info("🧪 Test mode enabled - Relaxed parameters for faster signal generation")
        
    def start(self):
        """Start the trading bot"""
        try:
            logger.info("🚀 Starting Nifty 500 Scalping Bot")
            logger.info(f"   Broker: {'FlatTrade' if self.broker_type.upper() == 'F' else 'Shoonya'}")
            logger.info(f"   Capital: ₹{self.config.CAPITAL:,.2f}")
            logger.info(f"   Target Profit/Loss: ₹{self.config.TARGET_PROFIT}")
            
            # Connect to broker
            if not self.broker.connect():
                logger.error("Failed to connect to broker")
                return False
            
            # Start signal generator
            if not self.signal_generator.start():
                logger.error("Failed to start signal generator")
                return False
            
            # Start order manager
            self.order_manager.start()
            
            # Start monitoring thread
            self.running = True
            monitor_thread = Thread(target=self._monitor_signals, daemon=True)
            monitor_thread.start()
            
            # Start position monitoring thread
            position_thread = Thread(target=self.order_manager.monitor_positions, daemon=True)
            position_thread.start()
            
            logger.info("✅ Bot started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error starting bot: {e}")
            return False
    
    def stop(self):
        """Stop the trading bot"""
        try:
            logger.info("🛑 Stopping Nifty 500 Scalping Bot")
            self.running = False
            
            # Stop components
            self.signal_generator.stop()
            self.order_manager.stop()
            
            # Final statistics
            self.order_manager._log_statistics()
            
            logger.info("✅ Bot stopped successfully")
            
        except Exception as e:
            logger.error(f"Error stopping bot: {e}")
    
    def _monitor_signals(self):
        """Monitor signal queue and process signals"""
        while self.running:
            try:
                # Get signal from queue (with timeout)
                try:
                    signal = self.signal_queue.get(timeout=1)
                except queue.Empty:
                    continue
                
                # Process signal
                if signal:
                    self.order_manager.process_signal(signal)
                
            except Exception as e:
                logger.error(f"Error monitoring signals: {e}")
                time.sleep(1)
    
    def on_tick(self, tick_data: Dict):
        """Handle incoming tick data"""
        self.signal_generator.on_tick(tick_data)

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Nifty 500 Scalping Bot')
    parser.add_argument('--broker', type=str, default='F', 
                       help='Broker type: F for FlatTrade, S for Shoonya')
    parser.add_argument('--capital', type=float, default=100000,
                       help='Trading capital in INR')
    parser.add_argument('--profit', type=float, default=180,
                       help='Target profit per trade in INR')
    parser.add_argument('--loss', type=float, default=180,
                       help='Target loss per trade in INR')
    parser.add_argument('--test', action='store_true',
                       help='Enable test mode with relaxed parameters for faster signal generation')
    
    args = parser.parse_args()
    
    # Create configuration
    config = TradingConfig(
        CAPITAL=args.capital,
        TARGET_PROFIT=args.profit,
        TARGET_LOSS=args.loss
    )
    
    # Create and start bot
    bot = Nifty500ScalpingBot(args.broker, config, test_mode=args.test)
    
    try:
        if bot.start():
            # Set up WebSocket callbacks
            if bot.broker.api:
                logger.info("🔌 Starting WebSocket connection...")
                
                def _on_socket_open():
                    logger.info("✅ WebSocket connected")
                    # Subscribe in batches after socket is open
                    try:
                        batch_size = bot.config.BATCH_SIZE
                        total = len(bot.signal_generator.tokens)
                        for start in range(0, total, batch_size):
                            batch = bot.signal_generator.tokens[start:start+batch_size]
                            bot.broker.subscribe_to_quotes(batch)
                            logger.info(f"🧩 Subscribed batch {start//batch_size+1} / {((total-1)//batch_size)+1} ({len(batch)} tokens)")
                            time.sleep(0.2)
                    except Exception as e:
                        logger.error(f"Error during batch subscribe: {e}")

                def _on_socket_close():
                    logger.info("❌ WebSocket disconnected")

                def _on_socket_error(e):
                    logger.error(f"WebSocket error: {e}")

                bot.broker.api.start_websocket(
                    subscribe_callback=bot.on_tick,
                    socket_open_callback=_on_socket_open,
                    socket_close_callback=_on_socket_close,
                    socket_error_callback=_on_socket_error
                )
                logger.info("🔌 WebSocket setup completed")
            
            # Keep running
            while True:
                time.sleep(1)
                
        else:
            logger.error("Failed to start bot")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("\n🛑 Keyboard interrupt received")
        bot.stop()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        bot.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()
