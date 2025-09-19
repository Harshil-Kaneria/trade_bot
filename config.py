#!/usr/bin/env python3
"""
Configuration file for Nifty 500 Scalping Bot
=============================================

This file contains all configuration parameters for the trading bot.
Modify these values according to your trading preferences and risk tolerance.
"""

from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class TradingConfig:
    """Main trading configuration parameters"""
    
    # Capital and Risk Management
    CAPITAL: float = 90000.0  # Total trading capital in INR
    TARGET_PROFIT: float = 180.0  # Fixed profit target per trade in INR
    TARGET_LOSS: float = 180.0  # Fixed loss limit per trade in INR
    
    # Stock Filtering
    MIN_PRICE: float = 50.0  # Minimum stock price to consider (INR)
    MAX_PRICE: float = 5000.0  # Maximum stock price to consider (INR)
    TICK_SIZE: float = 0.05  # Minimum price movement (INR)
    
    # Momentum Analysis
    MOMENTUM_WINDOW: int = 10  # Number of ticks to analyze for momentum
    MIN_MOMENTUM_STRENGTH: float = 0.001  # Minimum momentum strength (0.1%)
    SIGNAL_COOLDOWN: float = 5.0  # Seconds between signals for same stock
    
    # Position Management
    MAX_POSITION_DURATION: int = 300  # Maximum position duration in seconds (5 minutes)
    VOLUME_THRESHOLD: int = 1000  # Minimum volume for liquidity
    
    # Risk Management
    MAX_DAILY_LOSS: float = 1000.0  # Maximum daily loss limit in INR
    MAX_DAILY_TRADES: int = 50  # Maximum number of trades per day
    
    # Performance Optimization
    BATCH_SIZE: int = 100  # Number of symbols to process in each batch
    UPDATE_INTERVAL: float = 0.1  # Update interval in seconds (100ms)
    
    # Logging
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

# Default configuration
DEFAULT_CONFIG = TradingConfig()

# Conservative configuration (lower risk)
CONSERVATIVE_CONFIG = TradingConfig(
    CAPITAL=50000.0,
    TARGET_PROFIT=90.0,
    TARGET_LOSS=90.0,
    MIN_MOMENTUM_STRENGTH=0.002,  # Higher threshold for more conservative signals
    MAX_POSITION_DURATION=180,  # Shorter position duration
    MAX_DAILY_LOSS=500.0,
    MAX_DAILY_TRADES=25
)

# Aggressive configuration (higher risk)
AGGRESSIVE_CONFIG = TradingConfig(
    CAPITAL=200000.0,
    TARGET_PROFIT=360.0,
    TARGET_LOSS=360.0,
    MIN_MOMENTUM_STRENGTH=0.0005,  # Lower threshold for more signals
    MAX_POSITION_DURATION=600,  # Longer position duration
    MAX_DAILY_LOSS=2000.0,
    MAX_DAILY_TRADES=100
)

# Broker-specific configurations
BROKER_CONFIGS = {
    'FLATTRADE': {
        'websocket_url': 'wss://piconnect.flattrade.in/PiConnectWSTp/',
        'api_url': 'https://piconnect.flattrade.in/PiConnectTP/',
        'order_type': 'BO',  # Bracket Order
        'product_type': 'I'  # Intraday
    },
    'SHOONYA': {
        'websocket_url': 'wss://api.shoonya.com/NorenWSTP/',
        'api_url': 'https://api.shoonya.com/NorenWClientTP/',
        'order_type': 'BO',  # Bracket Order
        'product_type': 'I'  # Intraday
    }
}

# Market hours configuration (IST)
MARKET_HOURS = {
    'start_time': '09:15:00',
    'end_time': '15:30:00',
    'lunch_start': '12:00:00',
    'lunch_end': '12:15:00'
}

# Nifty 500 specific settings
NIFTY500_CONFIG = {
    'data_file': 'data_500.csv',
    'master_file': 'NSE_Equity_Master.csv',
    'exchange': 'NSE',
    'instrument_type': 'EQ',
    'symbol_suffix': '-EQ'
}
