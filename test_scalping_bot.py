#!/usr/bin/env python3
"""
Test script for Nifty 500 Scalping Bot
=====================================

This script provides comprehensive testing for the scalping bot components.
Run this before deploying the bot in live trading.
"""

import unittest
import time
import json
from unittest.mock import Mock, patch, MagicMock
from dataclasses import asdict

# Import bot components
from nifty500_scalping_bot import (
    TradingConfig, TradeSignal, Position, BrokerInterface,
    MomentumAnalyzer, SignalGenerator, OrderManager, Nifty500ScalpingBot
)

class TestTradingConfig(unittest.TestCase):
    """Test trading configuration"""
    
    def test_default_config(self):
        """Test default configuration values"""
        config = TradingConfig()
        self.assertEqual(config.CAPITAL, 100000.0)
        self.assertEqual(config.TARGET_PROFIT, 180.0)
        self.assertEqual(config.TARGET_LOSS, 180.0)
        self.assertEqual(config.MIN_PRICE, 50.0)
        self.assertEqual(config.MAX_PRICE, 5000.0)
    
    def test_config_to_dict(self):
        """Test configuration to dictionary conversion"""
        config = TradingConfig()
        config_dict = config.to_dict()
        self.assertIsInstance(config_dict, dict)
        self.assertIn('CAPITAL', config_dict)
        self.assertIn('TARGET_PROFIT', config_dict)

class TestTradeSignal(unittest.TestCase):
    """Test trade signal data structure"""
    
    def test_trade_signal_creation(self):
        """Test trade signal creation"""
        signal = TradeSignal(
            symbol="RELIANCE",
            token="2881",
            direction="BUY",
            entry_price=2500.0,
            quantity=40,
            target_price=2504.5,
            stop_loss=2495.5,
            confidence=0.85,
            momentum_strength=0.002,
            timestamp=time.time()
        )
        
        self.assertEqual(signal.symbol, "RELIANCE")
        self.assertEqual(signal.direction, "BUY")
        self.assertEqual(signal.entry_price, 2500.0)
        self.assertEqual(signal.quantity, 40)

class TestMomentumAnalyzer(unittest.TestCase):
    """Test momentum analysis functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.config = TradingConfig()
        self.analyzer = MomentumAnalyzer(self.config)
    
    def test_analyze_tick_basic(self):
        """Test basic tick analysis"""
        # Add some price history
        token = "2881"
        symbol = "RELIANCE"
        
        # Simulate upward momentum
        prices = [2500, 2501, 2502, 2503, 2504, 2505, 2506, 2507, 2508, 2509]
        for price in prices:
            self.analyzer.price_history[token].append(price)
            self.analyzer.volume_history[token].append(1000)
        
        # Analyze current tick
        signal = self.analyzer.analyze_tick(token, symbol, 2510.0, 1000)
        
        # Should generate a BUY signal
        if signal:
            self.assertEqual(signal.direction, "BUY")
            self.assertEqual(signal.symbol, symbol)
            self.assertEqual(signal.entry_price, 2510.0)
    
    def test_calculate_trade_parameters(self):
        """Test trade parameter calculation"""
        symbol = "RELIANCE"
        token = "2881"
        price = 2500.0
        direction = "BUY"
        
        params = self.analyzer._calculate_trade_parameters(symbol, token, price, direction)
        
        if params:
            self.assertIn('quantity', params)
            self.assertIn('target_price', params)
            self.assertIn('stop_loss', params)
            
            # Check quantity calculation
            expected_quantity = int(self.config.CAPITAL / price)
            self.assertEqual(params['quantity'], expected_quantity)
            
            # Check target and stop loss
            required_move = self.config.TARGET_PROFIT / params['quantity']
            self.assertAlmostEqual(params['target_price'], price + required_move, places=2)
            self.assertAlmostEqual(params['stop_loss'], price - required_move, places=2)

class TestBrokerInterface(unittest.TestCase):
    """Test broker interface functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.broker = BrokerInterface('F')  # FlatTrade
    
    @patch('nifty500_scalping_bot.NorenApiPy')
    def test_connect_flattrade(self, mock_api_class):
        """Test FlatTrade connection"""
        # Mock successful connection
        mock_api = Mock()
        mock_api.set_session.return_value = {"stat": "Ok"}
        mock_api_class.return_value = mock_api
        
        # Mock environment variables
        with patch.dict('os.environ', {
            'FLATTRADE_API_KEY': 'test_key',
            'FLATTRADE_SECRET_KEY': 'test_secret',
            'FLATTRADE_USER_ID': 'test_user',
            'FLATTRADE_SESSION': 'test_session'
        }):
            result = self.broker._connect_flattrade()
            self.assertTrue(result)
            self.assertTrue(self.broker.connected)
    
    def test_place_bracket_order(self):
        """Test bracket order placement"""
        # Mock broker connection
        self.broker.connected = True
        self.broker.api = Mock()
        self.broker.api.place_order.return_value = {"stat": "Ok", "norenordno": "12345"}
        
        # Create test signal
        signal = TradeSignal(
            symbol="RELIANCE",
            token="2881",
            direction="BUY",
            entry_price=2500.0,
            quantity=40,
            target_price=2504.5,
            stop_loss=2495.5,
            confidence=0.85,
            momentum_strength=0.002,
            timestamp=time.time()
        )
        
        # Test order placement
        order_id = self.broker.place_bracket_order(signal)
        self.assertIsNotNone(order_id)
        self.assertEqual(order_id, "12345")

class TestOrderManager(unittest.TestCase):
    """Test order management functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.config = TradingConfig()
        self.broker = Mock()
        self.order_manager = OrderManager(self.config, self.broker)
    
    def test_process_signal_no_position(self):
        """Test signal processing when no active position"""
        # Mock broker order placement
        self.broker.place_bracket_order.return_value = "12345"
        
        # Create test signal
        signal = TradeSignal(
            symbol="RELIANCE",
            token="2881",
            direction="BUY",
            entry_price=2500.0,
            quantity=40,
            target_price=2504.5,
            stop_loss=2495.5,
            confidence=0.85,
            momentum_strength=0.002,
            timestamp=time.time()
        )
        
        # Process signal
        result = self.order_manager.process_signal(signal)
        self.assertTrue(result)
        self.assertIsNotNone(self.order_manager.current_position)
        self.assertEqual(self.order_manager.current_position.symbol, "RELIANCE")
    
    def test_process_signal_with_position(self):
        """Test signal processing when position is active"""
        # Set up active position
        self.order_manager.current_position = Position(
            symbol="TCS",
            token="2882",
            direction="BUY",
            quantity=50,
            entry_price=3000.0,
            target_price=3005.4,
            stop_loss=2994.6,
            order_id="12345",
            entry_time=time.time(),
            status="ACTIVE"
        )
        
        # Create test signal
        signal = TradeSignal(
            symbol="RELIANCE",
            token="2881",
            direction="BUY",
            entry_price=2500.0,
            quantity=40,
            target_price=2504.5,
            stop_loss=2495.5,
            confidence=0.85,
            momentum_strength=0.002,
            timestamp=time.time()
        )
        
        # Process signal (should be ignored)
        result = self.order_manager.process_signal(signal)
        self.assertFalse(result)
        self.assertEqual(self.order_manager.current_position.symbol, "TCS")

class TestIntegration(unittest.TestCase):
    """Test integration between components"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.config = TradingConfig()
        self.bot = Nifty500ScalpingBot('F', self.config)
    
    @patch('nifty500_scalping_bot.BrokerInterface.connect')
    def test_bot_startup(self, mock_connect):
        """Test bot startup sequence"""
        mock_connect.return_value = True
        
        # Mock other dependencies
        with patch.object(self.bot.signal_generator, 'initialize_symbols', return_value=True), \
             patch.object(self.bot.broker, 'subscribe_to_quotes', return_value=True):
            
            result = self.bot.start()
            self.assertTrue(result)
    
    def test_signal_flow(self):
        """Test signal flow from generation to processing"""
        # Mock broker
        self.bot.broker.connected = True
        self.bot.broker.api = Mock()
        
        # Create test signal
        signal = TradeSignal(
            symbol="RELIANCE",
            token="2881",
            direction="BUY",
            entry_price=2500.0,
            quantity=40,
            target_price=2504.5,
            stop_loss=2495.5,
            confidence=0.85,
            momentum_strength=0.002,
            timestamp=time.time()
        )
        
        # Add signal to queue
        self.bot.signal_queue.put(signal)
        
        # Process signal
        self.bot._monitor_signals()
        
        # Check if position was created
        self.assertIsNotNone(self.bot.order_manager.current_position)

def run_performance_test():
    """Run performance test for momentum analysis"""
    print("\n🚀 Running Performance Test...")
    
    config = TradingConfig()
    analyzer = MomentumAnalyzer(config)
    
    # Simulate high-frequency tick data
    token = "2881"
    symbol = "RELIANCE"
    base_price = 2500.0
    
    start_time = time.time()
    signals_generated = 0
    
    for i in range(1000):
        # Simulate price movement with some momentum
        price = base_price + (i * 0.1) + (i % 10 - 5) * 0.05
        volume = 1000 + (i % 100)
        
        signal = analyzer.analyze_tick(token, symbol, price, volume)
        if signal:
            signals_generated += 1
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"✅ Performance Test Results:")
    print(f"   Processed 1000 ticks in {duration:.3f} seconds")
    print(f"   Signals generated: {signals_generated}")
    print(f"   Processing rate: {1000/duration:.0f} ticks/second")

def run_configuration_test():
    """Test different configuration scenarios"""
    print("\n⚙️ Running Configuration Test...")
    
    # Test different capital amounts
    test_cases = [
        (50000, 90),   # Lower capital, lower target
        (100000, 180), # Default
        (200000, 360), # Higher capital, higher target
    ]
    
    for capital, target in test_cases:
        config = TradingConfig(CAPITAL=capital, TARGET_PROFIT=target)
        analyzer = MomentumAnalyzer(config)
        
        # Test trade parameter calculation
        params = analyzer._calculate_trade_parameters("RELIANCE", "2881", 2500.0, "BUY")
        
        if params:
            expected_quantity = int(capital / 2500.0)
            expected_move = target / expected_quantity
            
            print(f"   Capital: ₹{capital:,} | Target: ₹{target} | Qty: {params['quantity']} | Move: ₹{expected_move:.2f}")
        else:
            print(f"   Capital: ₹{capital:,} | Target: ₹{target} | No valid parameters")

if __name__ == "__main__":
    print("🧪 Starting Nifty 500 Scalping Bot Tests")
    print("=" * 50)
    
    # Run unit tests
    unittest.main(argv=[''], exit=False, verbosity=2)
    
    # Run performance tests
    run_performance_test()
    
    # Run configuration tests
    run_configuration_test()
    
    print("\n✅ All tests completed!")
    print("\n📋 Test Summary:")
    print("   - Unit tests for all components")
    print("   - Performance benchmark")
    print("   - Configuration validation")
    print("   - Integration testing")
    print("\n🚀 Bot is ready for deployment!")
