#!/usr/bin/env python3
"""
Nifty 500 Scalping Bot - Example Usage
=====================================

This script demonstrates how to use the scalping bot with different configurations
and scenarios. Perfect for learning and testing.
"""

import time
import json
from datetime import datetime
from nifty500_scalping_bot import Nifty500ScalpingBot, TradingConfig

def example_basic_usage():
    """Example 1: Basic usage with default configuration"""
    print("📚 Example 1: Basic Usage")
    print("-" * 30)
    
    # Create bot with default configuration
    bot = Nifty500ScalpingBot('F')  # FlatTrade
    
    print("✅ Bot created with default configuration")
    print(f"   Capital: ₹{bot.config.CAPITAL:,.2f}")
    print(f"   Target Profit: ₹{bot.config.TARGET_PROFIT}")
    print(f"   Target Loss: ₹{bot.config.TARGET_LOSS}")
    
    # Note: In real usage, you would call bot.start() here
    print("   (Bot not started in example - requires live broker connection)")

def example_custom_configuration():
    """Example 2: Custom configuration"""
    print("\n📚 Example 2: Custom Configuration")
    print("-" * 30)
    
    # Create custom configuration
    config = TradingConfig(
        CAPITAL=200000.0,        # Higher capital
        TARGET_PROFIT=360.0,     # Higher profit target
        TARGET_LOSS=360.0,       # Higher loss limit
        MIN_PRICE=100.0,         # Higher minimum price
        MAX_PRICE=10000.0,       # Higher maximum price
        MOMENTUM_WINDOW=15,      # Longer momentum window
        MIN_MOMENTUM_STRENGTH=0.0005,  # Lower threshold for more signals
        MAX_POSITION_DURATION=600,     # Longer position duration
        MAX_DAILY_LOSS=2000.0,   # Higher daily loss limit
        MAX_DAILY_TRADES=100     # More trades per day
    )
    
    # Create bot with custom configuration
    bot = Nifty500ScalpingBot('S', config)  # Shoonya
    
    print("✅ Bot created with custom configuration")
    print(f"   Capital: ₹{bot.config.CAPITAL:,.2f}")
    print(f"   Target Profit: ₹{bot.config.TARGET_PROFIT}")
    print(f"   Min Price: ₹{bot.config.MIN_PRICE}")
    print(f"   Max Price: ₹{bot.config.MAX_PRICE}")
    print(f"   Momentum Window: {bot.config.MOMENTUM_WINDOW} ticks")
    print(f"   Min Momentum Strength: {bot.config.MIN_MOMENTUM_STRENGTH}")

def example_conservative_trading():
    """Example 3: Conservative trading approach"""
    print("\n📚 Example 3: Conservative Trading")
    print("-" * 30)
    
    # Conservative configuration
    config = TradingConfig(
        CAPITAL=50000.0,         # Lower capital
        TARGET_PROFIT=90.0,      # Lower profit target
        TARGET_LOSS=90.0,        # Lower loss limit
        MIN_MOMENTUM_STRENGTH=0.002,  # Higher threshold (fewer signals)
        MAX_POSITION_DURATION=180,    # Shorter duration
        MAX_DAILY_LOSS=500.0,    # Lower daily loss limit
        MAX_DAILY_TRADES=25      # Fewer trades per day
    )
    
    bot = Nifty500ScalpingBot('F', config)
    
    print("✅ Conservative bot created")
    print("   Focus: Lower risk, fewer trades, smaller positions")
    print(f"   Daily Loss Limit: ₹{config.MAX_DAILY_LOSS}")
    print(f"   Max Daily Trades: {config.MAX_DAILY_TRADES}")

def example_aggressive_trading():
    """Example 4: Aggressive trading approach"""
    print("\n📚 Example 4: Aggressive Trading")
    print("-" * 30)
    
    # Aggressive configuration
    config = TradingConfig(
        CAPITAL=500000.0,        # Higher capital
        TARGET_PROFIT=900.0,     # Higher profit target
        TARGET_LOSS=900.0,       # Higher loss limit
        MIN_MOMENTUM_STRENGTH=0.0003,  # Lower threshold (more signals)
        MAX_POSITION_DURATION=900,     # Longer duration
        MAX_DAILY_LOSS=5000.0,   # Higher daily loss limit
        MAX_DAILY_TRADES=200     # More trades per day
    )
    
    bot = Nifty500ScalpingBot('S', config)
    
    print("✅ Aggressive bot created")
    print("   Focus: Higher risk, more trades, larger positions")
    print(f"   Daily Loss Limit: ₹{config.MAX_DAILY_LOSS}")
    print(f"   Max Daily Trades: {config.MAX_DAILY_TRADES}")

def example_momentum_analysis():
    """Example 5: Momentum analysis demonstration"""
    print("\n📚 Example 5: Momentum Analysis")
    print("-" * 30)
    
    from nifty500_scalping_bot import MomentumAnalyzer
    
    config = TradingConfig()
    analyzer = MomentumAnalyzer(config)
    
    # Simulate price data with momentum
    token = "2881"
    symbol = "RELIANCE"
    
    print("Simulating price data with upward momentum...")
    
    # Create upward momentum pattern
    base_price = 2500.0
    prices = []
    
    for i in range(15):
        # Simulate increasing prices with some noise
        price = base_price + (i * 0.5) + (i % 3 - 1) * 0.1
        prices.append(price)
        
        # Add to analyzer history
        analyzer.price_history[token].append(price)
        analyzer.volume_history[token].append(1000 + i * 10)
        
        print(f"   Tick {i+1}: ₹{price:.2f}")
    
    # Analyze the last tick
    current_price = prices[-1]
    signal = analyzer.analyze_tick(token, symbol, current_price, 1200)
    
    if signal:
        print(f"\n✅ Signal generated!")
        print(f"   Symbol: {signal.symbol}")
        print(f"   Direction: {signal.direction}")
        print(f"   Entry Price: ₹{signal.entry_price:.2f}")
        print(f"   Quantity: {signal.quantity}")
        print(f"   Target Price: ₹{signal.target_price:.2f}")
        print(f"   Stop Loss: ₹{signal.stop_loss:.2f}")
        print(f"   Confidence: {signal.confidence:.2f}")
        print(f"   Momentum Strength: {signal.momentum_strength:.4f}")
    else:
        print("\n❌ No signal generated (momentum not strong enough)")

def example_position_sizing():
    """Example 6: Position sizing calculation"""
    print("\n📚 Example 6: Position Sizing")
    print("-" * 30)
    
    from nifty500_scalping_bot import MomentumAnalyzer
    
    config = TradingConfig(CAPITAL=100000.0, TARGET_PROFIT=180.0)
    analyzer = MomentumAnalyzer(config)
    
    # Test different stock prices
    test_prices = [50, 100, 250, 500, 1000, 2500, 5000]
    
    print("Position sizing for different stock prices:")
    print("Stock Price | Quantity | Required Move | Target Price | Stop Loss")
    print("-" * 70)
    
    for price in test_prices:
        params = analyzer._calculate_trade_parameters("TEST", "1234", price, "BUY")
        
        if params:
            required_move = config.TARGET_PROFIT / params['quantity']
            print(f"₹{price:8.0f} | {params['quantity']:8d} | ₹{required_move:8.2f} | ₹{params['target_price']:8.2f} | ₹{params['stop_loss']:8.2f}")
        else:
            print(f"₹{price:8.0f} | {'N/A':8s} | {'N/A':8s} | {'N/A':8s} | {'N/A':8s}")

def example_risk_management():
    """Example 7: Risk management demonstration"""
    print("\n📚 Example 7: Risk Management")
    print("-" * 30)
    
    from nifty500_scalping_bot import OrderManager, BrokerInterface
    
    # Mock broker for demonstration
    class MockBroker:
        def place_bracket_order(self, signal):
            return "MOCK_ORDER_123"
        
        def get_order_status(self, order_id):
            return {"status": "COMPLETE", "fillshares": "100"}
    
    config = TradingConfig()
    mock_broker = MockBroker()
    order_manager = OrderManager(config, mock_broker)
    
    print("Risk management features:")
    print(f"   Max Daily Loss: ₹{config.MAX_DAILY_LOSS:,.2f}")
    print(f"   Max Daily Trades: {config.MAX_DAILY_TRADES}")
    print(f"   Max Position Duration: {config.MAX_POSITION_DURATION} seconds")
    print(f"   Single Trade Constraint: Yes")
    print(f"   Fixed Profit/Loss: ₹{config.TARGET_PROFIT}")

def example_configuration_management():
    """Example 8: Configuration management"""
    print("\n📚 Example 8: Configuration Management")
    print("-" * 30)
    
    # Create configuration
    config = TradingConfig(
        CAPITAL=150000.0,
        TARGET_PROFIT=270.0,
        TARGET_LOSS=270.0
    )
    
    # Convert to dictionary
    config_dict = config.to_dict()
    
    print("Configuration as dictionary:")
    for key, value in config_dict.items():
        print(f"   {key}: {value}")
    
    # Save to file
    config_file = "example_config.json"
    with open(config_file, 'w') as f:
        json.dump(config_dict, f, indent=2)
    
    print(f"\n✅ Configuration saved to {config_file}")
    
    # Load from file
    with open(config_file, 'r') as f:
        loaded_dict = json.load(f)
    
    loaded_config = TradingConfig.from_dict(loaded_dict)
    
    print("✅ Configuration loaded from file")
    print(f"   Capital: ₹{loaded_config.CAPITAL:,.2f}")
    print(f"   Target Profit: ₹{loaded_config.TARGET_PROFIT}")

def example_error_handling():
    """Example 9: Error handling demonstration"""
    print("\n📚 Example 9: Error Handling")
    print("-" * 30)
    
    from nifty500_scalping_bot import MomentumAnalyzer
    
    config = TradingConfig()
    analyzer = MomentumAnalyzer(config)
    
    print("Testing error handling scenarios:")
    
    # Test with invalid data
    try:
        signal = analyzer.analyze_tick("", "", -100, -50)  # Invalid data
        print("   ✅ Handled invalid data gracefully")
    except Exception as e:
        print(f"   ❌ Error with invalid data: {e}")
    
    # Test with insufficient data
    try:
        signal = analyzer.analyze_tick("1234", "TEST", 100, 1000)  # No history
        print("   ✅ Handled insufficient data gracefully")
    except Exception as e:
        print(f"   ❌ Error with insufficient data: {e}")
    
    # Test with extreme values
    try:
        signal = analyzer.analyze_tick("1234", "TEST", 999999, 1)  # Extreme price, low volume
        print("   ✅ Handled extreme values gracefully")
    except Exception as e:
        print(f"   ❌ Error with extreme values: {e}")

def main():
    """Run all examples"""
    print("🚀 Nifty 500 Scalping Bot - Example Usage")
    print("=" * 50)
    
    # Run all examples
    example_basic_usage()
    example_custom_configuration()
    example_conservative_trading()
    example_aggressive_trading()
    example_momentum_analysis()
    example_position_sizing()
    example_risk_management()
    example_configuration_management()
    example_error_handling()
    
    print("\n✅ All examples completed!")
    print("\n📋 Summary:")
    print("   - Basic usage and configuration")
    print("   - Different trading approaches")
    print("   - Momentum analysis demonstration")
    print("   - Position sizing calculations")
    print("   - Risk management features")
    print("   - Configuration management")
    print("   - Error handling examples")
    print("\n🚀 Ready to implement your own trading strategies!")

if __name__ == "__main__":
    main()
