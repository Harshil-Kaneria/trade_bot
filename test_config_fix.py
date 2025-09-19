#!/usr/bin/env python3
"""
Test script to verify the TradingConfig fix
"""

from nifty500_scalping_bot import TradingConfig

def test_trading_config():
    """Test TradingConfig with all parameters"""
    print("🧪 Testing TradingConfig with all parameters...")
    
    # Test default configuration
    config = TradingConfig()
    print(f"✅ Default config created successfully")
    print(f"   Capital: ₹{config.CAPITAL:,.2f}")
    print(f"   Target Profit: ₹{config.TARGET_PROFIT}")
    print(f"   Max Daily Loss: ₹{config.MAX_DAILY_LOSS}")
    print(f"   Max Daily Trades: {config.MAX_DAILY_TRADES}")
    
    # Test custom configuration (like in launcher)
    custom_config = TradingConfig(
        CAPITAL=90000.0,
        TARGET_PROFIT=180.0,
        TARGET_LOSS=180.0,
        MIN_MOMENTUM_STRENGTH=0.0005,
        MAX_POSITION_DURATION=600,
        MAX_DAILY_LOSS=2000.0,
        MAX_DAILY_TRADES=100
    )
    print(f"\n✅ Custom config created successfully")
    print(f"   Capital: ₹{custom_config.CAPITAL:,.2f}")
    print(f"   Target Profit: ₹{custom_config.TARGET_PROFIT}")
    print(f"   Max Daily Loss: ₹{custom_config.MAX_DAILY_LOSS}")
    print(f"   Max Daily Trades: {custom_config.MAX_DAILY_TRADES}")
    
    # Test to_dict method
    config_dict = custom_config.to_dict()
    print(f"\n✅ Config to_dict works: {len(config_dict)} parameters")
    
    # Test from_dict method
    loaded_config = TradingConfig.from_dict(config_dict)
    print(f"✅ Config from_dict works: Capital = ₹{loaded_config.CAPITAL:,.2f}")
    
    print("\n🎉 All tests passed! The configuration fix is working correctly.")

if __name__ == "__main__":
    test_trading_config()
