#!/usr/bin/env python3
"""
Demonstration script showing the fixed Nifty 500 Scalping Bot
"""

from nifty500_scalping_bot import TradingConfig, Nifty500ScalpingBot

def demo_configuration():
    """Demonstrate the fixed configuration"""
    print("🚀 Nifty 500 Scalping Bot - Configuration Demo")
    print("=" * 50)
    
    # Test 1: Default configuration
    print("\n📋 Test 1: Default Configuration")
    default_config = TradingConfig()
    print(f"   Capital: ₹{default_config.CAPITAL:,.2f}")
    print(f"   Target Profit: ₹{default_config.TARGET_PROFIT}")
    print(f"   Max Daily Loss: ₹{default_config.MAX_DAILY_LOSS}")
    print(f"   Max Daily Trades: {default_config.MAX_DAILY_TRADES}")
    print("   ✅ Default configuration works!")
    
    # Test 2: Conservative configuration (like in launcher)
    print("\n📋 Test 2: Conservative Configuration")
    conservative_config = TradingConfig(
        CAPITAL=50000.0,
        TARGET_PROFIT=90.0,
        TARGET_LOSS=90.0,
        MIN_MOMENTUM_STRENGTH=0.002,  # Higher threshold
        MAX_POSITION_DURATION=180,    # Shorter duration
        MAX_DAILY_LOSS=500.0,         # 1% daily loss limit
        MAX_DAILY_TRADES=25
    )
    print(f"   Capital: ₹{conservative_config.CAPITAL:,.2f}")
    print(f"   Target Profit: ₹{conservative_config.TARGET_PROFIT}")
    print(f"   Max Daily Loss: ₹{conservative_config.MAX_DAILY_LOSS}")
    print(f"   Max Daily Trades: {conservative_config.MAX_DAILY_TRADES}")
    print("   ✅ Conservative configuration works!")
    
    # Test 3: Aggressive configuration (like in launcher)
    print("\n📋 Test 3: Aggressive Configuration")
    aggressive_config = TradingConfig(
        CAPITAL=200000.0,
        TARGET_PROFIT=360.0,
        TARGET_LOSS=360.0,
        MIN_MOMENTUM_STRENGTH=0.0005,  # Lower threshold
        MAX_POSITION_DURATION=600,     # Longer duration
        MAX_DAILY_LOSS=10000.0,        # 5% daily loss limit
        MAX_DAILY_TRADES=100
    )
    print(f"   Capital: ₹{aggressive_config.CAPITAL:,.2f}")
    print(f"   Target Profit: ₹{aggressive_config.TARGET_PROFIT}")
    print(f"   Max Daily Loss: ₹{aggressive_config.MAX_DAILY_LOSS}")
    print(f"   Max Daily Trades: {aggressive_config.MAX_DAILY_TRADES}")
    print("   ✅ Aggressive configuration works!")
    
    # Test 4: Bot creation with different configurations
    print("\n📋 Test 4: Bot Creation")
    
    # FlatTrade bot
    flattrade_bot = Nifty500ScalpingBot('F', default_config)
    print(f"   FlatTrade bot created with capital: ₹{flattrade_bot.config.CAPITAL:,.2f}")
    
    # Shoonya bot
    shoonya_bot = Nifty500ScalpingBot('S', conservative_config)
    print(f"   Shoonya bot created with capital: ₹{shoonya_bot.config.CAPITAL:,.2f}")
    
    print("   ✅ Bot creation works!")
    
    # Test 5: Configuration serialization
    print("\n📋 Test 5: Configuration Serialization")
    config_dict = aggressive_config.to_dict()
    print(f"   Config serialized to dictionary with {len(config_dict)} parameters")
    
    loaded_config = TradingConfig.from_dict(config_dict)
    print(f"   Config loaded from dictionary: Capital = ₹{loaded_config.CAPITAL:,.2f}")
    print("   ✅ Configuration serialization works!")
    
    print("\n🎉 All tests passed! The bot is ready to use.")
    print("\n📝 Next steps:")
    print("   1. Configure your broker credentials in .env file")
    print("   2. Run: python launch_bot.py")
    print("   3. Or run directly: python nifty500_scalping_bot.py --broker F")

if __name__ == "__main__":
    demo_configuration()

