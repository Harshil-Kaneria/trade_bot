#!/usr/bin/env python3
"""
Nifty 500 Scalping Bot Launcher
===============================

Simple launcher script with interactive configuration and safety checks.
"""

import os
import sys
import time
import json
from datetime import datetime
from nifty500_scalping_bot import Nifty500ScalpingBot, TradingConfig

def print_banner():
    """Print application banner"""
    print("=" * 60)
    print("🚀 NIFTY 500 SCALPING BOT")
    print("   Fixed Profit/Loss Trading System")
    print("=" * 60)
    print()

def check_environment():
    """Check environment setup"""
    print("🔍 Checking environment setup...")
    
    # Check Python version
    if sys.version_info < (3, 8):
        print("❌ Python 3.8 or higher is required")
        return False
    
    # Check required files
    required_files = [
        'data_500.csv',
        'NSE_Equity_Master.csv',
        'api_helper_flattrade.py',
        'api_helper_shoonya.py'
    ]
    
    missing_files = []
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)
    
    if missing_files:
        print(f"❌ Missing required files: {', '.join(missing_files)}")
        return False
    
    # Check environment variables
    env_file = '.env'
    if not os.path.exists(env_file):
        print(f"⚠️  Environment file {env_file} not found")
        print("   Please copy env_template.txt to .env and configure your credentials")
        return False
    
    print("✅ Environment setup complete")
    return True

def get_broker_choice():
    """Get broker choice from user"""
    print("\n📊 Select your broker:")
    print("   1. FlatTrade (F)")
    print("   2. Shoonya (S)")
    
    while True:
        choice = input("Enter choice (1 or 2): ").strip()
        if choice == '1':
            return 'F'
        elif choice == '2':
            return 'S'
        else:
            print("❌ Invalid choice. Please enter 1 or 2.")

def get_trading_parameters():
    """Get trading parameters from user"""
    print("\n💰 Trading Configuration:")
    
    # Capital
    while True:
        try:
            capital = float(input("Enter trading capital (INR) [100000]: ").strip() or "100000")
            if capital > 0:
                break
            else:
                print("❌ Capital must be positive")
        except ValueError:
            print("❌ Invalid number format")
    
    # Target profit
    while True:
        try:
            profit = float(input("Enter target profit per trade (INR) [180]: ").strip() or "180")
            if profit > 0:
                break
            else:
                print("❌ Profit target must be positive")
        except ValueError:
            print("❌ Invalid number format")
    
    # Target loss
    while True:
        try:
            loss = float(input("Enter target loss per trade (INR) [180]: ").strip() or "180")
            if loss > 0:
                break
            else:
                print("❌ Loss target must be positive")
        except ValueError:
            print("❌ Invalid number format")
    
    return capital, profit, loss

def get_trading_mode():
    """Get trading mode from user"""
    print("\n⚙️ Select trading mode:")
    print("   1. Conservative (Lower risk, fewer trades)")
    print("   2. Standard (Balanced approach)")
    print("   3. Aggressive (Higher risk, more trades)")
    
    while True:
        choice = input("Enter choice (1, 2, or 3): ").strip()
        if choice == '1':
            return 'conservative'
        elif choice == '2':
            return 'standard'
        elif choice == '3':
            return 'aggressive'
        else:
            print("❌ Invalid choice. Please enter 1, 2, or 3.")

def create_config(mode, capital, profit, loss):
    """Create trading configuration based on mode"""
    if mode == 'conservative':
        config = TradingConfig(
            CAPITAL=capital,
            TARGET_PROFIT=profit,
            TARGET_LOSS=loss,
            MIN_MOMENTUM_STRENGTH=0.002,  # Higher threshold
            MAX_POSITION_DURATION=180,    # Shorter duration
            MAX_DAILY_LOSS=capital * 0.01,  # 1% daily loss limit
            MAX_DAILY_TRADES=25
        )
    elif mode == 'aggressive':
        config = TradingConfig(
            CAPITAL=capital,
            TARGET_PROFIT=profit,
            TARGET_LOSS=loss,
            MIN_MOMENTUM_STRENGTH=0.0005,  # Lower threshold
            MAX_POSITION_DURATION=600,     # Longer duration
            MAX_DAILY_LOSS=capital * 0.05,  # 5% daily loss limit
            MAX_DAILY_TRADES=100
        )
    else:  # standard
        config = TradingConfig(
            CAPITAL=capital,
            TARGET_PROFIT=profit,
            TARGET_LOSS=loss,
            MIN_MOMENTUM_STRENGTH=0.001,   # Standard threshold
            MAX_POSITION_DURATION=300,     # Standard duration
            MAX_DAILY_LOSS=capital * 0.02,  # 2% daily loss limit
            MAX_DAILY_TRADES=50
        )
    
    return config

def show_configuration_summary(config, broker_type):
    """Show configuration summary"""
    print("\n📋 Configuration Summary:")
    print("=" * 40)
    print(f"   Broker: {'FlatTrade' if broker_type == 'F' else 'Shoonya'}")
    print(f"   Capital: ₹{config.CAPITAL:,.2f}")
    print(f"   Target Profit: ₹{config.TARGET_PROFIT}")
    print(f"   Target Loss: ₹{config.TARGET_LOSS}")
    print(f"   Min Momentum: {config.MIN_MOMENTUM_STRENGTH:.4f}")
    print(f"   Max Position Duration: {config.MAX_POSITION_DURATION}s")
    print(f"   Daily Loss Limit: ₹{config.MAX_DAILY_LOSS:,.2f}")
    print(f"   Max Daily Trades: {config.MAX_DAILY_TRADES}")
    print("=" * 40)

def confirm_start():
    """Get user confirmation to start trading"""
    print("\n⚠️  IMPORTANT DISCLAIMER:")
    print("   - Algorithmic trading involves significant risk")
    print("   - Past performance does not guarantee future results")
    print("   - Never risk more than you can afford to lose")
    print("   - Test thoroughly in paper trading mode first")
    print()
    
    while True:
        confirm = input("Do you want to proceed with live trading? (yes/no): ").strip().lower()
        if confirm in ['yes', 'y']:
            return True
        elif confirm in ['no', 'n']:
            return False
        else:
            print("❌ Please enter 'yes' or 'no'")

def run_bot(bot):
    """Run the trading bot"""
    print("\n🚀 Starting trading bot...")
    print("   Press Ctrl+C to stop the bot")
    print()
    
    try:
        if bot.start():
            # Set up WebSocket callbacks
            if bot.broker.api:
                bot.broker.api.start_websocket(
                    subscribe_callback=bot.on_tick,
                    socket_open_callback=lambda: print("✅ WebSocket connected"),
                    socket_close_callback=lambda: print("❌ WebSocket disconnected"),
                    socket_error_callback=lambda e: print(f"❌ WebSocket error: {e}")
                )
            
            # Keep running
            while True:
                time.sleep(1)
        else:
            print("❌ Failed to start bot")
            return False
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping bot...")
        bot.stop()
        return True
    except Exception as e:
        print(f"❌ Error running bot: {e}")
        bot.stop()
        return False

def main():
    """Main launcher function"""
    print_banner()
    
    # Check environment
    if not check_environment():
        print("\n❌ Environment check failed. Please fix the issues above.")
        return
    
    # Get user inputs
    broker_type = get_broker_choice()
    capital, profit, loss = get_trading_parameters()
    mode = get_trading_mode()
    
    # Create configuration
    config = create_config(mode, capital, profit, loss)
    
    # Show summary
    show_configuration_summary(config, broker_type)
    
    # Confirm start
    if not confirm_start():
        print("👋 Bot launch cancelled. Goodbye!")
        return
    
    # Create and run bot
    bot = Nifty500ScalpingBot(broker_type, config)
    
    # Save configuration for reference
    config_file = f"bot_config_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(config_file, 'w') as f:
        json.dump(config.to_dict(), f, indent=2)
    print(f"💾 Configuration saved to {config_file}")
    
    # Run bot
    success = run_bot(bot)
    
    if success:
        print("✅ Bot stopped successfully")
    else:
        print("❌ Bot stopped with errors")

if __name__ == "__main__":
    main()
