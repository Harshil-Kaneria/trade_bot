# Nifty 500 Scalping Bot

A sophisticated algorithmic trading bot designed for the Indian stock market that continuously monitors all Nifty 500 stocks to identify high-probability scalping opportunities with fixed profit/loss targets.

## 🎯 Key Features

- **Real-time Momentum Analysis**: Analyzes live websocket tick data to detect momentum patterns
- **Fixed Profit/Loss Trading**: Each trade targets exactly ₹180 profit with ₹180 stop-loss
- **Single Trade Constraint**: Only one active position at a time for risk management
- **Continuous Signal Generation**: Background analysis continues even during active trades
- **Dual Broker Support**: Compatible with both Shoonya and FlatTrade APIs
- **Comprehensive Logging**: Detailed logs for monitoring and debugging
- **Risk Management**: Built-in position sizing and risk controls

## 🏗️ Architecture

The bot consists of two main components that run in parallel:

### 1. Signal Generator
- Continuously monitors all Nifty 500 stocks via websocket
- Analyzes tick-by-tick price movements for momentum patterns
- Generates BUY/SELL signals based on momentum strength
- Queues signals for the Order Manager

### 2. Order Manager
- Processes signals from the queue
- Places bracket orders (BO) with fixed profit/loss targets
- Monitors active positions for exit conditions
- Maintains single trade constraint
- Tracks trading statistics

## 📊 Trading Logic

### Momentum Analysis
The bot analyzes price movements using multiple factors:
- **Price Momentum**: Rate of price change over recent ticks
- **Price Acceleration**: Rate of change in momentum
- **Volume Momentum**: Volume trend confirmation
- **Price Volatility**: Consistency of price movements

### Position Sizing
For each stock at price P:
- Maximum quantity: Q = Floor(₹1,00,000 / P)
- Required price move: R = ₹180 / Q
- Target price: Entry + R (for BUY) or Entry - R (for SELL)
- Stop loss: Entry - R (for BUY) or Entry + R (for SELL)

### Example
- Stock price: ₹100
- Quantity: 1,000 shares
- Required move: ₹0.18
- Target: ₹100.18
- Stop loss: ₹99.82

## 🚀 Quick Start

### 1. Prerequisites
- Python 3.8 or higher
- Active broker account (Shoonya or FlatTrade)
- Required data files (NSE_Equity_Master.csv, data_500.csv)

### 2. Installation
```bash
# Clone or download the bot files
# Install dependencies
pip install -r requirements_scalping_bot.txt

# Copy environment template
cp env_template.txt .env
```

### 3. Configuration
Edit the `.env` file with your broker credentials:

**For FlatTrade:**
```
FLATTRADE_API_KEY=your_api_key
FLATTRADE_SECRET_KEY=your_secret_key
FLATTRADE_USER_ID=your_user_id
FLATTRADE_SESSION=your_session_token
```

**For Shoonya:**
```
SHOONYA_UID=your_user_id
SHOONYA_PASSWORD=your_password
SHOONYA_TOTP=your_totp_secret
SHOONYA_VENDOR_CODE=your_vendor_code
SHOONYA_API_KEY=your_api_key
SHOONYA_IMEI=your_imei
```

### 4. Run the Bot
```bash
# For FlatTrade
python nifty500_scalping_bot.py --broker F

# For Shoonya
python nifty500_scalping_bot.py --broker S

# With custom parameters
python nifty500_scalping_bot.py --broker F --capital 200000 --profit 360 --loss 360
```

## ⚙️ Configuration Options

### Command Line Arguments
- `--broker`: Broker type (F for FlatTrade, S for Shoonya)
- `--capital`: Trading capital in INR (default: 100000)
- `--profit`: Target profit per trade in INR (default: 180)
- `--loss`: Target loss per trade in INR (default: 180)

### Configuration File
Modify `config.py` for advanced settings:
- Momentum analysis parameters
- Risk management settings
- Position sizing rules
- Market hours and filters

## 📈 Monitoring and Logs

The bot provides comprehensive logging:
- **Real-time signals**: Generated signals with confidence scores
- **Order execution**: Order placement and status updates
- **Position monitoring**: Active position tracking
- **Performance metrics**: Win rate, profit/loss, trade statistics
- **Error handling**: Detailed error logs for debugging

Log files are saved as `nifty500_scalping_bot.log`.

## 🛡️ Risk Management

### Built-in Safeguards
- **Single Trade Limit**: Only one position at a time
- **Position Timeout**: Automatic exit after maximum duration
- **Daily Loss Limit**: Configurable daily loss threshold
- **Volume Filtering**: Minimum volume requirements for liquidity
- **Price Range Filtering**: Exclude stocks outside price range

### Risk Controls
- Fixed profit/loss per trade (₹180)
- Maximum position duration (5 minutes)
- Signal cooldown period (5 seconds per stock)
- Minimum momentum strength threshold

## 🔧 Customization

### Adding New Indicators
Extend the `MomentumAnalyzer` class to add custom technical indicators:

```python
def _calculate_custom_indicator(self, prices):
    # Your custom indicator logic
    return indicator_value
```

### Modifying Signal Logic
Update the `_calculate_momentum` method to change signal generation criteria:

```python
def _calculate_momentum(self, token, current_price):
    # Your custom momentum calculation
    return direction, strength, confidence
```

### Broker Integration
Add support for new brokers by extending the `BrokerInterface` class:

```python
def _connect_new_broker(self):
    # Your broker connection logic
    pass
```

## 📋 Data Requirements

The bot requires the following data files:
1. **NSE_Equity_Master.csv**: NSE instrument master data
2. **data_500.csv**: Nifty 500 symbol list

These files should be in the same directory as the bot script.

## ⚠️ Important Notes

### Market Hours
The bot is designed to run during market hours (9:15 AM - 3:30 PM IST). It will automatically handle market open/close.

### Broker Requirements
- Ensure sufficient margin for bracket orders
- Verify API permissions for real-time data and order placement
- Test with small quantities before full deployment

### Risk Disclaimer
- Algorithmic trading involves significant risk
- Past performance does not guarantee future results
- Always test thoroughly in paper trading mode first
- Never risk more than you can afford to lose

## 🐛 Troubleshooting

### Common Issues
1. **Connection Errors**: Check broker credentials and API status
2. **Order Failures**: Verify account permissions and margin availability
3. **No Signals**: Adjust momentum parameters or check market conditions
4. **WebSocket Disconnects**: Check internet connection and broker status

### Debug Mode
Enable debug logging by modifying the log level in `config.py`:
```python
LOG_LEVEL = "DEBUG"
```

## 📞 Support

For issues and questions:
1. Check the logs for error messages
2. Verify broker API documentation
3. Test with paper trading first
4. Review configuration parameters

## 📄 License

This project is for educational and research purposes. Use at your own risk.

---

**Disclaimer**: This software is provided for educational purposes only. Trading in financial markets involves substantial risk of loss. The authors are not responsible for any financial losses incurred through the use of this software.
