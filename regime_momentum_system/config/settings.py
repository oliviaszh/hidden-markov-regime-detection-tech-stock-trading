"""
Configuration settings for the regime-aware momentum system.
"""

# Asset configuration
TECH_STOCKS = ['AAPL', 'MSFT', 'GOOGL', 'NVDA', 'AMZN', 'TSLA', 'META']
REGIME_ASSETS = ['QQQ', '^VIX']  # QQQ for market, VIX for volatility
ALL_ASSETS = TECH_STOCKS + REGIME_ASSETS

# Data configuration
START_DATE = '2015-01-01'
END_DATE = None  # Will use current date
DATA_FREQUENCY = '1d'  # Daily data

# Momentum calculation parameters
MOMENTUM_PERIODS = [21, 63, 126, 252]  # 1M, 3M, 6M, 12M trading days
MIN_DATA_POINTS = 252  # Minimum trading days required

# HMM configuration
HMM_N_STATES = 3
HMM_N_ITER = 100
HMM_TOL = 1e-4

# Backtesting configuration
INITIAL_CAPITAL = 100000
REBALANCE_FREQUENCY = 21  # Monthly rebalancing
MAX_POSITION_SIZE = 0.20  # 20% max allocation per stock

# File paths
DATA_DIR = 'data'
LOGS_DIR = 'logs'
OUTPUT_DIR = 'output'

# Logging configuration
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'