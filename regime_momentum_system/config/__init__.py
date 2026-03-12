"""
Configuration module for the regime-aware momentum system.

This module contains all configuration settings and constants
used throughout the system.
"""

from .settings import *

__all__ = [
    'TECH_STOCKS', 'REGIME_ASSETS', 'ALL_ASSETS',
    'START_DATE', 'END_DATE', 'DATA_FREQUENCY',
    'MOMENTUM_PERIODS', 'MIN_DATA_POINTS',
    'HMM_N_STATES', 'HMM_N_ITER', 'HMM_TOL',
    'INITIAL_CAPITAL', 'REBALANCE_FREQUENCY', 'MAX_POSITION_SIZE',
    'DATA_DIR', 'LOGS_DIR', 'OUTPUT_DIR',
    'LOG_LEVEL', 'LOG_FORMAT'
]