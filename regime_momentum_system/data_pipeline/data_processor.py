"""
Data processor module for calculating momentum metrics and preparing data.

This module handles momentum calculations, data alignment, and preparation
for the HMM regime detection and momentum strategy components.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
import logging

from regime_momentum_system.config.settings import MOMENTUM_PERIODS


class DataProcessor:
    """Processes raw price data to calculate momentum metrics and prepare datasets."""
    
    def __init__(self, momentum_periods: List[int] = MOMENTUM_PERIODS):
        """
        Initialize the data processor.
        
        Args:
            momentum_periods: List of lookback periods for momentum calculation
        """
        self.momentum_periods = momentum_periods
        self.logger = logging.getLogger(__name__)
    
    def calculate_momentum(self, prices: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate momentum metrics for all assets.
        
        Args:
            prices: DataFrame with asset closing prices
            
        Returns:
            DataFrame with momentum metrics
        """
        self.logger.info(f"Calculating momentum for {len(prices.columns)} assets with periods: {self.momentum_periods}")
        
        momentum_data = {}
        
        for asset in prices.columns:
            asset_prices = prices[asset].dropna()
            if len(asset_prices) < max(self.momentum_periods):
                self.logger.warning(f"Insufficient data for {asset}: {len(asset_prices)} points, need {max(self.momentum_periods)}")
                continue
            
            # Calculate momentum for each period
            for period in self.momentum_periods:
                momentum_col = f"{asset}_mom_{period}d"
                momentum_data[momentum_col] = self._calculate_asset_momentum(asset_prices, period)
        
        momentum_df = pd.DataFrame(momentum_data, index=prices.index)
        
        # Forward fill momentum values (momentum persists until next calculation)
        momentum_df = momentum_df.ffill()
        
        self.logger.info(f"Momentum calculation complete: {momentum_df.shape}")
        return momentum_df
    
    def _calculate_asset_momentum(self, prices: pd.Series, period: int) -> pd.Series:
        """
        Calculate momentum for a single asset over a specific period.
        
        Args:
            prices: Series of asset prices
            period: Lookback period in days
            
        Returns:
            Series with momentum values
        """
        # Calculate returns over the period
        if len(prices) < period + 1:
            return pd.Series(index=prices.index, dtype=float)
        
        # Momentum = (Current Price - Price N days ago) / Price N days ago
        momentum = (prices / prices.shift(period)) - 1
        
        return momentum
    
    def prepare_regime_data(self, prices: pd.DataFrame, regime_assets: List[str]) -> pd.DataFrame:
        """
        Prepare data for regime detection (QQQ and VIX).
        
        Args:
            prices: DataFrame with all asset prices
            regime_assets: List of regime detection asset symbols
            
        Returns:
            DataFrame with regime detection data
        """
        self.logger.info(f"Preparing regime data for assets: {regime_assets}")
        
        # Extract regime assets
        regime_data = prices[regime_assets].copy()
        
        # Calculate returns for regime detection
        regime_returns = regime_data.pct_change().dropna()
        
        # Calculate volatility (rolling standard deviation)
        volatility_window = 21  # 1 month volatility
        regime_volatility = regime_returns.rolling(window=volatility_window).std()
        
        # Combine returns and volatility
        regime_features = pd.concat([
            regime_returns,
            regime_volatility.add_suffix('_vol')
        ], axis=1)
        
        # Remove any rows with NaN values
        regime_features = regime_features.dropna()
        
        self.logger.info(f"Regime data prepared: {regime_features.shape}")
        return regime_features
    
    def align_data(self, prices: pd.DataFrame, momentum: pd.DataFrame, 
                   regime_data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Align all datasets to the same date range.
        
        Args:
            prices: Price data
            momentum: Momentum data
            regime_data: Regime detection data
            
        Returns:
            Tuple of aligned (prices, momentum, regime_data)
        """
        self.logger.info("Aligning datasets to common date range")
        
        # Find common date range
        common_dates = sorted(set(prices.index) & set(momentum.index) & set(regime_data.index))
        
        if not common_dates:
            raise ValueError("No common dates found across datasets")
        
        # Align all datasets
        aligned_prices = prices.loc[common_dates]
        aligned_momentum = momentum.loc[common_dates]
        aligned_regime = regime_data.loc[common_dates]
        
        self.logger.info(f"Data aligned to {len(common_dates)} common dates: {common_dates[0]} to {common_dates[-1]}")
        
        return aligned_prices, aligned_momentum, aligned_regime
    
    def prepare_full_dataset(self, prices: pd.DataFrame, regime_assets: List[str]) -> Dict[str, pd.DataFrame]:
        """
        Prepare complete dataset for the momentum system.
        
        Args:
            prices: DataFrame with all asset prices
            regime_assets: List of regime detection asset symbols
            
        Returns:
            Dictionary with 'prices', 'momentum', and 'regime' DataFrames
        """
        self.logger.info("Preparing full dataset for momentum system")
        
        # Calculate momentum metrics
        momentum_data = self.calculate_momentum(prices)
        
        # Prepare regime detection data
        regime_data = self.prepare_regime_data(prices, regime_assets)
        
        # Align all datasets
        aligned_prices, aligned_momentum, aligned_regime = self.align_data(
            prices, momentum_data, regime_data
        )
        
        # Create result dictionary
        result = {
            'prices': aligned_prices,
            'momentum': aligned_momentum,
            'regime': aligned_regime,
            'metadata': {
                'start_date': aligned_prices.index.min(),
                'end_date': aligned_prices.index.max(),
                'total_days': len(aligned_prices),
                'tech_assets': [col for col in aligned_prices.columns if col not in regime_assets],
                'regime_assets': regime_assets,
                'momentum_periods': self.momentum_periods
            }
        }
        
        self.logger.info(f"Full dataset prepared:")
        self.logger.info(f"  Prices: {aligned_prices.shape}")
        self.logger.info(f"  Momentum: {aligned_momentum.shape}")
        self.logger.info(f"  Regime: {aligned_regime.shape}")
        
        return result
    
    def get_momentum_rankings(self, momentum_data: pd.DataFrame, 
                            tech_assets: List[str], date: Optional[pd.Timestamp] = None) -> pd.Series:
        """
        Get momentum rankings for tech assets at a specific date.
        
        Args:
            momentum_data: DataFrame with momentum metrics
            tech_assets: List of tech asset symbols
            date: Specific date for ranking (None for most recent)
            
        Returns:
            Series with momentum rankings (1 = highest momentum)
        """
        if date is None:
            date = momentum_data.index[-1]
        
        if date not in momentum_data.index:
            raise ValueError(f"Date {date} not found in momentum data")
        
        # Get momentum values for the specified date
        date_momentum = momentum_data.loc[date]
        
        # Extract tech asset momentums (average across all periods)
        tech_momentum = {}
        for asset in tech_assets:
            asset_mom_cols = [col for col in date_momentum.index if col.startswith(f"{asset}_mom_")]
            if asset_mom_cols:
                tech_momentum[asset] = date_momentum[asset_mom_cols].mean()
        
        # Create ranking (higher momentum = better rank)
        momentum_series = pd.Series(tech_momentum)
        rankings = momentum_series.rank(ascending=False, method='dense')
        
        return rankings.sort_values()
    
    def calculate_performance_metrics(self, prices: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate performance metrics for all assets.
        
        Args:
            prices: DataFrame with asset prices
            
        Returns:
            DataFrame with performance metrics
        """
        self.logger.info("Calculating performance metrics")
        
        returns = prices.pct_change().dropna()
        
        metrics = {}
        
        for asset in prices.columns:
            asset_returns = returns[asset].dropna()
            if len(asset_returns) < 20:  # Need minimum data
                continue
            
            # Calculate metrics
            metrics[asset] = {
                'total_return': (prices[asset].iloc[-1] / prices[asset].iloc[0]) - 1,
                'annualized_return': self._calculate_annualized_return(asset_returns),
                'annualized_volatility': self._calculate_annualized_volatility(asset_returns),
                'sharpe_ratio': self._calculate_sharpe_ratio(asset_returns),
                'max_drawdown': self._calculate_max_drawdown(prices[asset]),
                'win_rate': (asset_returns > 0).sum() / len(asset_returns)
            }
        
        metrics_df = pd.DataFrame(metrics).T
        return metrics_df
    
    def _calculate_annualized_return(self, returns: pd.Series) -> float:
        """Calculate annualized return."""
        if len(returns) == 0:
            return 0.0
        trading_days = 252
        total_return = (1 + returns).prod() - 1
        years = len(returns) / trading_days
        return (1 + total_return) ** (1/years) - 1 if years > 0 else 0.0
    
    def _calculate_annualized_volatility(self, returns: pd.Series) -> float:
        """Calculate annualized volatility."""
        if len(returns) == 0:
            return 0.0
        trading_days = 252
        return returns.std() * np.sqrt(trading_days)
    
    def _calculate_sharpe_ratio(self, returns: pd.Series, risk_free_rate: float = 0.02) -> float:
        """Calculate Sharpe ratio."""
        if len(returns) == 0:
            return 0.0
        excess_returns = returns - risk_free_rate/252  # Daily risk-free rate
        return excess_returns.mean() / excess_returns.std() * np.sqrt(252)
    
    def _calculate_max_drawdown(self, prices: pd.Series) -> float:
        """Calculate maximum drawdown."""
        if len(prices) == 0:
            return 0.0
        rolling_max = prices.expanding().max()
        drawdowns = (prices - rolling_max) / rolling_max
        return drawdowns.min()


if __name__ == "__main__":
    # Example usage
    import sys
    sys.path.append('.')
    
    from data_fetcher import DataFetcher
    from ..config.settings import TECH_STOCKS, REGIME_ASSETS
    
    # Fetch sample data
    fetcher = DataFetcher()
    prices = fetcher.fetch_data()
    
    # Process data
    processor = DataProcessor()
    dataset = processor.prepare_full_dataset(prices, REGIME_ASSETS)
    
    print(f"Dataset prepared:")
    print(f"Prices shape: {dataset['prices'].shape}")
    print(f"Momentum shape: {dataset['momentum'].shape}")
    print(f"Regime shape: {dataset['regime'].shape}")
    
    # Get momentum rankings
    rankings = processor.get_momentum_rankings(
        dataset['momentum'], 
        TECH_STOCKS, 
        dataset['prices'].index[-1]
    )
    print(f"\nMomentum rankings (latest):")
    print(rankings)