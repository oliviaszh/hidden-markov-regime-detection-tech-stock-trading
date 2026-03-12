"""
Data fetcher module for downloading financial data from yfinance.

This module handles downloading daily closing prices for tech stocks
and regime detection assets (QQQ and VIX).
"""

import yfinance as yf
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import warnings

# Suppress yfinance warnings
warnings.filterwarnings('ignore', category=UserWarning)

from regime_momentum_system.config.settings import (
    ALL_ASSETS, START_DATE, END_DATE, DATA_FREQUENCY,
    TECH_STOCKS, REGIME_ASSETS
)


class DataFetcher:
    """Handles data fetching from yfinance for all required assets."""
    
    def __init__(self, start_date: str = START_DATE, end_date: Optional[str] = END_DATE):
        """
        Initialize the data fetcher.
        
        Args:
            start_date: Start date for data (YYYY-MM-DD format)
            end_date: End date for data (YYYY-MM-DD format), None for current date
        """
        self.start_date = start_date
        self.end_date = end_date or datetime.now().strftime('%Y-%m-%d')
        self.logger = logging.getLogger(__name__)
        
    def fetch_data(self, assets: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Fetch daily closing prices for specified assets.
        
        Args:
            assets: List of asset symbols to fetch. If None, uses ALL_ASSETS.
            
        Returns:
            DataFrame with date index and closing prices for all assets
        """
        if assets is None:
            assets = ALL_ASSETS
            
        self.logger.info(f"Fetching data for {len(assets)} assets from {self.start_date} to {self.end_date}")
        
        try:
            # Download data using yfinance
            data = yf.download(
                assets,
                start=self.start_date,
                end=self.end_date,
                interval=DATA_FREQUENCY,
                group_by='ticker',
                progress=False
            )
            
            # Handle single vs multiple assets
            if len(assets) == 1:
                # For single asset, yfinance returns a simple DataFrame
                if isinstance(data, pd.DataFrame):
                    df = data[['Close']].copy()
                    df.columns = [assets[0]]
                else:
                    raise ValueError(f"Unexpected data format for single asset: {assets[0]}")
            else:
                # For multiple assets, yfinance returns a multi-level column DataFrame
                if isinstance(data.columns, pd.MultiIndex):
                    # Extract Close prices for all assets
                    close_data = {}
                    for asset in assets:
                        try:
                            if asset in data.columns.get_level_values(0):
                                close_data[asset] = data[(asset, 'Close')]
                            else:
                                self.logger.warning(f"Asset {asset} not found in downloaded data")
                        except Exception as e:
                            self.logger.warning(f"Error processing asset {asset}: {e}")
                    
                    df = pd.DataFrame(close_data)
                else:
                    # Fallback for unexpected format
                    df = data[['Close']].copy() if 'Close' in data.columns else data
                    
            # Clean and validate the data
            df = self._clean_data(df)
            
            self.logger.info(f"Successfully fetched data: {df.shape[0]} rows, {df.shape[1]} columns")
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching data: {e}")
            raise
    
    def fetch_tech_stocks(self) -> pd.DataFrame:
        """Fetch data for tech stocks only."""
        return self.fetch_data(TECH_STOCKS)
    
    def fetch_regime_assets(self) -> pd.DataFrame:
        """Fetch data for regime detection assets (QQQ and VIX) only."""
        return self.fetch_data(REGIME_ASSETS)
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and prepare the downloaded data.
        
        Args:
            df: Raw DataFrame from yfinance
            
        Returns:
            Cleaned DataFrame with proper formatting
        """
        # Remove any completely empty columns
        df = df.dropna(axis=1, how='all')
        
        # Forward fill missing values (handle weekends/holidays)
        df = df.ffill()
        
        # Remove any remaining NaN values (should be minimal after ffill)
        if df.isnull().any().any():
            self.logger.warning(f"Found {df.isnull().sum().sum()} remaining NaN values after forward fill")
            df = df.dropna()
        
        # Ensure proper datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
        
        # Sort index chronologically
        df = df.sort_index()
        
        # Round prices to 2 decimal places
        df = df.round(2)
        
        return df
    
    def get_available_dates(self, df: pd.DataFrame) -> Dict[str, tuple]:
        """
        Get the date range for each asset.
        
        Args:
            df: DataFrame with asset data
            
        Returns:
            Dictionary with asset names as keys and (start_date, end_date) tuples as values
        """
        date_ranges = {}
        for column in df.columns:
            non_null_data = df[column].dropna()
            if len(non_null_data) > 0:
                date_ranges[column] = (non_null_data.index.min(), non_null_data.index.max())
        
        return date_ranges


if __name__ == "__main__":
    # Example usage
    fetcher = DataFetcher()
    
    # Fetch all data
    all_data = fetcher.fetch_data()
    print(f"All data shape: {all_data.shape}")
    print(f"Columns: {list(all_data.columns)}")
    print(f"Date range: {all_data.index.min()} to {all_data.index.max()}")
    
    # Fetch tech stocks only
    tech_data = fetcher.fetch_tech_stocks()
    print(f"\nTech stocks shape: {tech_data.shape}")
    
    # Fetch regime assets only
    regime_data = fetcher.fetch_regime_assets()
    print(f"\nRegime assets shape: {regime_data.shape}")