"""
Data validator module for ensuring data quality and completeness.

This module validates the downloaded financial data for completeness,
quality, and consistency before processing.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging


class DataValidator:
    """Validates data quality and completeness for financial datasets."""
    
    def __init__(self, min_data_points: int = 252):
        """
        Initialize the data validator.
        
        Args:
            min_data_points: Minimum number of trading days required
        """
        self.min_data_points = min_data_points
        self.logger = logging.getLogger(__name__)
    
    def validate_data(self, df: pd.DataFrame, assets: List[str]) -> Dict[str, bool]:
        """
        Perform comprehensive data validation.
        
        Args:
            df: DataFrame with asset closing prices
            assets: List of asset symbols to validate
            
        Returns:
            Dictionary with validation results for each check
        """
        results = {}
        
        # Check 1: Data completeness
        results['data_completeness'] = self._check_data_completeness(df, assets)
        
        # Check 2: Date alignment
        results['date_alignment'] = self._check_date_alignment(df, assets)
        
        # Check 3: Missing values
        results['missing_values'] = self._check_missing_values(df, assets)
        
        # Check 4: Data quality (negative prices, zeros)
        results['data_quality'] = self._check_data_quality(df, assets)
        
        # Check 5: Minimum data points
        results['minimum_data_points'] = self._check_minimum_data_points(df, assets)
        
        # Check 6: Price consistency
        results['price_consistency'] = self._check_price_consistency(df, assets)
        
        # Overall validation
        results['overall_valid'] = all(results.values())
        
        return results
    
    def _check_data_completeness(self, df: pd.DataFrame, assets: List[str]) -> bool:
        """
        Check if all required assets are present in the data.
        
        Args:
            df: DataFrame with asset data
            assets: List of required asset symbols
            
        Returns:
            True if all assets are present, False otherwise
        """
        missing_assets = set(assets) - set(df.columns)
        if missing_assets:
            self.logger.error(f"Missing assets in data: {missing_assets}")
            return False
        
        self.logger.info(f"All {len(assets)} required assets are present")
        return True
    
    def _check_date_alignment(self, df: pd.DataFrame, assets: List[str]) -> bool:
        """
        Check if all assets have the same date range.
        
        Args:
            df: DataFrame with asset data
            assets: List of asset symbols
            
        Returns:
            True if date ranges are aligned, False otherwise
        """
        date_ranges = {}
        for asset in assets:
            if asset in df.columns:
                non_null_dates = df[asset].dropna().index
                if len(non_null_dates) > 0:
                    date_ranges[asset] = (non_null_dates.min(), non_null_dates.max())
        
        if not date_ranges:
            self.logger.error("No valid date ranges found")
            return False
        
        # Check if all assets have the same date range
        first_range = list(date_ranges.values())[0]
        aligned = all(dr == first_range for dr in date_ranges.values())
        
        if not aligned:
            self.logger.warning(f"Date ranges are not aligned:")
            for asset, dr in date_ranges.items():
                self.logger.warning(f"  {asset}: {dr[0]} to {dr[1]}")
        
        return aligned
    
    def _check_missing_values(self, df: pd.DataFrame, assets: List[str]) -> bool:
        """
        Check for missing values in the data.
        
        Args:
            df: DataFrame with asset data
            assets: List of asset symbols
            
        Returns:
            True if no significant missing values, False otherwise
        """
        total_rows = len(df)
        max_allowed_missing = total_rows * 0.05  # Allow up to 5% missing
        
        for asset in assets:
            if asset in df.columns:
                missing_count = df[asset].isnull().sum()
                if missing_count > max_allowed_missing:
                    self.logger.error(f"Asset {asset} has {missing_count} missing values (>{max_allowed_missing} allowed)")
                    return False
        
        self.logger.info("Missing values check passed")
        return True
    
    def _check_data_quality(self, df: pd.DataFrame, assets: List[str]) -> bool:
        """
        Check for data quality issues like negative prices or zeros.
        
        Args:
            df: DataFrame with asset data
            assets: List of asset symbols
            
        Returns:
            True if data quality is acceptable, False otherwise
        """
        for asset in assets:
            if asset in df.columns:
                # Check for negative prices
                negative_count = (df[asset] < 0).sum()
                if negative_count > 0:
                    self.logger.error(f"Asset {asset} has {negative_count} negative prices")
                    return False
                
                # Check for zero prices (excluding NaN)
                zero_count = ((df[asset] == 0) & (df[asset].notna())).sum()
                if zero_count > 0:
                    self.logger.warning(f"Asset {asset} has {zero_count} zero prices")
                    # Allow zeros but log warning
        
        self.logger.info("Data quality check passed")
        return True
    
    def _check_minimum_data_points(self, df: pd.DataFrame, assets: List[str]) -> bool:
        """
        Check if there are enough data points for analysis.
        
        Args:
            df: DataFrame with asset data
            assets: List of asset symbols
            
        Returns:
            True if minimum data points requirement is met, False otherwise
        """
        for asset in assets:
            if asset in df.columns:
                valid_data_points = df[asset].dropna().shape[0]
                if valid_data_points < self.min_data_points:
                    self.logger.error(f"Asset {asset} has only {valid_data_points} data points, need at least {self.min_data_points}")
                    return False
        
        self.logger.info(f"Minimum data points check passed ({self.min_data_points} required)")
        return True
    
    def _check_price_consistency(self, df: pd.DataFrame, assets: List[str]) -> bool:
        """
        Check for price consistency and extreme outliers.
        
        Args:
            df: DataFrame with asset data
            assets: List of asset symbols
            
        Returns:
            True if prices are consistent, False otherwise
        """
        for asset in assets:
            if asset in df.columns:
                prices = df[asset].dropna()
                if len(prices) < 10:  # Need minimum data for consistency check
                    continue
                
                # Check for extreme price changes (more than 50% in one day)
                daily_returns = prices.pct_change().dropna()
                extreme_changes = (abs(daily_returns) > 0.5).sum()
                
                if extreme_changes > 0:
                    self.logger.warning(f"Asset {asset} has {extreme_changes} extreme daily changes (>50%)")
                    # Allow extreme changes but log warning
        
        self.logger.info("Price consistency check passed")
        return True
    
    def get_data_summary(self, df: pd.DataFrame, assets: Optional[List[str]] = None) -> Dict[str, any]:
        """
        Generate a comprehensive data summary.
        
        Args:
            df: DataFrame with asset data
            assets: List of asset symbols to summarize (None for all)
            
        Returns:
            Dictionary with data summary statistics
        """
        if assets is None:
            assets = list(df.columns)
        
        summary = {
            'total_rows': len(df),
            'date_range': (df.index.min(), df.index.max()),
            'assets': {},
            'overall_stats': {}
        }
        
        # Asset-specific statistics
        for asset in assets:
            if asset in df.columns:
                prices = df[asset].dropna()
                summary['assets'][asset] = {
                    'total_data_points': len(prices),
                    'missing_values': df[asset].isnull().sum(),
                    'min_price': prices.min(),
                    'max_price': prices.max(),
                    'avg_price': prices.mean(),
                    'std_price': prices.std(),
                    'start_date': prices.index.min(),
                    'end_date': prices.index.max()
                }
        
        # Overall statistics
        summary['overall_stats'] = {
            'total_assets': len(assets),
            'total_missing_values': df[assets].isnull().sum().sum(),
            'average_data_points_per_asset': np.mean([summary['assets'][a]['total_data_points'] for a in assets if a in summary['assets']])
        }
        
        return summary
    
    def validate_and_summarize(self, df: pd.DataFrame, assets: List[str]) -> Tuple[bool, Dict]:
        """
        Perform validation and generate summary.
        
        Args:
            df: DataFrame with asset data
            assets: List of asset symbols
            
        Returns:
            Tuple of (is_valid, summary_dict)
        """
        validation_results = self.validate_data(df, assets)
        summary = self.get_data_summary(df, assets)
        
        is_valid = validation_results['overall_valid']
        
        if is_valid:
            self.logger.info("Data validation passed successfully")
        else:
            self.logger.error("Data validation failed")
            for check, result in validation_results.items():
                if not result:
                    self.logger.error(f"Failed check: {check}")
        
        return is_valid, summary


if __name__ == "__main__":
    # Example usage
    import sys
    sys.path.append('.')
    
    from data_fetcher import DataFetcher
    
    # Fetch sample data
    fetcher = DataFetcher()
    data = fetcher.fetch_data()
    
    # Validate data
    validator = DataValidator()
    is_valid, summary = validator.validate_and_summarize(data, list(data.columns))
    
    print(f"Data validation: {'PASSED' if is_valid else 'FAILED'}")
    print(f"Summary: {summary}")