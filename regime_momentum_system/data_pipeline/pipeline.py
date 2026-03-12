"""
Main data pipeline orchestrator for the regime-aware momentum system.

This module provides a high-level interface to run the complete data pipeline
including fetching, validation, and processing of financial data.
"""

import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple
import warnings

from .data_fetcher import DataFetcher
from .data_validator import DataValidator
from .data_processor import DataProcessor
from regime_momentum_system.config.settings import (
    ALL_ASSETS, TECH_STOCKS, REGIME_ASSETS, 
    START_DATE, END_DATE, MIN_DATA_POINTS
)


class DataPipeline:
    """
    Main data pipeline orchestrator.
    
    This class coordinates the entire data pipeline process:
    1. Data fetching from yfinance
    2. Data validation and quality checks
    3. Data processing and momentum calculations
    4. Dataset preparation for the momentum system
    """
    
    def __init__(self, start_date: str = START_DATE, end_date: Optional[str] = END_DATE):
        """
        Initialize the data pipeline.
        
        Args:
            start_date: Start date for data (YYYY-MM-DD format)
            end_date: End date for data (YYYY-MM-DD format), None for current date
        """
        self.start_date = start_date
        self.end_date = end_date
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.fetcher = DataFetcher(start_date, end_date)
        self.validator = DataValidator(min_data_points=MIN_DATA_POINTS)
        self.processor = DataProcessor()
        
        # Pipeline state
        self.raw_data = None
        self.validated_data = None
        self.processed_data = None
        self.pipeline_results = {}
    
    def run_pipeline(self, assets: Optional[List[str]] = None, 
                    validate: bool = True) -> Dict[str, pd.DataFrame]:
        """
        Run the complete data pipeline.
        
        Args:
            assets: List of asset symbols to process (None for ALL_ASSETS)
            validate: Whether to perform data validation
            
        Returns:
            Dictionary with processed datasets
        """
        if assets is None:
            assets = ALL_ASSETS
        
        self.logger.info(f"Starting data pipeline for {len(assets)} assets")
        
        try:
            # Step 1: Fetch data
            self.logger.info("Step 1: Fetching data from yfinance")
            self.raw_data = self.fetcher.fetch_data(assets)
            self.logger.info(f"Raw data fetched: {self.raw_data.shape}")
            
            # Step 2: Validate data (optional)
            if validate:
                self.logger.info("Step 2: Validating data quality")
                is_valid, validation_summary = self.validator.validate_and_summarize(
                    self.raw_data, assets
                )
                
                if not is_valid:
                    self.logger.error("Data validation failed")
                    raise ValueError("Data validation failed - check logs for details")
                
                self.validated_data = self.raw_data
                self.pipeline_results['validation'] = validation_summary
                self.logger.info("Data validation passed")
            else:
                self.validated_data = self.raw_data
                self.logger.info("Skipping data validation")
            
            # Step 3: Process data
            self.logger.info("Step 3: Processing data and calculating momentum")
            self.processed_data = self.processor.prepare_full_dataset(
                self.validated_data, REGIME_ASSETS
            )
            
            # Store pipeline results
            self.pipeline_results.update({
                'raw_data_shape': self.raw_data.shape,
                'processed_data_shapes': {
                    'prices': self.processed_data['prices'].shape,
                    'momentum': self.processed_data['momentum'].shape,
                    'regime': self.processed_data['regime'].shape
                },
                'date_range': {
                    'start': self.processed_data['metadata']['start_date'],
                    'end': self.processed_data['metadata']['end_date']
                },
                'assets': self.processed_data['metadata']
            })
            
            self.logger.info("Data pipeline completed successfully")
            return self.processed_data
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            raise
    
    def get_momentum_rankings(self, date: Optional[pd.Timestamp] = None) -> pd.Series:
        """
        Get momentum rankings for tech assets.
        
        Args:
            date: Specific date for ranking (None for most recent)
            
        Returns:
            Series with momentum rankings
        """
        if self.processed_data is None:
            raise ValueError("Pipeline must be run first")
        
        return self.processor.get_momentum_rankings(
            self.processed_data['momentum'],
            TECH_STOCKS,
            date
        )
    
    def get_performance_metrics(self) -> pd.DataFrame:
        """
        Get performance metrics for all assets.
        
        Returns:
            DataFrame with performance metrics
        """
        if self.processed_data is None:
            raise ValueError("Pipeline must be run first")
        
        return self.processor.calculate_performance_metrics(
            self.processed_data['prices']
        )
    
    def get_pipeline_summary(self) -> Dict:
        """
        Get a summary of the pipeline execution.
        
        Returns:
            Dictionary with pipeline summary
        """
        if not self.pipeline_results:
            raise ValueError("Pipeline must be run first")
        
        summary = {
            'execution_summary': self.pipeline_results,
            'data_info': {
                'total_tech_assets': len(TECH_STOCKS),
                'total_regime_assets': len(REGIME_ASSETS),
                'momentum_periods': self.processor.momentum_periods,
                'start_date': self.start_date,
                'end_date': self.end_date or 'Current'
            }
        }
        
        return summary
    
    def save_data(self, output_dir: str = 'data') -> None:
        """
        Save processed data to CSV files.
        
        Args:
            output_dir: Directory to save files
        """
        if self.processed_data is None:
            raise ValueError("Pipeline must be run first")
        
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        # Save datasets
        self.processed_data['prices'].to_csv(f'{output_dir}/prices.csv')
        self.processed_data['momentum'].to_csv(f'{output_dir}/momentum.csv')
        self.processed_data['regime'].to_csv(f'{output_dir}/regime.csv')
        
        # Save metadata
        import json
        with open(f'{output_dir}/metadata.json', 'w') as f:
            json.dump(self.processed_data['metadata'], f, default=str, indent=2)
        
        self.logger.info(f"Data saved to {output_dir}/")


class QuickPipeline:
    """
    Quick pipeline for simple use cases.
    
    This provides a simplified interface for users who just want to get
    the processed data without worrying about the pipeline details.
    """
    
    @staticmethod
    def get_data(start_date: str = START_DATE, end_date: Optional[str] = END_DATE,
                validate: bool = True) -> Dict[str, pd.DataFrame]:
        """
        Get processed data with minimal configuration.
        
        Args:
            start_date: Start date for data
            end_date: End date for data
            validate: Whether to validate data
            
        Returns:
            Dictionary with processed datasets
        """
        pipeline = DataPipeline(start_date, end_date)
        return pipeline.run_pipeline(validate=validate)
    
    @staticmethod
    def get_momentum_rankings(start_date: str = START_DATE, 
                            end_date: Optional[str] = END_DATE) -> pd.Series:
        """
        Get momentum rankings with minimal configuration.
        
        Args:
            start_date: Start date for data
            end_date: End date for data
            
        Returns:
            Series with momentum rankings
        """
        data = QuickPipeline.get_data(start_date, end_date)
        processor = DataProcessor()
        return processor.get_momentum_rankings(
            data['momentum'], TECH_STOCKS, data['prices'].index[-1]
        )


if __name__ == "__main__":
    # Example usage
    import sys
    sys.path.append('.')
    
    # Run full pipeline
    pipeline = DataPipeline()
    dataset = pipeline.run_pipeline()
    
    print("Pipeline completed successfully!")
    print(f"Prices shape: {dataset['prices'].shape}")
    print(f"Momentum shape: {dataset['momentum'].shape}")
    print(f"Regime shape: {dataset['regime'].shape}")
    
    # Get momentum rankings
    rankings = pipeline.get_momentum_rankings()
    print(f"\nMomentum rankings:")
    print(rankings)
    
    # Get performance metrics
    metrics = pipeline.get_performance_metrics()
    print(f"\nPerformance metrics (first 5 assets):")
    print(metrics.head())
    
    # Get pipeline summary
    summary = pipeline.get_pipeline_summary()
    print(f"\nPipeline summary:")
    print(f"Date range: {summary['execution_summary']['date_range']}")
    print(f"Assets processed: {len(summary['execution_summary']['assets']['tech_assets'])} tech + {len(summary['execution_summary']['assets']['regime_assets'])} regime")