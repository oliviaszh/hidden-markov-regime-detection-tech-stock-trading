#!/usr/bin/env python3
"""
Simple test script for the data pipeline.
"""

import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import modules directly
from regime_momentum_system.config.settings import (
    ALL_ASSETS, TECH_STOCKS, REGIME_ASSETS, 
    START_DATE, END_DATE, MIN_DATA_POINTS, MOMENTUM_PERIODS
)
from regime_momentum_system.data_pipeline.data_fetcher import DataFetcher
from regime_momentum_system.data_pipeline.data_validator import DataValidator
from regime_momentum_system.data_pipeline.data_processor import DataProcessor

def test_pipeline():
    """Test the data pipeline components."""
    print("Testing data pipeline components...")
    
    # Test 1: Data Fetcher
    print("\n1. Testing Data Fetcher...")
    fetcher = DataFetcher()
    raw_data = fetcher.fetch_data()
    print(f"✓ Raw data fetched: {raw_data.shape}")
    print(f"✓ Assets: {list(raw_data.columns)}")
    
    # Test 2: Data Validator
    print("\n2. Testing Data Validator...")
    validator = DataValidator(min_data_points=MIN_DATA_POINTS)
    is_valid, summary = validator.validate_and_summarize(raw_data, list(raw_data.columns))
    print(f"✓ Data validation: {'PASSED' if is_valid else 'FAILED'}")
    print(f"✓ Total assets: {summary['overall_stats']['total_assets']}")
    
    # Test 3: Data Processor
    print("\n3. Testing Data Processor...")
    processor = DataProcessor(momentum_periods=MOMENTUM_PERIODS)
    dataset = processor.prepare_full_dataset(raw_data, REGIME_ASSETS)
    print(f"✓ Prices shape: {dataset['prices'].shape}")
    print(f"✓ Momentum shape: {dataset['momentum'].shape}")
    print(f"✓ Regime shape: {dataset['regime'].shape}")
    
    # Test 4: Momentum Rankings
    print("\n4. Testing Momentum Rankings...")
    rankings = processor.get_momentum_rankings(
        dataset['momentum'], 
        TECH_STOCKS, 
        dataset['prices'].index[-1]
    )
    print(f"✓ Momentum rankings calculated: {len(rankings)} assets ranked")
    print("Top 5 momentum assets:")
    for i, (asset, rank) in enumerate(rankings.head().items(), 1):
        print(f"  {i}. {asset}: Rank {rank}")
    
    # Test 5: Performance Metrics
    print("\n5. Testing Performance Metrics...")
    metrics = processor.calculate_performance_metrics(dataset['prices'])
    print(f"✓ Performance metrics calculated: {metrics.shape[0]} assets")
    print("Top 5 performance metrics:")
    print(metrics[['total_return', 'annualized_return', 'sharpe_ratio']].head())
    
    print("\n🎉 All pipeline tests completed successfully!")
    return dataset

if __name__ == "__main__":
    try:
        dataset = test_pipeline()
        print(f"\nFinal dataset summary:")
        print(f"- Date range: {dataset['metadata']['start_date']} to {dataset['metadata']['end_date']}")
        print(f"- Total days: {dataset['metadata']['total_days']}")
        print(f"- Tech assets: {len(dataset['metadata']['tech_assets'])}")
        print(f"- Regime assets: {len(dataset['metadata']['regime_assets'])}")
        print(f"- Momentum periods: {dataset['metadata']['momentum_periods']}")
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()