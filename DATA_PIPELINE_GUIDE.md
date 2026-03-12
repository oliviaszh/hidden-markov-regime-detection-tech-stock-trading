# Data Pipeline Guide

This guide explains how to use the data pipeline for the regime-aware momentum system.

## Overview

The data pipeline consists of three main components:

1. **Data Fetcher** (`data_fetcher.py`) - Downloads financial data from yfinance
2. **Data Validator** (`data_validator.py`) - Validates data quality and completeness
3. **Data Processor** (`data_processor.py`) - Calculates momentum metrics and prepares datasets

## Quick Start

### Using the Quick Pipeline

For simple use cases, use the `QuickPipeline` class:

```python
from regime_momentum_system.data_pipeline.pipeline import QuickPipeline

# Get processed data with default settings
dataset = QuickPipeline.get_data()

# Get momentum rankings
rankings = QuickPipeline.get_momentum_rankings()

print(f"Prices shape: {dataset['prices'].shape}")
print(f"Momentum shape: {dataset['momentum'].shape}")
print(f"Regime shape: {dataset['regime'].shape}")
```

### Using the Full Pipeline

For more control over the pipeline process:

```python
from regime_momentum_system.data_pipeline.pipeline import DataPipeline

# Initialize pipeline with custom dates
pipeline = DataPipeline(start_date='2020-01-01', end_date='2024-12-31')

# Run the complete pipeline
dataset = pipeline.run_pipeline()

# Get momentum rankings for a specific date
rankings = pipeline.get_momentum_rankings(date='2024-12-31')

# Get performance metrics
metrics = pipeline.get_performance_metrics()

# Get pipeline summary
summary = pipeline.get_pipeline_summary()
```

## Components

### Data Fetcher

Downloads daily closing prices for tech stocks and regime detection assets.

```python
from regime_momentum_system.data_pipeline.data_fetcher import DataFetcher

fetcher = DataFetcher()
data = fetcher.fetch_data()  # Fetch all assets
tech_data = fetcher.fetch_tech_stocks()  # Fetch only tech stocks
regime_data = fetcher.fetch_regime_assets()  # Fetch only regime assets
```

**Features:**
- Automatic handling of weekends/holidays (forward fill)
- Data cleaning and validation
- Support for custom asset lists
- Date range specification

### Data Validator

Validates data quality and completeness:

```python
from regime_momentum_system.data_pipeline.data_validator import DataValidator

validator = DataValidator(min_data_points=252)
is_valid, summary = validator.validate_and_summarize(data, assets)

# Validation checks:
# - Data completeness (all required assets present)
# - Date alignment (consistent date ranges)
# - Missing values (limited to 5%)
# - Data quality (no negative prices)
# - Minimum data points
# - Price consistency (detects extreme outliers)
```

### Data Processor

Calculates momentum metrics and prepares datasets:

```python
from regime_momentum_system.data_pipeline.data_processor import DataProcessor

processor = DataProcessor(momentum_periods=[21, 63, 126, 252])
dataset = processor.prepare_full_dataset(prices, regime_assets)

# Get momentum rankings
rankings = processor.get_momentum_rankings(
    momentum_data, 
    tech_assets, 
    date='2024-12-31'
)

# Calculate performance metrics
metrics = processor.calculate_performance_metrics(prices)
```

**Features:**
- Momentum calculation for multiple lookback periods
- Regime data preparation (returns and volatility)
- Data alignment across all datasets
- Performance metrics calculation
- Momentum ranking generation

## Configuration

### Default Settings

The pipeline uses these default settings from `config/settings.py`:

```python
# Assets
TECH_STOCKS = ['AAPL', 'GOOGL', 'NVDA', 'AMZN', 'TSLA', 'META']
REGIME_ASSETS = ['QQQ', '^VIX']
ALL_ASSETS = TECH_STOCKS + REGIME_ASSETS

# Data parameters
START_DATE = '2015-01-01'
END_DATE = None  # Current date
DATA_FREQUENCY = '1d'  # Daily data
MIN_DATA_POINTS = 252  # 1 year of trading days
MOMENTUM_PERIODS = [21, 63, 126, 252]  # 1, 3, 6, 12 months
```

### Custom Configuration

You can override defaults when initializing components:

```python
# Custom date range
pipeline = DataPipeline(
    start_date='2020-01-01',
    end_date='2024-12-31'
)

# Custom momentum periods
processor = DataProcessor(
    momentum_periods=[10, 30, 90, 180]
)

# Custom minimum data points
validator = DataValidator(
    min_data_points=500
)
```

## Output Data

The pipeline produces a dictionary with the following structure:

```python
{
    'prices': pd.DataFrame,      # Asset closing prices
    'momentum': pd.DataFrame,    # Momentum metrics for all periods
    'regime': pd.DataFrame,      # Regime detection features
    'metadata': {
        'start_date': datetime,
        'end_date': datetime,
        'total_days': int,
        'tech_assets': list,
        'regime_assets': list,
        'momentum_periods': list
    }
}
```

### Momentum Data Format

Momentum columns are named as `{asset}_mom_{period}d`:

```
AAPL_mom_21d    # 21-day momentum for AAPL
GOOGL_mom_63d   # 63-day momentum for GOOGL
NVDA_mom_126d   # 126-day momentum for NVDA
...
```

### Regime Data Format

Regime features include:
- Returns for QQQ and VIX
- Rolling volatility (21-day window) for QQQ and VIX

```
QQQ         # QQQ returns
^VIX        # VIX returns
QQQ_vol     # QQQ volatility
^VIX_vol    # VIX volatility
```

## Performance Metrics

The processor calculates these performance metrics:

- `total_return`: Total return over the period
- `annualized_return`: Annualized return
- `annualized_volatility`: Annualized volatility
- `sharpe_ratio`: Sharpe ratio (with 2% risk-free rate)
- `max_drawdown`: Maximum drawdown
- `win_rate`: Percentage of positive daily returns

## Error Handling

The pipeline includes comprehensive error handling:

- **Data Fetching**: Handles missing assets, network issues, and yfinance errors
- **Data Validation**: Validates data quality and provides detailed error messages
- **Data Processing**: Checks for sufficient data points and handles edge cases
- **Date Alignment**: Ensures all datasets have consistent date ranges

## Best Practices

1. **Data Quality**: Always validate data before processing
2. **Date Ranges**: Use sufficient historical data (minimum 1 year)
3. **Asset Selection**: Ensure all required assets are available
4. **Error Handling**: Check validation results and handle failures gracefully
5. **Performance**: The pipeline can be computationally intensive for large datasets

## Example Usage

```python
import pandas as pd
from regime_momentum_system.data_pipeline.pipeline import DataPipeline

# Initialize pipeline
pipeline = DataPipeline(
    start_date='2020-01-01',
    end_date='2024-12-31'
)

try:
    # Run pipeline
    dataset = pipeline.run_pipeline()
    
    # Analyze results
    print(f"Data range: {dataset['metadata']['start_date']} to {dataset['metadata']['end_date']}")
    print(f"Assets: {dataset['metadata']['tech_assets']}")
    
    # Get momentum rankings
    rankings = pipeline.get_momentum_rankings()
    print("Current momentum rankings:")
    print(rankings)
    
    # Get performance metrics
    metrics = pipeline.get_performance_metrics()
    print("Performance metrics:")
    print(metrics[['annualized_return', 'sharpe_ratio']].sort_values('sharpe_ratio', ascending=False))
    
except Exception as e:
    print(f"Pipeline failed: {e}")
```

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure all dependencies are installed
2. **Data Fetching Failures**: Check internet connection and yfinance availability
3. **Validation Failures**: Check data quality and completeness
4. **Insufficient Data**: Ensure enough historical data for momentum calculations

### Dependencies

Required packages:
- `yfinance` - For data fetching
- `pandas` - For data manipulation
- `numpy` - For numerical calculations
- `scikit-learn` - For HMM regime detection
- `matplotlib` - For plotting (optional)

Install with:
```bash
pip install yfinance pandas numpy scikit-learn matplotlib
```

## Next Steps

After setting up the data pipeline, you can:
1. Implement the HMM regime detection module
2. Create the momentum strategy logic
3. Build the backtesting framework
4. Develop the main trading system orchestrator

See the main README for the complete system architecture and implementation guide.