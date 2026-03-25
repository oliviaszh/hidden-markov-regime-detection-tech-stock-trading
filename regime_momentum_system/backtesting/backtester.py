"""
Core backtesting engine for the regime-aware momentum system.

This module provides the main backtesting framework that simulates
trading strategies over historical data with comprehensive performance analysis.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from collections import defaultdict

from regime_momentum_system.config.settings import (
    INITIAL_CAPITAL, REBALANCE_FREQUENCY, MAX_POSITION_SIZE,
    TECH_STOCKS, REGIME_ASSETS
)


@dataclass
class BacktestConfig:
    """Configuration for backtesting parameters."""
    initial_capital: float = INITIAL_CAPITAL
    rebalance_frequency: int = REBALANCE_FREQUENCY  # Trading days
    max_position_size: float = MAX_POSITION_SIZE
    transaction_cost: float = 0.001  # 10 bps
    slippage: float = 0.0005  # 5 bps
    min_momentum_threshold: float = 0.0  # Minimum momentum to include asset


class Trade:
    """Represents a single trade in the backtest."""
    
    def __init__(self, date: pd.Timestamp, asset: str, quantity: float, 
                 price: float, trade_type: str):
        self.date = date
        self.asset = asset
        self.quantity = quantity
        self.price = price
        self.trade_type = trade_type  # 'buy' or 'sell'
        self.value = abs(quantity * price)
        
    def __repr__(self):
        return f"Trade({self.date}, {self.asset}, {self.trade_type}, {self.quantity:.2f}, ${self.price:.2f})"


class Portfolio:
    """Manages portfolio state during backtesting."""
    
    def __init__(self, initial_capital: float, assets: List[str]):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.positions = {asset: 0.0 for asset in assets}
        self.total_value = initial_capital
        self.history = []
        self.trades = []
        
    def update_value(self, prices: pd.Series, date: pd.Timestamp):
        """Update portfolio value based on current prices."""
        positions_value = sum(
            self.positions[asset] * prices[asset] 
            for asset in self.positions.keys()
        )
        self.total_value = self.cash + positions_value
        
        # Record portfolio state
        state = {
            'date': date,
            'cash': self.cash,
            'total_value': self.total_value,
            'positions': self.positions.copy(),
            'positions_value': positions_value
        }
        self.history.append(state)
        
    def get_weights(self) -> Dict[str, float]:
        """Get current portfolio weights."""
        if self.total_value <= 0:
            return {asset: 0.0 for asset in self.positions.keys()}
        
        weights = {}
        for asset, quantity in self.positions.items():
            if self.total_value > 0:
                weights[asset] = (quantity * self.get_current_price(asset)) / self.total_value
            else:
                weights[asset] = 0.0
        return weights
    
    def get_current_price(self, asset: str) -> float:
        """Get current price for an asset (simplified for backtesting)."""
        # In a real implementation, this would come from the price data
        return 1.0  # Placeholder
    
    def execute_trade(self, asset: str, quantity: float, price: float, 
                     transaction_cost: float = 0.0):
        """Execute a trade and update portfolio."""
        trade_value = quantity * price
        
        # Apply transaction costs
        total_cost = trade_value * transaction_cost
        total_trade_value = trade_value + total_cost
        
        if quantity > 0:  # Buy
            if self.cash >= total_trade_value:
                self.cash -= total_trade_value
                self.positions[asset] += quantity
                trade_type = 'buy'
            else:
                # Partial fill or skip
                max_affordable = (self.cash / (1 + transaction_cost)) / price
                if max_affordable > 0:
                    self.cash -= max_affordable * price * (1 + transaction_cost)
                    self.positions[asset] += max_affordable
                    quantity = max_affordable
                    trade_type = 'buy'
                else:
                    return  # Can't afford any
        else:  # Sell
            if self.positions[asset] >= abs(quantity):
                self.cash += abs(quantity) * price * (1 - transaction_cost)
                self.positions[asset] += quantity  # quantity is negative
                trade_type = 'sell'
            else:
                # Partial sell
                max_sellable = self.positions[asset]
                if max_sellable > 0:
                    self.cash += max_sellable * price * (1 - transaction_cost)
                    self.positions[asset] = 0
                    quantity = -max_sellable
                    trade_type = 'sell'
                else:
                    return  # Nothing to sell
        
        # Record trade
        self.trades.append(Trade(
            date=datetime.now(),  # Will be updated by backtester
            asset=asset,
            quantity=quantity,
            price=price,
            trade_type=trade_type
        ))


class Backtester:
    """
    Main backtesting engine for the regime-aware momentum system.
    
    This class simulates trading strategies over historical data and provides
    comprehensive performance analysis.
    """
    
    def __init__(self, config: BacktestConfig):
        """
        Initialize the backtester.
        
        Args:
            config: Backtest configuration parameters
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Backtest state
        self.portfolio = None
        self.prices = None
        self.momentum = None
        self.regime = None
        self.dates = None
        
        # Results storage
        self.results = {}
        self.performance_metrics = {}
        
    def run_backtest(self, dataset: Dict[str, pd.DataFrame], 
                    strategy_func: Callable,
                    start_date: Optional[pd.Timestamp] = None,
                    end_date: Optional[pd.Timestamp] = None) -> Dict:
        """
        Run the backtest simulation.
        
        Args:
            dataset: Dictionary containing prices, momentum, and regime data
            strategy_func: Function that implements the trading strategy
            start_date: Start date for backtest (None for full dataset)
            end_date: End date for backtest (None for full dataset)
            
        Returns:
            Dictionary with backtest results
        """
        self.logger.info("Starting backtest simulation...")
        
        # Prepare data
        self._prepare_data(dataset, start_date, end_date)
        
        # Initialize portfolio
        assets = list(self.prices.columns)
        self.portfolio = Portfolio(self.config.initial_capital, assets)
        
        # Run simulation
        self._run_simulation(strategy_func)
        
        # Calculate performance metrics
        self._calculate_performance_metrics()
        
        # Compile results
        results = self._compile_results()
        
        self.logger.info("Backtest completed successfully!")
        return results
    
    def _prepare_data(self, dataset: Dict[str, pd.DataFrame], 
                     start_date: Optional[pd.Timestamp],
                     end_date: Optional[pd.Timestamp]):
        """Prepare and validate input data."""
        self.prices = dataset['prices'].copy()
        self.momentum = dataset['momentum'].copy()
        self.regime = dataset['regime'].copy()
        
        # Filter dates if specified
        if start_date:
            self.prices = self.prices[self.prices.index >= start_date]
            self.momentum = self.momentum[self.momentum.index >= start_date]
            self.regime = self.regime[self.regime.index >= start_date]
            
        if end_date:
            self.prices = self.prices[self.prices.index <= end_date]
            self.momentum = self.momentum[self.momentum.index <= end_date]
            self.regime = self.regime[self.regime.index <= end_date]
        
        # Ensure data alignment
        common_dates = self.prices.index.intersection(self.momentum.index).intersection(self.regime.index)
        self.prices = self.prices.loc[common_dates]
        self.momentum = self.momentum.loc[common_dates]
        self.regime = self.regime.loc[common_dates]
        
        self.dates = self.prices.index
        self.logger.info(f"Data prepared: {len(self.dates)} trading days from {self.dates[0]} to {self.dates[-1]}")
    
    def _run_simulation(self, strategy_func: Callable):
        """Run the backtest simulation day by day."""
        rebalance_dates = self._get_rebalance_dates()
        
        for i, current_date in enumerate(self.dates):
            # Update portfolio value
            current_prices = self.prices.loc[current_date]
            self.portfolio.update_value(current_prices, current_date)
            
            # Check if it's time to rebalance
            if current_date in rebalance_dates:
                self._execute_rebalance(current_date, current_prices, strategy_func)
                
        self.logger.info(f"Simulation completed over {len(self.dates)} trading days")
    
    def _get_rebalance_dates(self) -> List[pd.Timestamp]:
        """Get dates when portfolio should be rebalanced."""
        # Start from the first date and rebalance every N trading days
        rebalance_dates = []
        for i in range(0, len(self.dates), self.config.rebalance_frequency):
            if i < len(self.dates):
                rebalance_dates.append(self.dates[i])
        return rebalance_dates
    
    def _execute_rebalance(self, date: pd.Timestamp, current_prices: pd.Series, 
                          strategy_func: Callable):
        """Execute portfolio rebalancing based on strategy."""
        # Get current momentum data
        momentum_data = self.momentum.loc[date]
        regime_data = self.regime.loc[date]
        
        # Apply strategy to get target weights
        target_weights = strategy_func(
            date=date,
            prices=current_prices,
            momentum=momentum_data,
            regime=regime_data,
            current_weights=self.portfolio.get_weights(),
            config=self.config
        )
        
        # Execute trades to reach target weights
        self._rebalance_to_target_weights(date, current_prices, target_weights)
    
    def _rebalance_to_target_weights(self, date: pd.Timestamp, current_prices: pd.Series, 
                                   target_weights: Dict[str, float]):
        """Rebalance portfolio to target weights."""
        current_weights = self.portfolio.get_weights()
        
        for asset in target_weights:
            target_weight = target_weights[asset]
            current_weight = current_weights.get(asset, 0.0)
            
            # Calculate desired position value
            desired_value = self.portfolio.total_value * target_weight
            current_value = self.portfolio.positions[asset] * current_prices[asset]
            
            # Calculate trade value
            trade_value = desired_value - current_value
            
            if abs(trade_value) > 0.01:  # Only trade if significant
                # Calculate quantity to trade
                price = current_prices[asset]
                quantity = trade_value / price
                
                # Execute trade
                self.portfolio.execute_trade(
                    asset=asset,
                    quantity=quantity,
                    price=price,
                    transaction_cost=self.config.transaction_cost
                )
    
    def _calculate_performance_metrics(self):
        """Calculate comprehensive performance metrics."""
        if not self.portfolio.history:
            return
        
        # Extract portfolio values over time
        dates = [h['date'] for h in self.portfolio.history]
        values = [h['total_value'] for h in self.portfolio.history]
        
        portfolio_series = pd.Series(values, index=dates)
        
        # Calculate returns
        returns = portfolio_series.pct_change().dropna()
        
        # Basic metrics
        total_return = (portfolio_series.iloc[-1] / portfolio_series.iloc[0]) - 1
        annualized_return = (1 + total_return) ** (252 / len(portfolio_series)) - 1
        annualized_volatility = returns.std() * np.sqrt(252)
        sharpe_ratio = annualized_return / annualized_volatility if annualized_volatility > 0 else 0
        
        # Maximum drawdown
        rolling_max = portfolio_series.expanding().max()
        drawdowns = (portfolio_series - rolling_max) / rolling_max
        max_drawdown = drawdowns.min()
        
        # Win rate
        win_rate = (returns > 0).sum() / len(returns)
        
        # Risk metrics
        var_95 = np.percentile(returns, 5)
        cvar_95 = returns[returns <= var_95].mean()
        
        self.performance_metrics = {
            'total_return': total_return,
            'annualized_return': annualized_return,
            'annualized_volatility': annualized_volatility,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'win_rate': win_rate,
            'var_95': var_95,
            'cvar_95': cvar_95,
            'final_value': portfolio_series.iloc[-1],
            'initial_value': portfolio_series.iloc[0],
            'num_trades': len(self.portfolio.trades),
            'turnover': self._calculate_turnover()
        }
    
    def _calculate_turnover(self) -> float:
        """Calculate portfolio turnover rate."""
        if not self.portfolio.trades:
            return 0.0
        
        total_trade_value = sum(trade.value for trade in self.portfolio.trades)
        average_portfolio_value = np.mean([h['total_value'] for h in self.portfolio.history])
        
        return total_trade_value / average_portfolio_value
    
    def _compile_results(self) -> Dict:
        """Compile all backtest results."""
        portfolio_history = pd.DataFrame(self.portfolio.history)
        
        results = {
            'config': self.config,
            'performance_metrics': self.performance_metrics,
            'portfolio_history': portfolio_history,
            'trades': self.portfolio.trades,
            'summary': {
                'initial_capital': self.config.initial_capital,
                'final_value': self.performance_metrics['final_value'],
                'total_return': self.performance_metrics['total_return'],
                'cagr': self.performance_metrics['annualized_return'],
                'sharpe': self.performance_metrics['sharpe_ratio'],
                'max_drawdown': self.performance_metrics['max_drawdown'],
                'win_rate': self.performance_metrics['win_rate'],
                'num_trades': self.performance_metrics['num_trades'],
                'turnover': self.performance_metrics['turnover']
            }
        }
        
        return results
    
    def get_equity_curve(self) -> pd.Series:
        """Get the equity curve as a pandas Series."""
        if not self.portfolio.history:
            return pd.Series()
        
        dates = [h['date'] for h in self.portfolio.history]
        values = [h['total_value'] for h in self.portfolio.history]
        
        return pd.Series(values, index=dates)
    
    def get_trade_log(self) -> pd.DataFrame:
        """Get a DataFrame of all trades executed."""
        if not self.portfolio.trades:
            return pd.DataFrame()
        
        trade_data = []
        for trade in self.portfolio.trades:
            trade_data.append({
                'date': trade.date,
                'asset': trade.asset,
                'quantity': trade.quantity,
                'price': trade.price,
                'trade_type': trade.trade_type,
                'value': trade.value
            })
        
        return pd.DataFrame(trade_data)


def momentum_strategy(date: pd.Timestamp, prices: pd.Series, 
                     momentum: pd.Series, regime: pd.Series,
                     current_weights: Dict[str, float], 
                     config: BacktestConfig) -> Dict[str, float]:
    """
    Simple momentum strategy implementation.
    
    This is a placeholder strategy that can be enhanced with regime detection.
    """
    # Get momentum scores for tech stocks
    momentum_scores = {}
    for asset in TECH_STOCKS:
        momentum_col = f"{asset}_mom_252d"
        if momentum_col in momentum.index:
            momentum_scores[asset] = momentum[momentum_col]
    
    # Filter assets with positive momentum
    positive_momentum = {k: v for k, v in momentum_scores.items() 
                        if v > config.min_momentum_threshold}
    
    if not positive_momentum:
        # If no positive momentum, hold cash
        return {asset: 0.0 for asset in TECH_STOCKS}
    
    # Equal weight among positive momentum assets
    num_assets = len(positive_momentum)
    target_weight = min(1.0 / num_assets, config.max_position_size)
    
    target_weights = {}
    for asset in TECH_STOCKS:
        if asset in positive_momentum:
            target_weights[asset] = target_weight
        else:
            target_weights[asset] = 0.0
    
    return target_weights


if __name__ == "__main__":
    # Example usage
    from regime_momentum_system.data_pipeline.pipeline import DataPipeline
    
    # Load data
    pipeline = DataPipeline()
    dataset = pipeline.run_pipeline()
    
    # Configure backtest
    config = BacktestConfig(
        initial_capital=100000,
        rebalance_frequency=21,  # Monthly
        max_position_size=0.20,  # 20% max per asset
        transaction_cost=0.001   # 10 bps
    )
    
    # Run backtest
    backtester = Backtester(config)
    results = backtester.run_backtest(dataset, momentum_strategy)
    
    # Print summary
    print("Backtest Summary:")
    print(f"Initial Capital: ${results['summary']['initial_capital']:,.2f}")
    print(f"Final Value: ${results['summary']['final_value']:,.2f}")
    print(f"Total Return: {results['summary']['total_return']:.2%}")
    print(f"CAGR: {results['summary']['cagr']:.2%}")
    print(f"Sharpe Ratio: {results['summary']['sharpe']:.2f}")
    print(f"Max Drawdown: {results['summary']['max_drawdown']:.2%}")
    print(f"Win Rate: {results['summary']['win_rate']:.2%}")
    print(f"Number of Trades: {results['summary']['num_trades']}")
    print(f"Turnover: {results['summary']['turnover']:.2%}")