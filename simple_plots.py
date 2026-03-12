#!/usr/bin/env python3
"""
Simple plotting script for key momentum system results.
"""

import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from regime_momentum_system.config.settings import TECH_STOCKS, REGIME_ASSETS
from regime_momentum_system.data_pipeline.pipeline import DataPipeline

def create_simple_plots():
    """Create simple, focused visualizations of key momentum system results."""
    
    print("Running data pipeline...")
    
    # Run the pipeline
    pipeline = DataPipeline()
    dataset = pipeline.run_pipeline()
    
    print("Creating simple plots...")
    
    # Create figure with 2x2 grid
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Regime-Aware Momentum System - Key Results', fontsize=16, fontweight='bold')
    
    # 1. Tech Stock Performance (Top Left)
    prices_tech = dataset['prices'][TECH_STOCKS]
    normalized_prices = (prices_tech / prices_tech.iloc[0]) * 100
    
    for column in normalized_prices.columns:
        ax1.plot(normalized_prices.index, normalized_prices[column], label=column, linewidth=2)
    
    ax1.set_title('Tech Stock Performance (Normalized)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Normalized Price (Base 100)')
    ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax1.xaxis.set_major_locator(mdates.YearLocator())
    
    # 2. Momentum Rankings (Top Right)
    rankings = pipeline.get_momentum_rankings()
    colors = plt.cm.viridis(np.linspace(0, 1, len(rankings)))
    bars = ax2.bar(rankings.index, rankings.values, color=colors)
    
    ax2.set_title('Current Momentum Rankings', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Rank (1 = Highest Momentum)')
    ax2.invert_yaxis()
    
    # Add value labels on bars
    for bar, rank in zip(bars, rankings.values):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                f'{int(rank)}', ha='center', va='bottom', fontweight='bold')
    
    ax2.grid(True, alpha=0.3, axis='y')
    
    # 3. QQQ and VIX (Bottom Left)
    regime_prices = dataset['prices'][REGIME_ASSETS]
    
    # Create twin axes for VIX
    ax3_twin = ax3.twinx()
    
    # QQQ on left axis
    line1 = ax3.plot(regime_prices.index, regime_prices['QQQ'], 'b-', linewidth=2, label='QQQ')
    ax3.set_ylabel('QQQ Price', color='blue')
    ax3.tick_params(axis='y', labelcolor='blue')
    
    # VIX on right axis
    line2 = ax3_twin.plot(regime_prices.index, regime_prices['^VIX'], 'r-', linewidth=2, label='VIX')
    ax3_twin.set_ylabel('VIX Level', color='red')
    ax3_twin.tick_params(axis='y', labelcolor='red')
    
    ax3.set_title('Market Regime Indicators', fontsize=12, fontweight='bold')
    ax3.grid(True, alpha=0.3)
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax3.xaxis.set_major_locator(mdates.YearLocator())
    
    # 4. Performance Summary (Bottom Right)
    metrics = pipeline.get_performance_metrics()
    tech_metrics = metrics.loc[TECH_STOCKS]
    
    # Plot Sharpe ratios
    sharpe_data = tech_metrics['sharpe_ratio'].sort_values(ascending=True)
    colors = plt.cm.plasma(np.linspace(0, 1, len(sharpe_data)))
    bars = ax4.barh(sharpe_data.index, sharpe_data.values, color=colors)
    
    ax4.set_title('Sharpe Ratios by Asset', fontsize=12, fontweight='bold')
    ax4.set_xlabel('Sharpe Ratio')
    ax4.grid(True, alpha=0.3, axis='x')
    
    # Add value labels
    for bar, value in zip(bars, sharpe_data.values):
        width = bar.get_width()
        ax4.text(width + 0.05, bar.get_y() + bar.get_height()/2, 
                f'{value:.2f}', ha='left', va='center', fontweight='bold')
    
    plt.tight_layout()
    plt.subplots_adjust(top=0.92)
    
    # Save the plot
    output_file = 'momentum_system_simple.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Simple plot saved as {output_file}")
    
    # Show the plot
    plt.show()
    
    return dataset

if __name__ == "__main__":
    try:
        dataset = create_simple_plots()
        print("\n✅ Simple analysis completed successfully!")
        
        # Print key insights
        print("\n📊 Key Insights:")
        print(f"• Analysis period: {dataset['metadata']['start_date'].strftime('%Y-%m-%d')} to {dataset['metadata']['end_date'].strftime('%Y-%m-%d')}")
        print(f"• Total trading days: {dataset['metadata']['total_days']}")
        
        # Get performance metrics
        metrics = dataset['prices'].pct_change().dropna()
        print(f"• Average daily volatility: {metrics.std().mean():.2%}")
        
        # Show top performers
        tech_metrics = metrics.loc[:, TECH_STOCKS]
        annualized_returns = tech_metrics.mean() * 252
        top_performer = annualized_returns.idxmax()
        print(f"• Top performing asset: {top_performer} ({annualized_returns.max():.1%} annualized return)")
        
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()