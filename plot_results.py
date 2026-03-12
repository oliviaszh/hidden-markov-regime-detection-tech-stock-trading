#!/usr/bin/env python3
"""
Comprehensive plotting script for the regime-aware momentum system results.
"""

import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from regime_momentum_system.config.settings import TECH_STOCKS, REGIME_ASSETS
from regime_momentum_system.data_pipeline.pipeline import DataPipeline

def create_comprehensive_plots():
    """Create comprehensive visualizations of the momentum system results."""
    
    print("Running data pipeline to generate fresh data...")
    
    # Run the pipeline
    pipeline = DataPipeline()
    dataset = pipeline.run_pipeline()
    
    print("Generating comprehensive plots...")
    
    # Create figure with subplots
    fig = plt.figure(figsize=(20, 16))
    gs = fig.add_gridspec(4, 3, hspace=0.3, wspace=0.3)
    
    # 1. Tech Stock Prices (Top Left)
    ax1 = fig.add_subplot(gs[0, 0])
    prices_tech = dataset['prices'][TECH_STOCKS]
    
    # Normalize prices to start at 100 for comparison
    normalized_prices = (prices_tech / prices_tech.iloc[0]) * 100
    
    for column in normalized_prices.columns:
        ax1.plot(normalized_prices.index, normalized_prices[column], label=column, linewidth=1.5)
    
    ax1.set_title('Tech Stock Performance (Normalized to 100)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Normalized Price')
    ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax1.xaxis.set_major_locator(mdates.YearLocator())
    
    # 2. Momentum Rankings (Top Middle)
    ax2 = fig.add_subplot(gs[0, 1])
    rankings = pipeline.get_momentum_rankings()
    
    colors = plt.cm.viridis(np.linspace(0, 1, len(rankings)))
    bars = ax2.bar(rankings.index, rankings.values, color=colors)
    
    ax2.set_title('Current Momentum Rankings', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Rank (1 = Highest Momentum)')
    ax2.invert_yaxis()  # Higher momentum at top
    
    # Add value labels on bars
    for bar, rank in zip(bars, rankings.values):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                f'{int(rank)}', ha='center', va='bottom', fontweight='bold')
    
    ax2.grid(True, alpha=0.3, axis='y')
    
    # 3. QQQ and VIX Prices (Top Right)
    ax3 = fig.add_subplot(gs[0, 2])
    regime_prices = dataset['prices'][REGIME_ASSETS]
    
    # Create twin axes for VIX (different scale)
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
    
    # 4. Momentum Over Time for Top 3 Assets (Middle Left)
    ax4 = fig.add_subplot(gs[1, 0])
    
    # Get top 3 momentum assets
    top_3_assets = rankings.head(3).index.tolist()
    
    # Plot 252-day momentum for top 3 assets
    for asset in top_3_assets:
        momentum_col = f"{asset}_mom_252d"
        if momentum_col in dataset['momentum'].columns:
            ax4.plot(dataset['momentum'].index, dataset['momentum'][momentum_col], 
                    label=f'{asset} (252d)', linewidth=2)
    
    ax4.set_title('Long-term Momentum (252-day) - Top 3 Assets', fontsize=12, fontweight='bold')
    ax4.set_ylabel('Momentum (%)')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    ax4.axhline(y=0, color='black', linestyle='--', alpha=0.5)
    ax4.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax4.xaxis.set_major_locator(mdates.YearLocator())
    
    # 5. Performance Metrics (Middle Middle)
    ax5 = fig.add_subplot(gs[1, 1])
    metrics = pipeline.get_performance_metrics()
    
    # Plot Sharpe ratios for tech stocks
    tech_metrics = metrics.loc[TECH_STOCKS]
    sharpe_data = tech_metrics['sharpe_ratio'].sort_values(ascending=True)
    
    colors = plt.cm.plasma(np.linspace(0, 1, len(sharpe_data)))
    bars = ax5.barh(sharpe_data.index, sharpe_data.values, color=colors)
    
    ax5.set_title('Sharpe Ratios by Asset', fontsize=12, fontweight='bold')
    ax5.set_xlabel('Sharpe Ratio')
    ax5.grid(True, alpha=0.3, axis='x')
    
    # Add value labels
    for bar, value in zip(bars, sharpe_data.values):
        width = bar.get_width()
        ax5.text(width + 0.05, bar.get_y() + bar.get_height()/2, 
                f'{value:.2f}', ha='left', va='center', fontweight='bold')
    
    # 6. QQQ Returns and Volatility (Middle Right)
    ax6 = fig.add_subplot(gs[1, 2])
    
    # Plot QQQ returns
    regime_data = dataset['regime']
    ax6.plot(regime_data.index, regime_data['QQQ'], 'g-', linewidth=1, alpha=0.7, label='QQQ Returns')
    
    # Plot QQQ volatility
    ax6_twin = ax6.twinx()
    ax6_twin.plot(regime_data.index, regime_data['QQQ_vol'], 'orange', linewidth=1, alpha=0.7, label='QQQ Volatility')
    
    ax6.set_title('QQQ Returns vs Volatility', fontsize=12, fontweight='bold')
    ax6.set_ylabel('Returns', color='green')
    ax6_twin.set_ylabel('Volatility', color='orange')
    ax6.grid(True, alpha=0.3)
    ax6.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax6.xaxis.set_major_locator(mdates.YearLocator())
    
    # 7. Momentum Heatmap (Bottom Left)
    ax7 = fig.add_subplot(gs[2, 0])
    
    # Create momentum heatmap for recent period
    recent_momentum = dataset['momentum'].tail(60)  # Last 60 days
    momentum_matrix = []
    
    for asset in TECH_STOCKS:
        asset_momentum = []
        for period in [21, 63, 126, 252]:
            col = f"{asset}_mom_{period}d"
            if col in recent_momentum.columns:
                asset_momentum.append(recent_momentum[col].iloc[-1])  # Latest value
        momentum_matrix.append(asset_momentum)
    
    momentum_df = pd.DataFrame(momentum_matrix, 
                              index=TECH_STOCKS, 
                              columns=['21d', '63d', '126d', '252d'])
    
    im = ax7.imshow(momentum_df.values, cmap='RdYlBu_r', aspect='auto', vmin=-0.5, vmax=0.5)
    
    # Add text annotations
    for i in range(len(momentum_df.index)):
        for j in range(len(momentum_df.columns)):
            value = momentum_df.iloc[i, j]
            ax7.text(j, i, f'{value:.1%}', ha='center', va='center', 
                    color='white' if abs(value) > 0.25 else 'black', fontweight='bold')
    
    ax7.set_title('Momentum Heatmap (Latest Values)', fontsize=12, fontweight='bold')
    ax7.set_xticks(range(len(momentum_df.columns)))
    ax7.set_xticklabels(momentum_df.columns)
    ax7.set_yticks(range(len(momentum_df.index)))
    ax7.set_yticklabels(momentum_df.index)
    
    # Add colorbar
    cbar = plt.colorbar(im, ax=ax7, shrink=0.8)
    cbar.set_label('Momentum %')
    
    # 8. Drawdown Analysis (Bottom Middle)
    ax8 = fig.add_subplot(gs[2, 1])
    
    # Calculate drawdowns for top performing asset
    top_asset = tech_metrics['sharpe_ratio'].idxmax()
    prices_top = dataset['prices'][top_asset]
    
    # Calculate running maximum
    running_max = prices_top.expanding().max()
    drawdown = (prices_top - running_max) / running_max
    
    ax8.fill_between(drawdown.index, drawdown.values, 0, alpha=0.3, color='red')
    ax8.plot(drawdown.index, drawdown.values, 'r-', linewidth=1)
    ax8.set_title(f'Maximum Drawdown - {top_asset}', fontsize=12, fontweight='bold')
    ax8.set_ylabel('Drawdown %')
    ax8.grid(True, alpha=0.3)
    ax8.axhline(y=0, color='black', linestyle='-', alpha=0.5)
    ax8.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax8.xaxis.set_major_locator(mdates.YearLocator())
    
    # 9. VIX Analysis (Bottom Right)
    ax9 = fig.add_subplot(gs[2, 2])
    
    # VIX levels and volatility
    vix_data = dataset['regime'][['^VIX', '^VIX_vol']]
    
    # Create subplots within this subplot
    ax9_twin = ax9.twinx()
    
    line1 = ax9.plot(vix_data.index, vix_data['^VIX'], 'purple', linewidth=2, label='VIX Level')
    line2 = ax9_twin.plot(vix_data.index, vix_data['^VIX_vol'], 'cyan', linewidth=2, label='VIX Volatility')
    
    ax9.set_title('VIX Analysis', fontsize=12, fontweight='bold')
    ax9.set_ylabel('VIX Level', color='purple')
    ax9_twin.set_ylabel('VIX Volatility', color='cyan')
    ax9.tick_params(axis='y', labelcolor='purple')
    ax9_twin.tick_params(axis='y', labelcolor='cyan')
    ax9.grid(True, alpha=0.3)
    ax9.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax9.xaxis.set_major_locator(mdates.YearLocator())
    
    # 10. Summary Statistics Table (Bottom Full Width)
    ax10 = fig.add_subplot(gs[3, :])
    ax10.axis('off')
    
    # Create summary table
    summary_data = []
    for asset in TECH_STOCKS:
        if asset in metrics.index:
            row = [
                asset,
                f"{metrics.loc[asset, 'annualized_return']:.1%}",
                f"{metrics.loc[asset, 'sharpe_ratio']:.2f}",
                f"{metrics.loc[asset, 'max_drawdown']:.1%}",
                f"{metrics.loc[asset, 'win_rate']:.1%}"
            ]
            summary_data.append(row)
    
    summary_df = pd.DataFrame(summary_data, 
                             columns=['Asset', 'Annual Return', 'Sharpe Ratio', 'Max Drawdown', 'Win Rate'])
    
    table = ax10.table(cellText=summary_df.values,
                      colLabels=summary_df.columns,
                      cellLoc='center',
                      loc='center')
    
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.5)
    
    # Style the table
    for i in range(len(summary_df.columns)):
        table[(0, i)].set_facecolor('#4CAF50')
        table[(0, i)].set_text_props(weight='bold', color='white')
    
    ax10.set_title('Performance Summary Table', fontsize=14, fontweight='bold', pad=20)
    
    # Main title
    fig.suptitle('Regime-Aware Momentum System - Comprehensive Analysis', 
                fontsize=16, fontweight='bold', y=0.98)
    
    # Add timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fig.text(0.02, 0.02, f'Generated on: {timestamp} | Date Range: {dataset["metadata"]["start_date"].strftime("%Y-%m-%d")} to {dataset["metadata"]["end_date"].strftime("%Y-%m-%d")}', 
            fontsize=8, style='italic')
    
    plt.tight_layout()
    plt.subplots_adjust(top=0.95, bottom=0.08)
    
    # Save the plot
    output_file = 'momentum_system_analysis.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Plot saved as {output_file}")
    
    # Show the plot
    plt.show()
    
    return dataset

if __name__ == "__main__":
    try:
        dataset = create_comprehensive_plots()
        print("\n🎉 Comprehensive analysis completed successfully!")
        
        # Print some key insights
        print("\n📊 Key Insights:")
        print(f"• Analysis period: {dataset['metadata']['start_date'].strftime('%Y-%m-%d')} to {dataset['metadata']['end_date'].strftime('%Y-%m-%d')}")
        print(f"• Total trading days: {dataset['metadata']['total_days']}")
        print(f"• Current momentum leader: {dataset['prices'].columns[0]} (rank 1)")
        
        # Get performance metrics
        metrics = dataset['prices'].pct_change().dropna()
        print(f"• Average daily volatility: {metrics.std().mean():.2%}")
        
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()