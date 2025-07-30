"""
Coinbase Analytics Calculator - Lambda Function Equivalent
Computes derived metrics and analytics from processed data.
"""

import sys
import os
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aws.connect_to_s3 import S3Helper


def load_processed_data(environment: str) -> Dict:
    """
    Load processed data from S3.
    Returns: Dictionary containing all processed data.
    """
    try:
        s3_helper = S3Helper()

        print("Loading processed data from S3...")

        # Load current data
        current_positions_key = (
            f"{environment}/coinbase/processed/current/positions.csv"
        )
        current_positions = s3_helper.read_csv_from_s3(current_positions_key)

        portfolio_summary_key = (
            f"{environment}/coinbase/processed/current/portfolio_summary.csv"
        )
        portfolio_summary = s3_helper.read_csv_from_s3(portfolio_summary_key)

        market_overview_key = (
            f"{environment}/coinbase/processed/current/market_overview.csv"
        )
        market_overview = s3_helper.read_csv_from_s3(market_overview_key)

        # Load historical data

        prices_key = f"{environment}/coinbase/processed/historical/prices.csv"
        prices = s3_helper.read_csv_from_s3(prices_key)

        portfolio_snapshots_key = (
            f"{environment}/coinbase/processed/historical/portfolio_snapshots.csv"
        )
        portfolio_snapshots = s3_helper.read_csv_from_s3(portfolio_snapshots_key)

        pnl_tracking_key = (
            f"{environment}/coinbase/processed/historical/pnl_tracking.csv"
        )
        pnl_tracking = s3_helper.read_csv_from_s3(pnl_tracking_key)

        return {
            "current_positions": current_positions,
            "portfolio_summary": portfolio_summary,
            "market_overview": market_overview,
            "prices": prices,
            "portfolio_snapshots": portfolio_snapshots,
            "pnl_tracking": pnl_tracking,
        }

    except Exception as e:
        print(f"Error loading processed data: {e}")
        raise


def calculate_performance_metrics(
    portfolio_snapshots: pd.DataFrame, pnl_tracking: pd.DataFrame
) -> pd.DataFrame:
    """
    Calculate portfolio performance metrics.
    Returns: DataFrame with performance metrics over time.
    """
    try:
        print("Calculating performance metrics...")

        # Convert date columns to datetime
        portfolio_snapshots["date"] = pd.to_datetime(portfolio_snapshots["date"])
        pnl_tracking["date"] = pd.to_datetime(pnl_tracking["date"])

        # Calculate daily returns
        portfolio_snapshots = portfolio_snapshots.sort_values("date")
        portfolio_snapshots["daily_return"] = (
            portfolio_snapshots["total_portfolio_value"]
            .pct_change()
            .replace([np.inf, -np.inf], 0)
        )
        portfolio_snapshots["cumulative_return"] = (
            1 + portfolio_snapshots["daily_return"]
        ).cumprod() - 1

        # Calculate volatility (rolling 30-day)
        portfolio_snapshots["volatility_30d"] = portfolio_snapshots[
            "daily_return"
        ].rolling(window=30).std() * np.sqrt(252)

        # Calculate Sharpe ratio (assuming risk-free rate of 2%)
        risk_free_rate = 0.02
        portfolio_snapshots["excess_return"] = portfolio_snapshots["daily_return"] - (
            risk_free_rate / 252
        )
        portfolio_snapshots["sharpe_ratio"] = (
            portfolio_snapshots["excess_return"].rolling(window=252).mean() * 252
        ) / (
            portfolio_snapshots["daily_return"].rolling(window=252).std() * np.sqrt(252)
        )

        # Calculate maximum drawdown
        portfolio_snapshots["peak"] = (
            portfolio_snapshots["total_portfolio_value"].expanding().max()
        )
        portfolio_snapshots["drawdown"] = (
            portfolio_snapshots["total_portfolio_value"] - portfolio_snapshots["peak"]
        ) / portfolio_snapshots["peak"]
        portfolio_snapshots["max_drawdown"] = (
            portfolio_snapshots["drawdown"].expanding().min()
        )

        # Calculate beta (simplified - using overall market correlation)
        # In a real implementation, you'd compare against a market index
        portfolio_snapshots["beta"] = 1.0  # Placeholder

        # Calculate alpha (simplified)
        portfolio_snapshots["alpha"] = portfolio_snapshots["daily_return"] - (
            risk_free_rate / 252
        )

        # Select relevant columns
        performance_metrics = portfolio_snapshots[
            [
                "date",
                "total_portfolio_value",
                "daily_return",
                "cumulative_return",
                "volatility_30d",
                "sharpe_ratio",
                "max_drawdown",
                "beta",
                "alpha",
            ]
        ]

        performance_metrics = performance_metrics.replace(
            [np.inf, -np.inf], np.nan
        ).fillna(0)
        return performance_metrics

    except Exception as e:
        print(f"Error calculating performance metrics: {e}")
        raise


def calculate_risk_metrics(
    portfolio_snapshots: pd.DataFrame, pnl_tracking: pd.DataFrame
) -> pd.DataFrame:
    """
    Calculate risk indicators and performance analytics.
    Returns: DataFrame with risk metrics.
    """
    try:
        print("Calculating risk metrics...")

        # Convert date columns to datetime
        portfolio_snapshots["date"] = pd.to_datetime(portfolio_snapshots["date"])
        pnl_tracking["date"] = pd.to_datetime(pnl_tracking["date"])

        # Calculate Value at Risk (VaR) - 95% confidence level
        portfolio_snapshots = portfolio_snapshots.sort_values("date")
        portfolio_snapshots["daily_return"] = (
            portfolio_snapshots["total_portfolio_value"]
            .pct_change()
            .replace([np.inf, -np.inf], 0)
        )

        # Add dependent calculations from performance metrics
        portfolio_snapshots["volatility_30d"] = portfolio_snapshots[
            "daily_return"
        ].rolling(window=30).std() * np.sqrt(252)
        portfolio_snapshots["peak"] = (
            portfolio_snapshots["total_portfolio_value"].expanding().max()
        )
        portfolio_snapshots["drawdown"] = (
            portfolio_snapshots["total_portfolio_value"] - portfolio_snapshots["peak"]
        ) / portfolio_snapshots["peak"]
        portfolio_snapshots["max_drawdown"] = (
            portfolio_snapshots["drawdown"].expanding().min()
        )
        risk_free_rate = 0.02
        portfolio_snapshots["excess_return"] = portfolio_snapshots["daily_return"] - (
            risk_free_rate / 252
        )

        # Rolling VaR (30-day window)
        portfolio_snapshots["var_95_30d"] = (
            portfolio_snapshots["daily_return"].rolling(window=30).quantile(0.05)
        )

        # Calculate Conditional VaR (Expected Shortfall)
        def calculate_cvar(returns):
            if len(returns) < 30:
                return np.nan
            var_95 = np.percentile(returns, 5)
            return returns[returns <= var_95].mean()

        portfolio_snapshots["cvar_95_30d"] = (
            portfolio_snapshots["daily_return"].rolling(window=30).apply(calculate_cvar)
        )

        # Calculate downside deviation
        portfolio_snapshots["downside_deviation"] = (
            portfolio_snapshots["daily_return"]
            .rolling(window=30)
            .apply(lambda x: np.sqrt(np.mean(np.minimum(x - x.mean(), 0) ** 2)))
        )

        # Calculate Sortino ratio
        portfolio_snapshots["sortino_ratio"] = (
            (portfolio_snapshots["daily_return"].rolling(window=252).mean() * 252)
            - risk_free_rate
        ) / (portfolio_snapshots["downside_deviation"] * np.sqrt(252))

        # Calculate Calmar ratio (annual return / maximum drawdown)
        portfolio_snapshots["annual_return"] = (
            portfolio_snapshots["daily_return"].rolling(window=252).mean() * 252
        )
        portfolio_snapshots["calmar_ratio"] = portfolio_snapshots[
            "annual_return"
        ] / abs(portfolio_snapshots["max_drawdown"])

        # Calculate information ratio (excess return / tracking error)
        portfolio_snapshots["tracking_error"] = portfolio_snapshots[
            "daily_return"
        ].rolling(window=252).std() * np.sqrt(252)
        portfolio_snapshots["information_ratio"] = (
            portfolio_snapshots["excess_return"].rolling(window=252).mean()
            * 252
            / portfolio_snapshots["tracking_error"]
        )

        # Select relevant columns
        risk_metrics = portfolio_snapshots[
            [
                "date",
                "var_95_30d",
                "cvar_95_30d",
                "downside_deviation",
                "sortino_ratio",
                "calmar_ratio",
                "information_ratio",
                "volatility_30d",
                "max_drawdown",
            ]
        ]

        risk_metrics = risk_metrics.replace([np.inf, -np.inf], np.nan).fillna(0)
        return risk_metrics

    except Exception as e:
        print(f"Error calculating risk metrics: {e}")
        raise


def calculate_allocation_analysis(
    current_positions: pd.DataFrame, portfolio_snapshots: pd.DataFrame
) -> pd.DataFrame:
    """
    Perform asset allocation analysis.
    Returns: DataFrame with allocation metrics.
    """
    try:
        print("Calculating allocation analysis...")

        # Current allocation analysis
        current_allocation = current_positions.copy()
        current_allocation["allocation_fraction"] = (
            current_allocation["percentage_allocation"] / 100
        )
        current_allocation["allocation_rank"] = current_allocation[
            "percentage_allocation"
        ].rank(ascending=False)
        current_allocation["concentration_risk"] = (
            current_allocation["allocation_fraction"] ** 2
        )  # Herfindahl index component

        # Calculate diversification metrics
        total_concentration = current_allocation["concentration_risk"].sum()
        effective_assets = 1 / total_concentration if total_concentration > 0 else 0

        # Add portfolio-level metrics
        current_allocation["portfolio_concentration"] = total_concentration
        current_allocation["effective_assets"] = effective_assets
        current_allocation["diversification_score"] = (
            1 - total_concentration
        )  # Higher is better

        # Historical allocation analysis
        portfolio_snapshots["date"] = pd.to_datetime(portfolio_snapshots["date"])

        # Calculate allocation changes over time
        allocation_history = []

        for _, snapshot in portfolio_snapshots.iterrows():
            # This is a simplified version - in reality you'd need to parse the asset_quantities and asset_values
            # For now, we'll use the number of assets as a proxy
            allocation_record = {
                "date": snapshot["date"],
                "num_assets": snapshot["num_assets"],
                "total_value": snapshot["total_portfolio_value"],
                "concentration_index": (
                    1 / snapshot["num_assets"] if snapshot["num_assets"] > 0 else 1
                ),
            }
            allocation_history.append(allocation_record)

        allocation_df = pd.DataFrame(allocation_history)

        # Calculate allocation stability metrics
        allocation_df["allocation_change"] = allocation_df["num_assets"].diff()
        allocation_df["value_change"] = allocation_df["total_value"].pct_change()

        return current_allocation.drop(columns=["allocation_fraction"])

    except Exception as e:
        print(f"Error calculating allocation analysis: {e}")
        raise


def calculate_market_insights(
    prices: pd.DataFrame, market_overview: pd.DataFrame
) -> pd.DataFrame:
    """
    Generate market insights and trends.
    Returns: DataFrame with market insights.
    """
    try:
        print("Calculating market insights...")

        # Convert date columns to datetime
        prices["date"] = pd.to_datetime(prices["date"])

        # Calculate asset-specific insights
        asset_insights = []

        for asset in prices["asset_symbol"].unique():
            asset_prices = prices[prices["asset_symbol"] == asset].sort_values("date")

            if len(asset_prices) < 30:
                continue

            # Calculate rolling metrics
            asset_prices["price_ma_30d"] = (
                asset_prices["close_price"].rolling(window=30).mean()
            )
            asset_prices["price_ma_90d"] = (
                asset_prices["close_price"].rolling(window=90).mean()
            )
            asset_prices["volatility_30d"] = asset_prices[
                "close_price"
            ].pct_change().rolling(window=30).std() * np.sqrt(252)

            # Calculate momentum indicators
            asset_prices["momentum_30d"] = (
                asset_prices["close_price"] / asset_prices["close_price"].shift(30) - 1
            )
            asset_prices["momentum_90d"] = (
                asset_prices["close_price"] / asset_prices["close_price"].shift(90) - 1
            )

            # Calculate RSI (simplified)
            delta = asset_prices["close_price"].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            asset_prices["rsi"] = 100 - (100 / (1 + rs))

            # Get latest values
            latest = asset_prices.iloc[-1]

            insight = {
                "asset_symbol": asset,
                "current_price": latest["close_price"],
                "price_ma_30d": latest["price_ma_30d"],
                "price_ma_90d": latest["price_ma_90d"],
                "volatility_30d": latest["volatility_30d"],
                "momentum_30d": latest["momentum_30d"],
                "momentum_90d": latest["momentum_90d"],
                "rsi": latest["rsi"],
                "trend": (
                    "bullish"
                    if latest["close_price"]
                    > latest["price_ma_30d"]
                    > latest["price_ma_90d"]
                    else (
                        "bearish"
                        if latest["close_price"]
                        < latest["price_ma_30d"]
                        < latest["price_ma_90d"]
                        else "neutral"
                    )
                ),
                "volatility_level": (
                    "high"
                    if latest["volatility_30d"] > 0.5
                    else "medium" if latest["volatility_30d"] > 0.3 else "low"
                ),
            }
            asset_insights.append(insight)

        # Return asset insights
        market_insights = pd.DataFrame(asset_insights)

        return market_insights

    except Exception as e:
        print(f"Error calculating market insights: {e}")
        raise


def calculate_correlation_metrics(prices):
    """Calculate correlation metrics between assets."""
    try:
        print("Calculating correlation metrics...")

        # Convert to DataFrame
        prices_df = pd.DataFrame(prices)
        prices_df["date"] = pd.to_datetime(prices_df["date"])

        # Remove duplicates if they exist (keep last occurrence)
        prices_df = prices_df.drop_duplicates(
            subset=["date", "asset_symbol"], keep="last"
        )

        # Check if we have enough data
        if len(prices_df) == 0:
            print("No price data available for correlation calculation")
            return pd.DataFrame(
                {
                    "calculation_date": [pd.Timestamp.now()],
                    "avg_correlation": [0.0],
                    "diversification_score": [0.0],
                    "correlation_matrix": [json.dumps({})],
                    "notes": ["No price data available"],
                }
            )

        # Pivot price data to have assets as columns and dates as index
        try:
            price_pivot = prices_df.pivot(
                index="date", columns="asset_symbol", values="close_price"
            ).fillna(0)
        except ValueError as e:
            print(f"Pivot failed, trying alternative approach: {e}")
            # Alternative approach: group by date and asset, take mean if duplicates
            prices_df = (
                prices_df.groupby(["date", "asset_symbol"])["close_price"]
                .mean()
                .reset_index()
            )
            price_pivot = prices_df.pivot(
                index="date", columns="asset_symbol", values="close_price"
            ).fillna(0)

        # Check if we have at least 2 assets
        if len(price_pivot.columns) < 2:
            print(
                f"Only {len(price_pivot.columns)} asset(s) available, cannot calculate correlation"
            )
            return pd.DataFrame(
                {
                    "calculation_date": [pd.Timestamp.now()],
                    "avg_correlation": [0.0],
                    "diversification_score": [0.0],
                    "correlation_matrix": [json.dumps({})],
                    "notes": [f"Only {len(price_pivot.columns)} asset(s) available"],
                }
            )

        # Calculate daily returns
        returns = price_pivot.pct_change().dropna()

        # Check if we have enough return data
        if len(returns) < 2:
            print("Insufficient return data for correlation calculation")
            return pd.DataFrame(
                {
                    "calculation_date": [pd.Timestamp.now()],
                    "avg_correlation": [0.0],
                    "diversification_score": [0.0],
                    "correlation_matrix": [json.dumps({})],
                    "notes": ["Insufficient return data"],
                }
            )

        # Calculate correlation matrix
        correlation_matrix = returns.corr()

        # Calculate average correlation
        if len(returns.columns) < 2:
            avg_correlation = 0.0
            diversification_score = 0.0
        else:
            # Get upper triangle of correlation matrix (excluding diagonal)
            upper_triangle = correlation_matrix.where(
                np.triu(np.ones(correlation_matrix.shape), k=1).astype(bool)
            )
            avg_correlation = upper_triangle.stack().mean()
            # Lower average correlation = better diversification
            diversification_score = 1 - abs(avg_correlation)

        # Create a DataFrame to store the results
        correlation_df = pd.DataFrame(
            {
                "calculation_date": [pd.Timestamp.now()],
                "avg_correlation": [avg_correlation],
                "diversification_score": [diversification_score],
                "correlation_matrix": [json.dumps(correlation_matrix.to_dict())],
                "notes": [f"Correlation calculated for {len(returns.columns)} assets."],
            }
        )

        return correlation_df

    except Exception as e:
        print(f"Error calculating correlation metrics: {e}")
        # Return a valid DataFrame with error information
        return pd.DataFrame(
            {
                "calculation_date": [pd.Timestamp.now()],
                "avg_correlation": [0.0],
                "diversification_score": [0.0],
                "correlation_matrix": [json.dumps({})],
                "notes": [f"Error in calculation: {str(e)}"],
            }
        )


def save_analytics_to_s3(environment: str, timestamp: str, analytics_data: Dict) -> str:
    """
    Save analytics data to S3 as CSV files.
    """
    try:
        s3_helper = S3Helper()

        print("Saving analytics data to S3...")

        # Save performance metrics
        performance_key = f"{environment}/coinbase/analytics/performance_metrics.csv"
        s3_helper.upload_csv_to_s3(
            analytics_data["performance_metrics"], performance_key
        )
        print(f"Performance metrics saved: {performance_key}")

        # Save risk metrics
        risk_key = f"{environment}/coinbase/analytics/risk_metrics.csv"
        s3_helper.upload_csv_to_s3(analytics_data["risk_metrics"], risk_key)
        print(f"Risk metrics saved: {risk_key}")

        # Save allocation analysis
        allocation_key = f"{environment}/coinbase/analytics/allocation_analysis.csv"
        s3_helper.upload_csv_to_s3(
            analytics_data["allocation_analysis"], allocation_key
        )
        print(f"Allocation analysis saved: {allocation_key}")

        # Save market insights
        market_insights_key = f"{environment}/coinbase/analytics/market_insights.csv"
        s3_helper.upload_csv_to_s3(
            analytics_data["market_insights"], market_insights_key
        )
        print(f"Market insights saved: {market_insights_key}")

        # Save correlation metrics
        correlation_key = f"{environment}/coinbase/analytics/correlation_metrics.csv"
        s3_helper.upload_csv_to_s3(
            analytics_data["correlation_metrics"], correlation_key
        )
        print(f"Correlation metrics saved: {correlation_key}")

        return timestamp

    except Exception as e:
        print(f"Error saving analytics to S3: {e}")
        raise


def main():
    """
    Main analytics calculation function - equivalent to Analytics Calculator Lambda.
    """
    print("=== Starting Coinbase Analytics Calculation ===")
    print(
        f"Analytics calculation started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    try:
        from configuration.secrets import ENVIRONMENT

        # Load processed data
        processed_data = load_processed_data(ENVIRONMENT)

        # Calculate analytics
        print("\n1. Calculating performance metrics...")
        performance_metrics = calculate_performance_metrics(
            processed_data["portfolio_snapshots"], processed_data["pnl_tracking"]
        )

        print("\n2. Calculating risk metrics...")
        risk_metrics = calculate_risk_metrics(
            processed_data["portfolio_snapshots"], processed_data["pnl_tracking"]
        )

        print("\n3. Calculating allocation analysis...")
        allocation_analysis = calculate_allocation_analysis(
            processed_data["current_positions"], processed_data["portfolio_snapshots"]
        )

        print("\n4. Calculating market insights...")
        market_insights = calculate_market_insights(
            processed_data["prices"], processed_data["market_overview"]
        )

        print("\n5. Calculating correlation metrics...")
        correlation_metrics = calculate_correlation_metrics(processed_data["prices"])

        # Save analytics
        print("\n6. Saving analytics to S3...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        analytics_data = {
            "performance_metrics": performance_metrics,
            "risk_metrics": risk_metrics,
            "allocation_analysis": allocation_analysis,
            "market_insights": market_insights,
            "correlation_metrics": correlation_metrics,
        }

        save_analytics_to_s3(ENVIRONMENT, timestamp, analytics_data)

        print(f"\n=== Coinbase Analytics Calculation Completed Successfully ===")
        print(f"Analytics calculation completed at: {timestamp}")

        return {
            "status": "success",
            "timestamp": timestamp,
            "analytics_created": list(analytics_data.keys()),
            "records_processed": {
                "performance_metrics": len(performance_metrics),
                "risk_metrics": len(risk_metrics),
                "allocation_analysis": len(allocation_analysis),
                "market_insights": len(market_insights),
                "correlation_metrics": len(correlation_metrics),
            },
        }

    except Exception as e:
        print(f"\n!!! Analytics calculation failed with error: {e} !!!")
        raise


if __name__ == "__main__":
    main()
