"""
Coinbase Data Transformer - Lambda Function Equivalent
Converts raw API data into structured CSV format following the ETL strategy data model.
"""

import sys
import os
import json
from datetime import datetime, timedelta
import pandas as pd
from typing import Dict, List, Optional

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aws.connect_to_s3 import S3Helper


def load_latest_raw_data(environment: str) -> Dict:
    """
    Load the latest raw data from S3.
    Returns: Dictionary containing all raw data types.
    """
    try:
        s3_helper = S3Helper()

        # Load latest raw data
        print("Loading latest raw data from S3...")

        # Load positions
        positions_key = f"{environment}/coinbase/raw/positions/latest/positions.json"
        if s3_helper.file_exists(positions_key):
            positions_data = json.loads(
                s3_helper.s3_client.get_object(
                    Bucket=s3_helper.bucket_name, Key=positions_key
                )["Body"]
                .read()
                .decode("utf-8")
            )
        else:
            raise FileNotFoundError(f"Positions data not found: {positions_key}")

        # Load prices
        prices_key = f"{environment}/coinbase/raw/prices/latest/prices.json"
        if s3_helper.file_exists(prices_key):
            prices_data = json.loads(
                s3_helper.s3_client.get_object(
                    Bucket=s3_helper.bucket_name, Key=prices_key
                )["Body"]
                .read()
                .decode("utf-8")
            )
        else:
            raise FileNotFoundError(f"Prices data not found: {prices_key}")

        # Load market data
        market_key = f"{environment}/coinbase/raw/market/latest/market.json"
        if s3_helper.file_exists(market_key):
            market_data = json.loads(
                s3_helper.s3_client.get_object(
                    Bucket=s3_helper.bucket_name, Key=market_key
                )["Body"]
                .read()
                .decode("utf-8")
            )
        else:
            raise FileNotFoundError(f"Market data not found: {market_key}")

        return {
            "positions": positions_data,
            "prices": prices_data,
            "market": market_data,
        }

    except Exception as e:
        print(f"Error loading raw data: {e}")
        raise


def transform_current_positions(positions_data: Dict) -> pd.DataFrame:
    """
    Transform current positions data.
    Returns: Cleaned and structured current positions DataFrame.
    """
    try:
        print("Transforming current positions data...")

        positions = positions_data.get("positions", [])
        if not positions:
            return pd.DataFrame()

        # Convert to DataFrame
        df = pd.DataFrame(positions)

        # Ensure numeric columns are properly typed
        numeric_columns = [
            "quantity",
            "current_value_gbp",
            "average_purchase_price",
            "unrealized_pnl",
            "percentage_allocation",
        ]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Calculate additional fields
        df["cost_basis"] = df["quantity"] * df["average_purchase_price"]
        df["unrealized_pnl_percentage"] = (
            df["unrealized_pnl"] / df["cost_basis"] * 100
        ).fillna(0)

        # Sort by current value (descending)
        df = df.sort_values("current_value_gbp", ascending=False)

        return df

    except Exception as e:
        print(f"Error transforming current positions: {e}")
        raise


def transform_price_history(prices_data: Dict) -> pd.DataFrame:
    """
    Transform price history data.
    Returns: DataFrame with cleaned and structured price data.
    """
    try:
        print("Transforming price history data...")

        prices = prices_data["prices"]
        if not prices:
            return pd.DataFrame()

        # Convert to DataFrame
        df = pd.DataFrame(prices)

        # Convert UNIX timestamp to datetime
        df["date"] = pd.to_datetime(df["date"].astype(int), unit="s")

        # Ensure numeric columns are properly typed
        numeric_columns = [
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
        ]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Calculate additional fields
        df["price_range"] = df["high_price"] - df["low_price"]
        df["price_change"] = df["close_price"] - df["open_price"]
        df["price_change_pct"] = (df["price_change"] / df["open_price"] * 100).fillna(0)

        # Sort by date
        df = df.sort_values("date")

        return df

    except Exception as e:
        print(f"Error transforming price history: {e}")
        raise


def create_portfolio_snapshots(positions_data: Dict, prices_data: Dict) -> pd.DataFrame:
    """
    Create portfolio snapshots over time using current positions and historical prices.
    Returns: DataFrame with portfolio composition over time.
    """
    try:
        print("Creating portfolio snapshots...")

        positions = positions_data.get("positions", [])
        prices = prices_data["prices"]

        if not positions or not prices:
            return pd.DataFrame()

        # Convert to DataFrames
        positions_df = pd.DataFrame(positions)
        prices_df = pd.DataFrame(prices)
        prices_df["date"] = pd.to_datetime(prices_df["date"].astype(int), unit="s")

        # Get unique dates from price data
        unique_dates = sorted(prices_df["date"].unique())

        snapshots = []

        for date in unique_dates:
            date_prices = prices_df[prices_df["date"] == date]
            portfolio_value = 0
            asset_values = {}

            for _, position in positions_df.iterrows():
                asset_symbol = position["asset_symbol"]
                quantity = position["quantity"]

                # Find price for this asset on this date
                asset_price = date_prices[date_prices["asset_symbol"] == asset_symbol]
                if not asset_price.empty:
                    price = asset_price["close_price"].iloc[0]
                    asset_value = quantity * price
                    portfolio_value += asset_value
                    asset_values[asset_symbol] = asset_value

            if portfolio_value > 0:  # Only add snapshots with meaningful data
                snapshot = {
                    "date": date,
                    "total_portfolio_value": portfolio_value,
                    "num_assets": len([k for k, v in asset_values.items() if v > 0]),
                    "timestamp": date.isoformat(),
                }
                snapshots.append(snapshot)

        return pd.DataFrame(snapshots)

    except Exception as e:
        print(f"Error creating portfolio snapshots: {e}")
        return pd.DataFrame()


def create_pnl_tracking(positions_data: Dict, prices_data: Dict) -> pd.DataFrame:
    """
    Create P&L tracking over time using current positions and price history.
    Returns: DataFrame with P&L tracking over time.
    """
    try:
        print("Creating P&L tracking data...")

        positions = positions_data.get("positions", [])
        prices = prices_data["prices"]

        if not positions or not prices:
            return pd.DataFrame()

        # Convert to DataFrames
        positions_df = pd.DataFrame(positions)
        prices_df = pd.DataFrame(prices)
        prices_df["date"] = pd.to_datetime(prices_df["date"].astype(int), unit="s")

        # Get unique dates from price data
        unique_dates = sorted(prices_df["date"].unique())

        pnl_data = []

        for date in unique_dates:
            date_prices = prices_df[prices_df["date"] == date]

            total_unrealized_pnl = 0
            total_cost_basis = 0

            for _, position in positions_df.iterrows():
                asset_symbol = position["asset_symbol"]
                quantity = position["quantity"]
                avg_purchase_price = position["average_purchase_price"]

                # Find price for this asset on this date
                asset_price = date_prices[date_prices["asset_symbol"] == asset_symbol]
                if not asset_price.empty:
                    current_price = asset_price["close_price"].iloc[0]

                    # Calculate P&L
                    cost_basis = quantity * avg_purchase_price
                    current_value = quantity * current_price
                    unrealized_pnl = current_value - cost_basis

                    total_cost_basis += cost_basis
                    total_unrealized_pnl += unrealized_pnl

            if total_cost_basis > 0:
                pnl_entry = {
                    "date": date,
                    "total_cost_basis": total_cost_basis,
                    "total_current_value": total_cost_basis + total_unrealized_pnl,
                    "unrealized_pnl": total_unrealized_pnl,
                    "unrealized_pnl_percentage": (
                        total_unrealized_pnl / total_cost_basis * 100
                    ),
                    "timestamp": date.isoformat(),
                }
                pnl_data.append(pnl_entry)

        return pd.DataFrame(pnl_data)

    except Exception as e:
        print(f"Error creating P&L tracking: {e}")
        return pd.DataFrame()


def create_market_overview(market_data: Dict) -> pd.DataFrame:
    """
    Create market overview data.
    Returns: DataFrame with market overview information.
    """
    try:
        print("Creating market overview data...")

        asset_market_data = market_data.get("asset_market_data", [])
        if not asset_market_data:
            return pd.DataFrame()

        # Convert to DataFrame
        df = pd.DataFrame(asset_market_data)

        # Ensure numeric columns are properly typed
        numeric_columns = [
            "current_price",
            "best_bid",
            "best_ask",
            "bid_ask_spread",
            "spread_percentage",
            "volume_24h",
            "price_change_24h",
            "price_change_percentage_24h",
        ]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Sort by volume (descending)
        df = df.sort_values("volume_24h", ascending=False)

        return df

    except Exception as e:
        print(f"Error creating market overview: {e}")
        raise


def save_processed_data_to_s3(
    environment: str, timestamp: str, dataframes: Dict
) -> str:
    """
    Save processed data to S3 as CSV files.
    """
    try:
        s3_helper = S3Helper()
        date_folder = datetime.now().strftime("%Y/%m/%d")

        print("Saving processed data to S3...")

        # Save current data
        current_positions_key = (
            f"{environment}/coinbase/processed/current/positions.csv"
        )
        s3_helper.upload_csv_to_s3(
            dataframes["current_positions"], current_positions_key
        )
        print(f"Current positions saved: {current_positions_key}")

        # Save historical data
        prices_key = f"{environment}/coinbase/processed/historical/prices.csv"
        s3_helper.upload_csv_to_s3(dataframes["prices"], prices_key)
        print(f"Prices saved: {prices_key}")

        portfolio_snapshots_key = (
            f"{environment}/coinbase/processed/historical/portfolio_snapshots.csv"
        )
        s3_helper.upload_csv_to_s3(
            dataframes["portfolio_snapshots"], portfolio_snapshots_key
        )
        print(f"Portfolio snapshots saved: {portfolio_snapshots_key}")

        pnl_tracking_key = (
            f"{environment}/coinbase/processed/historical/pnl_tracking.csv"
        )
        s3_helper.upload_csv_to_s3(dataframes["pnl_tracking"], pnl_tracking_key)
        print(f"P&L tracking saved: {pnl_tracking_key}")

        # Save market data
        market_overview_key = (
            f"{environment}/coinbase/processed/current/market_overview.csv"
        )
        s3_helper.upload_csv_to_s3(dataframes["market_overview"], market_overview_key)
        print(f"Market overview saved: {market_overview_key}")

        # Create portfolio summary
        portfolio_summary = {
            "timestamp": datetime.now().isoformat(),
            "total_portfolio_value": dataframes["current_positions"][
                "current_value_gbp"
            ].sum(),
            "num_assets": len(dataframes["current_positions"]),
            "total_unrealized_pnl": dataframes["current_positions"][
                "unrealized_pnl"
            ].sum(),
            "total_unrealized_pnl_pct": (
                (
                    dataframes["current_positions"]["unrealized_pnl"].sum()
                    / dataframes["current_positions"]["cost_basis"].sum()
                    * 100
                )
                if dataframes["current_positions"]["cost_basis"].sum() > 0
                else 0
            ),
        }

        portfolio_summary_df = pd.DataFrame([portfolio_summary])
        portfolio_summary_key = (
            f"{environment}/coinbase/processed/current/portfolio_summary.csv"
        )
        s3_helper.upload_csv_to_s3(portfolio_summary_df, portfolio_summary_key)
        print(f"Portfolio summary saved: {portfolio_summary_key}")

        return timestamp

    except Exception as e:
        print(f"Error saving processed data to S3: {e}")
        raise


def main():
    """
    Main data transformation function - equivalent to Data Transformer Lambda.
    """
    print("=== Starting Coinbase Data Transformation ===")
    print(f"Transformation started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        from configuration.secrets import ENVIRONMENT

        # Load raw data
        raw_data = load_latest_raw_data(ENVIRONMENT)

        # Transform data
        print("\n1. Transforming current positions...")
        current_positions = transform_current_positions(raw_data["positions"])

        print("\n2. Transforming price history...")
        prices = transform_price_history(raw_data["prices"])

        print("\n3. Creating portfolio snapshots...")
        portfolio_snapshots = create_portfolio_snapshots(
            raw_data["positions"], raw_data["prices"]
        )

        print("\n4. Creating P&L tracking...")
        pnl_tracking = create_pnl_tracking(raw_data["positions"], raw_data["prices"])

        print("\n5. Creating market overview...")
        market_overview = create_market_overview(raw_data["market"])

        # Save processed data
        print("\n6. Saving processed data to S3...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        dataframes = {
            "current_positions": current_positions,
            "prices": prices,
            "portfolio_snapshots": portfolio_snapshots,
            "pnl_tracking": pnl_tracking,
            "market_overview": market_overview,
        }

        save_processed_data_to_s3(ENVIRONMENT, timestamp, dataframes)

        print(f"\n=== Coinbase Data Transformation Completed Successfully ===")
        print(f"Transformation completed at: {timestamp}")

        return {
            "status": "success",
            "timestamp": timestamp,
            "dataframes_created": list(dataframes.keys()),
            "records_processed": {
                "current_positions": len(current_positions),
                "prices": len(prices),
                "portfolio_snapshots": len(portfolio_snapshots),
                "pnl_tracking": len(pnl_tracking),
                "market_overview": len(market_overview),
            },
        }

    except Exception as e:
        print(f"\n!!! Data transformation failed with error: {e} !!!")
        raise


if __name__ == "__main__":
    main()
