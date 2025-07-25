#!/usr/bin/env python3
"""
Create Wise staging tables from cleansed transaction data.
Generates daily and monthly balance summaries for dashboard.
"""

import pandas as pd
import glob
from datetime import datetime
from typing import List, Optional
import sys
import os

# Add parent directory to path for S3 connector
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from aws.connect_to_s3 import S3Helper


class WiseStagingTables:
    """Create staging tables from cleansed Wise data."""

    def __init__(self):
        self.s3 = S3Helper()

    def find_latest_cleansed_file(self, base_path: str) -> str:
        """Find the most recent cleansed file by timestamp."""
        try:
            # List all cleansed files
            files = self.s3.list_files(
                prefix=f"{base_path}/cleansed/wise_transactions_cleansed_"
            )

            if not files:
                raise FileNotFoundError("No cleansed files found")

            # Sort by filename (timestamp) to get latest
            files.sort(reverse=True)
            latest_file = files[0]

            print(f"Found latest cleansed file: {latest_file}")
            return latest_file

        except Exception as e:
            raise RuntimeError(f"Failed to find latest cleansed file: {str(e)}")

    def load_cleansed_data(self, file_key: str) -> pd.DataFrame:
        """Load cleansed transaction data from S3."""
        try:
            print(f"Loading cleansed data from: {file_key}")
            df = self.s3.read_csv_from_s3(file_key)

            # Convert datetime to pandas datetime
            df["datetime"] = pd.to_datetime(df["datetime"])
            df["date"] = df["datetime"].dt.date

            print(f"Loaded {len(df)} transactions")
            return df

        except Exception as e:
            raise RuntimeError(f"Failed to load cleansed data: {str(e)}")

    def validate_cleansed_data(self, df: pd.DataFrame) -> bool:
        """Validate cleansed data quality."""
        print("Validating cleansed data...")

        # Check required fields
        required_fields = ["datetime", "amount", "running_balance", "transaction_type"]
        for field in required_fields:
            if field not in df.columns:
                raise ValueError(f"Missing required field: {field}")

        # Check for valid dates
        if df["datetime"].isnull().any():
            print("Warning: Found transactions with null datetime")

        # Check for valid amounts
        if df["amount"].isnull().any():
            print("Warning: Found transactions with null amounts")

        # Check transaction types
        type_counts = df["transaction_type"].value_counts()
        print("Transaction type distribution:")
        for ttype, count in type_counts.items():
            print(f"  {ttype}: {count}")

        print("Cleansed data validation passed")
        return True

    def calculate_daily_balances(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate daily balance summaries."""
        print("Calculating daily balances...")

        # Group by date
        daily_data = []

        for date, group in df.groupby("date"):
            # Sort by datetime within the day
            group = group.sort_values("datetime")

            # Get first and last transactions of the day
            first_txn = group.iloc[0]
            last_txn = group.iloc[-1]

            # Calculate opening balance (first transaction's running balance - amount)
            opening_balance = first_txn["running_balance"] - first_txn["amount"]
            closing_balance = last_txn["running_balance"]

            # Calculate net change
            net_change = closing_balance - opening_balance

            # Sum amounts by transaction type
            deposits = group[group["transaction_type"] == "transfer_in"]["amount"].sum()
            withdrawals = abs(
                group[group["transaction_type"].isin(["transfer_out", "card"])][
                    "amount"
                ].sum()
            )
            fees = abs(group[group["transaction_type"] == "fee"]["amount"].sum())

            daily_data.append(
                {
                    "date": date.strftime("%Y-%m-%d"),
                    "opening_balance": round(opening_balance, 2),
                    "closing_balance": round(closing_balance, 2),
                    "net_change": round(net_change, 2),
                    "transaction_count": len(group),
                    "deposits": round(deposits, 2),
                    "withdrawals": round(withdrawals, 2),
                    "fees": round(fees, 2),
                }
            )

        # Create DataFrame and sort by date
        daily_df = pd.DataFrame(daily_data)
        daily_df = daily_df.sort_values("date")

        print(f"Calculated daily balances for {len(daily_df)} days")
        return daily_df

    def validate_staging_data(self, daily_df: pd.DataFrame) -> bool:
        """Validate staging table calculations."""
        print("Validating staging data...")

        if len(daily_df) == 0:
            print("Warning: No daily balance data generated")
            return False

        # Check for negative balances (if any)
        negative_daily = daily_df[daily_df["closing_balance"] < 0]
        if len(negative_daily) > 0:
            print(f"Warning: {len(negative_daily)} days with negative closing balance")

        print("Staging data validation completed")
        return True

    def save_staging_data(self, daily_df: pd.DataFrame, base_path: str):
        """Save staging table to S3."""
        try:
            # Generate timestamp for filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Save daily balances
            daily_key = f"{base_path}/staging/wise_balance_daily_{timestamp}.csv"
            print(f"Saving daily balances to: {daily_key}")
            self.s3.upload_csv_to_s3(daily_df, daily_key, index=False)

            print("Staging table saved successfully")

        except Exception as e:
            raise RuntimeError(f"Failed to save staging data: {str(e)}")

    def process_staging_tables(self, base_path: str):
        """Main processing pipeline."""
        print("=" * 50)
        print("Wise Staging Tables Pipeline")
        print("=" * 50)

        # Find latest cleansed file
        latest_file = self.find_latest_cleansed_file(base_path)

        # Load cleansed data
        df_cleansed = self.load_cleansed_data(latest_file)

        # Validate cleansed data
        self.validate_cleansed_data(df_cleansed)

        # Calculate daily balances
        daily_df = self.calculate_daily_balances(df_cleansed)

        # Validate staging data
        self.validate_staging_data(daily_df)

        # Save staging table
        self.save_staging_data(daily_df, base_path)

        print("=" * 50)
        print("Pipeline completed successfully!")
        print(f"Input: {len(df_cleansed)} transactions")
        print(f"Output: {len(daily_df)} daily records")
        print("=" * 50)


def main():
    """Main execution function."""
    try:
        # Get environment from secrets
        try:
            from configuration.secrets import ENVIRONMENT
        except ImportError:
            ENVIRONMENT = "develop"  # Default to develop

        # Configuration based on environment
        base_path = f"{ENVIRONMENT}/bank-statements/wise-gbp"

        print(f"Environment: {ENVIRONMENT}")
        print(f"Base path: {base_path}")

        # Process staging tables
        staging = WiseStagingTables()
        staging.process_staging_tables(base_path)

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
