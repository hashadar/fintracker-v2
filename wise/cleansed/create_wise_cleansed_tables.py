#!/usr/bin/env python3
"""
Create cleansed Wise transaction tables from raw statement CSV.
Transforms raw 30-column statement into analysis-ready format.
"""

import pandas as pd
import re
from datetime import datetime
from typing import List, Dict, Optional
import sys
import os

# Add parent directory to path for S3 connector
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from aws.connect_to_s3 import S3Helper


class WiseDataCleaner:
    """Clean and transform Wise statement data."""

    def __init__(self):
        self.s3 = S3Helper()

    def load_raw_data(self, file_key: str) -> pd.DataFrame:
        """Load raw Wise statement from S3."""
        try:
            print(f"Loading raw data from: {file_key}")
            df = self.s3.read_csv_from_s3(file_key)
            print(f"Loaded {len(df)} transactions")
            return df
        except Exception as e:
            raise RuntimeError(f"Failed to load raw data: {str(e)}")

    def validate_raw_data(self, df: pd.DataFrame) -> bool:
        """Validate required columns exist in raw data."""
        required_columns = [
            "Date Time",
            "Amount",
            "Currency",
            "Description",
            "Running Balance",
        ]

        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        print("Raw data validation passed")
        return True

    def categorize_transaction(self, row: pd.Series) -> str:
        """Categorize transaction based on description only."""
        description = str(row.get("Description", "")).strip()

        # GBP Assets service fee - fee charged for open balance
        if description == "GBP Assets service fee":
            return "fee"

        # Received money - transfer in
        if description.startswith("Received money"):
            return "transfer_in"

        # Sent money - transfer out
        if description.startswith("Sent money"):
            return "transfer_out"

        # Card transaction - transfer out
        if description.startswith("Card transaction"):
            return "card"

        # Wise Charges for - fee for a transaction
        if description.startswith("Wise Charges for"):
            return "fee"

        # Default category
        return "other"

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw data into cleansed format."""
        print("Transforming data...")

        # Create cleansed dataframe
        cleansed = pd.DataFrame()

        # Core fields
        cleansed["datetime"] = pd.to_datetime(
            df["Date Time"], format="%d-%m-%Y %H:%M:%S.%f"
        )
        cleansed["amount"] = pd.to_numeric(df["Amount"], errors="coerce")
        cleansed["running_balance"] = pd.to_numeric(
            df["Running Balance"], errors="coerce"
        )
        cleansed["currency"] = df["Currency"]

        # Categorize transactions
        cleansed["transaction_type"] = df.apply(self.categorize_transaction, axis=1)

        # Original description
        cleansed["description"] = df["Description"]

        # Sort by datetime
        cleansed = cleansed.sort_values(["datetime"])

        # Remove duplicates based on datetime, amount, and description
        cleansed = cleansed.drop_duplicates(
            subset=["datetime", "amount", "description"]
        )

        print(f"Transformed {len(cleansed)} transactions")
        return cleansed

    def validate_cleansed_data(self, df: pd.DataFrame) -> bool:
        """Validate cleansed data quality."""
        print("Validating cleansed data...")

        # Check for required fields
        required_fields = ["datetime", "amount", "running_balance"]
        for field in required_fields:
            if df[field].isnull().any():
                print(f"Warning: Missing values in {field}")

        # Check for valid dates
        invalid_dates = df[df["datetime"].isnull()]
        if len(invalid_dates) > 0:
            print(f"Warning: {len(invalid_dates)} transactions with invalid dates")

        # Check for valid amounts
        invalid_amounts = df[df["amount"].isnull()]
        if len(invalid_amounts) > 0:
            print(f"Warning: {len(invalid_amounts)} transactions with invalid amounts")

        # Check transaction type distribution
        type_counts = df["transaction_type"].value_counts()
        print("Transaction type distribution:")
        for ttype, count in type_counts.items():
            print(f"  {ttype}: {count}")

        print("Cleansed data validation completed")
        return True

    def save_cleansed_data(self, df: pd.DataFrame, output_key: str):
        """Save cleansed data to S3."""
        try:
            print(f"Saving cleansed data to: {output_key}")
            self.s3.upload_csv_to_s3(df, output_key, index=False)
            print("Cleansed data saved successfully")
        except Exception as e:
            raise RuntimeError(f"Failed to save cleansed data: {str(e)}")

    def process_wise_statement(self, input_key: str, output_key: str):
        """Main processing pipeline."""
        print("=" * 50)
        print("Wise Statement Cleansing Pipeline")
        print("=" * 50)

        # Load raw data
        df_raw = self.load_raw_data(input_key)

        # Validate raw data
        self.validate_raw_data(df_raw)

        # Transform data
        df_cleansed = self.transform_data(df_raw)

        # Validate cleansed data
        self.validate_cleansed_data(df_cleansed)

        # Save cleansed data
        self.save_cleansed_data(df_cleansed, output_key)

        print("=" * 50)
        print("Pipeline completed successfully!")
        print(f"Input: {len(df_raw)} transactions")
        print(f"Output: {len(df_cleansed)} transactions")
        print("=" * 50)


def main():
    """Main execution function."""
    try:
        # Get environment from secrets
        try:
            from configuration.secrets import ENVIRONMENT
        except ImportError:
            ENVIRONMENT = "develop"  # Default to develop

        # Generate timestamp for filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Configuration based on environment
        base_path = f"{ENVIRONMENT}/bank-statements/wise-gbp"
        input_key = f"{base_path}/raw/statement_29519495_GBP_2025-01-01_2025-07-25.csv"
        output_key = f"{base_path}/cleansed/wise_transactions_cleansed_{timestamp}.csv"

        print(f"Environment: {ENVIRONMENT}")
        print(f"Input path: {input_key}")
        print(f"Output path: {output_key}")

        # Process the statement
        cleaner = WiseDataCleaner()
        cleaner.process_wise_statement(input_key, output_key)

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
