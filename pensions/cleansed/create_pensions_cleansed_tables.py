# pensions/cleansed/create_pensions_cleansed_tables.py
import pandas as pd
import sys
import os
from datetime import datetime
import re

# Add project root to path to allow importing helpers
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from aws.connect_to_s3 import S3Helper


class PensionsDataCleaner:
    """Cleans and transforms raw pensions data from S3."""

    def __init__(self):
        """Initializes the cleaner with an S3 helper."""
        self.s3_helper = S3Helper()
        self.pension_platforms = ["Wahed", "Standard Life"]

    def find_latest_raw_files(self, base_s3_path: str):
        """Finds the most recent raw snapshots and cashflows files in S3."""
        print("Searching for the latest raw pension files...")

        all_raw_files = self.s3_helper.list_files(prefix=base_s3_path)

        snapshot_files = sorted(
            [f for f in all_raw_files if "asset_snapshots_raw" in f], reverse=True
        )
        cashflow_files = sorted(
            [f for f in all_raw_files if "cashflows_raw" in f], reverse=True
        )

        if not snapshot_files:
            raise FileNotFoundError("No raw asset snapshot files found.")
        if not cashflow_files:
            raise FileNotFoundError("No raw cashflow files found.")

        latest_snapshots_key = snapshot_files[0]
        latest_cashflows_key = cashflow_files[0]

        print(f"Found latest snapshots file: {latest_snapshots_key}")
        print(f"Found latest cashflows file: {latest_cashflows_key}")

        return latest_snapshots_key, latest_cashflows_key

    def clean_value_column(self, series: pd.Series) -> pd.Series:
        """Converts a currency string series to a numeric series."""
        return pd.to_numeric(
            series.astype(str).str.replace(r"[£,]", "", regex=True), errors="coerce"
        )

    def clean_dataframes(self, snapshots_df: pd.DataFrame, cashflows_df: pd.DataFrame):
        """Filters, cleans, and standardizes the raw pension data."""
        print("Cleaning and transforming raw dataframes...")

        # --- Clean Snapshots DataFrame ---
        # Filter for pension platforms
        snapshots_df = snapshots_df[
            snapshots_df["Platform"].isin(self.pension_platforms)
        ].copy()

        # Standardize column names
        snapshots_df.columns = [
            col.lower().replace(" ", "_") for col in snapshots_df.columns
        ]

        # Clean data types
        snapshots_df["value"] = self.clean_value_column(snapshots_df["value"])
        snapshots_df["timestamp"] = pd.to_datetime(
            snapshots_df["timestamp"], dayfirst=True
        )

        # Drop unused column
        if "token_amount" in snapshots_df.columns:
            snapshots_df = snapshots_df.drop(columns=["token_amount"])

        print(f"Cleansed {len(snapshots_df)} pension snapshot records.")

        # --- Clean Cashflows DataFrame ---
        # Filter for pension platforms
        cashflows_df = cashflows_df[
            cashflows_df["Platform"].isin(self.pension_platforms)
        ].copy()

        # Standardize column names
        cashflows_df.columns = [
            col.lower().replace(" ", "_") for col in cashflows_df.columns
        ]

        # Clean data types
        cashflows_df["value"] = self.clean_value_column(cashflows_df["value"])
        cashflows_df["timestamp"] = pd.to_datetime(
            cashflows_df["timestamp"], dayfirst=True
        )

        print(f"Cleansed {len(cashflows_df)} pension cashflow records.")

        return snapshots_df, cashflows_df

    def save_cleansed_data(
        self, snapshots_df: pd.DataFrame, cashflows_df: pd.DataFrame, base_s3_path: str
    ):
        """Uploads the cleansed dataframes to the 'cleansed' layer in S3."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        snapshots_key = f"{base_s3_path}/pensions_snapshots_cleansed_{timestamp}.csv"
        cashflows_key = f"{base_s3_path}/pensions_cashflows_cleansed_{timestamp}.csv"

        print(f"Uploading cleansed snapshots to: {snapshots_key}")
        self.s3_helper.upload_csv_to_s3(snapshots_df, snapshots_key, index=False)

        print(f"Uploading cleansed cashflows to: {cashflows_key}")
        self.s3_helper.upload_csv_to_s3(cashflows_df, cashflows_key, index=False)


def main():
    """
    Main execution function to run the pensions cleansing pipeline.
    """
    print("--- Starting Pensions Data Cleansing ---")
    try:
        from configuration.secrets import ENVIRONMENT

        raw_base_path = f"{ENVIRONMENT}/pensions/raw"
        cleansed_base_path = f"{ENVIRONMENT}/pensions/cleansed"

        cleaner = PensionsDataCleaner()

        # 1. Find latest raw files
        snapshots_key, cashflows_key = cleaner.find_latest_raw_files(raw_base_path)

        # 2. Load raw data
        snapshots_df = cleaner.s3_helper.read_csv_from_s3(snapshots_key)
        cashflows_df = cleaner.s3_helper.read_csv_from_s3(cashflows_key)

        # 3. Clean and transform data
        cleansed_snapshots_df, cleansed_cashflows_df = cleaner.clean_dataframes(
            snapshots_df, cashflows_df
        )

        # 4. Save cleansed data
        cleaner.save_cleansed_data(
            cleansed_snapshots_df, cleansed_cashflows_df, cleansed_base_path
        )

        print("\n--- Pensions Data Cleansing Successful ---")

    except Exception as e:
        print(f"\n!!! An error occurred during the cleansing process: {e} !!!")
        sys.exit(1)


if __name__ == "__main__":
    main()
