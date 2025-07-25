# pensions/staging/create_pensions_staging_tables.py
import pandas as pd
import sys
import os
from datetime import datetime

# Add project root to path to allow importing helpers
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from aws.connect_to_s3 import S3Helper


class PensionsStagingCreator:
    """Creates pension performance staging tables from cleansed data."""

    def __init__(self):
        """Initializes the creator with an S3 helper."""
        self.s3_helper = S3Helper()
        self.pension_platforms = ["Wahed", "Standard Life"]

    def find_latest_cleansed_files(self, base_s3_path: str):
        """Finds the most recent cleansed snapshots and cashflows files."""
        print("Searching for the latest cleansed pension files...")
        all_cleansed_files = self.s3_helper.list_files(prefix=base_s3_path)

        snapshot_files = sorted(
            [f for f in all_cleansed_files if "pensions_snapshots_cleansed" in f],
            reverse=True,
        )
        cashflow_files = sorted(
            [f for f in all_cleansed_files if "pensions_cashflows_cleansed" in f],
            reverse=True,
        )

        if not snapshot_files:
            raise FileNotFoundError("No cleansed snapshot files found.")
        if not cashflow_files:
            raise FileNotFoundError("No cleansed cashflow files found.")

        latest_snapshots_key = snapshot_files[0]
        latest_cashflows_key = cashflow_files[0]

        print(f"Found latest snapshots file: {latest_snapshots_key}")
        print(f"Found latest cashflows file: {latest_cashflows_key}")

        return latest_snapshots_key, latest_cashflows_key

    def calculate_performance_timeseries(
        self, snapshots_df: pd.DataFrame, cashflows_df: pd.DataFrame
    ):
        """Calculates a detailed, event-driven gain/loss timeseries for each pension."""
        all_performance_data = {}

        for platform in self.pension_platforms:
            print(f"\n--- Processing performance for {platform} ---")

            platform_cashflows = cashflows_df[
                cashflows_df["platform"] == platform
            ].copy()
            platform_snapshots = snapshots_df[
                snapshots_df["platform"] == platform
            ].copy()

            if platform_cashflows.empty or platform_snapshots.empty:
                print(f"Not enough data for {platform}. Skipping.")
                continue

            # --- Step 1: Prepare and combine all cashflow and snapshot events ---
            platform_cashflows = platform_cashflows.sort_values("timestamp")
            platform_cashflows["cash_invested"] = platform_cashflows["value"].cumsum()
            platform_snapshots = platform_snapshots.rename(
                columns={"value": "pension_value"}
            )

            events_df = pd.concat(
                [
                    platform_cashflows[["timestamp", "cash_invested"]],
                    platform_snapshots[["timestamp", "pension_value"]],
                ]
            ).sort_values("timestamp")

            # Aggregate events on the same timestamp, keeping the last value for each column
            events_df = events_df.groupby("timestamp").last().reset_index()

            # --- Step 2: Use linear interpolation for imputed value between snapshots ---
            events_df = events_df.set_index("timestamp")
            events_df["imputed_pension_value"] = events_df["pension_value"].interpolate(
                method="time"
            )

            # --- Step 3: Fill remaining gaps ---
            # Forward-fill cash invested to all rows
            events_df["cash_invested"] = events_df["cash_invested"].ffill()
            # For periods before the first snapshot, impute value as cash invested
            events_df["imputed_pension_value"] = events_df[
                "imputed_pension_value"
            ].fillna(events_df["cash_invested"])
            # For periods after the last snapshot, carry the last known imputed value forward
            events_df["imputed_pension_value"] = events_df[
                "imputed_pension_value"
            ].ffill()

            events_df = events_df.dropna(
                subset=["cash_invested", "imputed_pension_value"]
            ).reset_index()

            # --- Step 4: Calculate all gain/loss metrics ---
            # Standard gain/loss (will have NaN where there's no real snapshot)
            events_df["gain_loss_absolute"] = (
                events_df["pension_value"] - events_df["cash_invested"]
            )
            events_df["gain_loss_percentage"] = 100 * (
                events_df["gain_loss_absolute"] / events_df["cash_invested"]
            )

            # Imputed gain/loss (will always have a value)
            events_df["imputed_gain_loss_absolute"] = (
                events_df["imputed_pension_value"] - events_df["cash_invested"]
            )
            events_df["imputed_gain_loss_percentage"] = 100 * (
                events_df["imputed_gain_loss_absolute"] / events_df["cash_invested"]
            )

            # --- Step 5: Final Schema ---
            final_df = events_df[
                [
                    "timestamp",
                    "cash_invested",
                    "pension_value",
                    "imputed_pension_value",
                    "gain_loss_absolute",
                    "gain_loss_percentage",
                    "imputed_gain_loss_absolute",
                    "imputed_gain_loss_percentage",
                ]
            ].copy()

            for col in final_df.columns[1:]:  # Round all numeric columns
                final_df[col] = final_df[col].round(2)

            all_performance_data[platform] = final_df
            print(f"Successfully calculated event-driven performance for {platform}.")

        return all_performance_data

    def save_staging_data(self, performance_data: dict, base_s3_path: str):
        """Saves the performance timeseries data to the staging layer in S3."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        for platform, df in performance_data.items():
            platform_name_snake_case = platform.lower().replace(" ", "_")
            staging_key = (
                f"{base_s3_path}/timeseries_{platform_name_snake_case}_{timestamp}.csv"
            )

            print(f"Uploading {platform} staging data to: {staging_key}")
            self.s3_helper.upload_csv_to_s3(df, staging_key, index=False)


def main():
    """Main execution function to run the pensions staging pipeline."""
    print("--- Starting Pensions Data Staging ---")
    try:
        from configuration.secrets import ENVIRONMENT

        cleansed_base_path = f"{ENVIRONMENT}/pensions/cleansed"
        staging_base_path = f"{ENVIRONMENT}/pensions/staging"

        staging_creator = PensionsStagingCreator()

        # 1. Find latest cleansed files
        snapshots_key, cashflows_key = staging_creator.find_latest_cleansed_files(
            cleansed_base_path
        )

        # 2. Load cleansed data
        snapshots_df = staging_creator.s3_helper.read_csv_from_s3(
            snapshots_key, parse_dates=["timestamp"]
        )
        cashflows_df = staging_creator.s3_helper.read_csv_from_s3(
            cashflows_key, parse_dates=["timestamp"]
        )

        # 3. Calculate performance
        performance_data = staging_creator.calculate_performance_timeseries(
            snapshots_df, cashflows_df
        )

        # 4. Save staging data
        if performance_data:
            staging_creator.save_staging_data(performance_data, staging_base_path)
            print("\n--- Pensions Data Staging Successful ---")
        else:
            print("\n--- No performance data was generated. Pipeline finished. ---")

    except Exception as e:
        print(f"\n!!! An error occurred during the staging process: {e} !!!")
        sys.exit(1)


if __name__ == "__main__":
    main()
