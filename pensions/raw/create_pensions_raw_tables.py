# pensions/raw/create_pensions_raw_tables.py
import sys
import os
from datetime import datetime

# Add project root to path to allow importing helpers
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from gcp.google_sheets_helper import GoogleSheetsHelper
from aws.connect_to_s3 import S3Helper


def main():
    """
    Fetches pension data from Google Sheets and saves the raw, untouched
    tables to the 'raw' layer in S3.
    """
    print("--- Starting Raw Pensions Data Ingestion ---")

    try:
        # --- Configuration ---
        from configuration.secrets import GOOGLE_SHEET_ID, ENVIRONMENT

        # Names of the worksheets in your Google Sheet
        snapshots_worksheet_name = "Balance Sheet"
        cashflows_worksheet_name = "Pension Cashflows"

        # --- Initialize Helpers ---
        print("Initializing helpers...")
        gcp_helper = GoogleSheetsHelper()
        s3_helper = S3Helper()

        # --- Fetch Data from Google Sheets ---
        print(f"Fetching data from Google Sheet ID: {GOOGLE_SHEET_ID}")

        # Fetch asset snapshots
        snapshots_df = gcp_helper.get_worksheet_as_dataframe(
            spreadsheet_id=GOOGLE_SHEET_ID, worksheet_name=snapshots_worksheet_name
        )

        # Fetch cashflows
        cashflows_df = gcp_helper.get_worksheet_as_dataframe(
            spreadsheet_id=GOOGLE_SHEET_ID, worksheet_name=cashflows_worksheet_name
        )

        # --- Upload Raw Data to S3 ---
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_path = f"{ENVIRONMENT}/pensions/raw"

        # Define S3 file paths
        snapshots_s3_key = f"{base_path}/asset_snapshots_raw_{timestamp}.csv"
        cashflows_s3_key = f"{base_path}/cashflows_raw_{timestamp}.csv"

        # Upload snapshots
        print(
            f"Uploading {len(snapshots_df)} snapshot records to S3 at: {snapshots_s3_key}"
        )
        s3_helper.upload_csv_to_s3(snapshots_df, snapshots_s3_key, index=False)

        # Upload cashflows
        print(
            f"Uploading {len(cashflows_df)} cashflow records to S3 at: {cashflows_s3_key}"
        )
        s3_helper.upload_csv_to_s3(cashflows_df, cashflows_s3_key, index=False)

        print("\n--- Raw Pensions Data Ingestion Successful ---")

    except Exception as e:
        print(f"\n!!! An error occurred during the raw data ingestion process: {e} !!!")
        sys.exit(1)


if __name__ == "__main__":
    main()
