# Pensions Data Pipeline

This folder contains the scripts for processing personal pension data, from raw data extraction from Google Sheets to the creation of detailed performance analysis tables.

## Data Pipeline Overview

The pipeline consists of three main scripts that are executed in order by the main `run_pipeline.py` script:

1.  **`raw/create_pensions_raw_tables.py`**
    *   **Input:** Data from two worksheets ("Balance Sheet", "Pension Cashflows") in a Google Sheet.
    *   **Action:** Fetches the raw data using the Google Sheets API.
    *   **Output:** Two timestamped raw CSV files (`asset_snapshots_raw_*.csv`, `cashflows_raw_*.csv`) in the `raw/` directory in S3.

2.  **`cleansed/create_pensions_cleansed_tables.py`**
    *   **Input:** The latest raw CSV files from the `raw/` directory.
    *   **Action:** Filters the data to include only pension platforms (`Wahed`, `Standard Life`), cleans data types, and standardizes formats.
    *   **Output:** Two timestamped cleansed files (`pensions_snapshots_cleansed_*.csv`, `pensions_cashflows_cleansed_*.csv`) in the `cleansed/` directory in S3.

3.  **`staging/create_pensions_staging_tables.py`**
    *   **Input:** The latest cleansed files from the `cleansed/` directory.
    *   **Action:** Performs advanced performance analysis. It calculates the cumulative cash invested and uses linear interpolation to create a detailed, event-driven timeseries of the pension's value, absolute gain/loss, and percentage gain/loss.
    *   **Output:** A separate, timestamped performance timeseries table for each pension provider in the `staging/` directory.

### S3 Folder Structure

The scripts expect the following folder structure within your S3 bucket's environment (`develop` or `production`):

```
{ENVIRONMENT}/
└── pensions/
    ├── raw/
    ├── cleansed/
    └── staging/
```

## How to Run the Pipeline

To process your pensions data, simply run the main pipeline script from the root of the repository. It will execute all three steps (`raw`, `cleansed`, `staging`) in the correct order.

```bash
py pensions/run_pipeline.py
```

After running this script, the final performance analysis tables will be available in the `staging` directory in your S3 bucket. 