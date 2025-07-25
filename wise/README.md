# Wise Account Data Pipeline

This folder contains the scripts for processing Wise bank statements, from raw data to dashboard-ready staging tables.

## Data Pipeline Overview

The pipeline consists of two main scripts that should be run in order:

1.  **`cleansed/create_wise_cleansed_tables.py`**
    *   **Input:** Raw Wise statement CSVs from the `raw/` directory in your S3 bucket.
    *   **Action:** Cleans the data, standardizes formats, and categorizes transactions.
    *   **Output:** A timestamped, cleansed transaction file in the `cleansed/` directory in S3.

2.  **`staging/create_wise_staging_tables.py`**
    *   **Input:** The latest cleansed transaction file from the `cleansed/` directory.
    *   **Action:** Aggregates the cleansed data to create a daily balance summary.
    *   **Output:** A timestamped daily balance file in the `staging/` directory, ready to be used by the dashboard.

### S3 Folder Structure

The scripts expect the following folder structure within your S3 bucket's environment (`develop` or `production`):

```
{ENVIRONMENT}/
└── bank-statements/
    └── wise-gbp/
        ├── raw/
        ├── cleansed/
        └── staging/
```

## How to Run the Pipeline

To process your Wise statements, simply run the main pipeline script from the root of the repository. It will execute the cleansing and staging steps in the correct order.

```bash
py wise/run_pipeline.py
```

After running this script, the processed data will be available in the `staging` directory in your S3 bucket. 