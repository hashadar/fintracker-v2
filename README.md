# FinTracker V2

A personal finance tracking application designed to process bank statements, generate financial insights, and visualize account data.

## Project Overview

This repository contains a set of Python scripts to build a data pipeline for processing Wise bank statements. It fetches raw data from an S3 bucket, cleans and transforms it, creates staging tables for analysis, and provides an optional dashboard for visualization.

### Core Components

1.  **AWS S3 Connector (`aws/connect_to_s3.py`)**
    *   A reusable helper module to securely connect to and interact with AWS S3.
    *   Handles reading, writing, and listing files in an S3 bucket.

2.  **Wise Data Pipeline (`wise/`)**
    *   A two-step ETL pipeline for processing Wise statements.
    *   **Cleansing:** `wise/cleansed/create_wise_cleansed_tables.py` transforms raw data into a clean, analysis-ready format.
    *   **Staging:** `wise/staging/create_wise_staging_tables.py` aggregates the cleansed data into a daily balance summary for a dashboard.
    *   **Execution:** `wise/run_pipeline.py` runs the entire pipeline with a single command.

3.  **Streamlit Dashboard (`streamlit/Home.py`)**
    *   An optional, simple web-based dashboard to visualize the processed data.
    *   Features charts for balance trends, net change analysis, and a cash flow calendar heatmap.

## Getting Started

Follow these steps to set up and run the project.

### 1. Prerequisites

*   Python 3.8+
*   An AWS account with an S3 bucket and proper IAM credentials.

### 2. Installation

Clone the repository and install the required dependencies:

```bash
git clone <repository-url>
cd fintracker-v2
pip install -r requirements.txt
```

### 3. Configuration

All project configuration is managed through a single secrets file.

1.  **Create a `configuration` directory** at the root of the project.
2.  Inside it, create a `secrets.py` file with the following content:

    ```python
    # configuration/secrets.py

    # "develop" or "production"
    ENVIRONMENT = "develop" 

    # AWS Credentials
    AWS_ACCESS_KEY_ID = "your_aws_access_key_id"
    AWS_SECRET_ACCESS_KEY = "your_aws_secret_access_key"
    AWS_REGION = "your_aws_region" # e.g., "us-east-1"
    
    # S3 Bucket Configuration
    S3_BUCKET_NAME = "your_s3_bucket_name"
    ```

3.  This `secrets.py` file is listed in `.gitignore` and will not be committed to version control.

### 4. S3 Bucket Structure

The pipeline scripts expect the following folder structure within your S3 bucket, based on the `ENVIRONMENT` variable set in your `secrets.py`:

```
{your_s3_bucket_name}/
└── {ENVIRONMENT}/
    └── bank-statements/
        └── wise-gbp/
            ├── raw/       # -> Place your raw Wise statement CSVs here
            ├── cleansed/  # <- Cleansed data will be stored here
            └── staging/   # <- Dashboard-ready data will be stored here
```

## Running the Pipeline

To process your Wise statements, run the main pipeline script from the root of the repository. This will execute both the cleansing and staging steps in the correct order.

```bash
py wise/run_pipeline.py
```

## Viewing the Dashboard (Optional)

If you wish to visualize the processed data, you can run the Streamlit dashboard:

```bash
streamlit run streamlit/Home.py
```

This will open a local web server where you can view the charts and data tables. 