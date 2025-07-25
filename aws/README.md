# S3 CSV Connector

Simple AWS S3 connector for CSV file operations.

## Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure credentials** in `configuration/secrets.py`:
   ```python
   AWS_ACCESS_KEY_ID = "your_access_key"
   AWS_SECRET_ACCESS_KEY = "your_secret_key"
   AWS_REGION = "your_region"
   S3_BUCKET_NAME = "your_bucket_name"
   ```

## Usage

```python
from aws.connect_to_s3 import S3Helper

# Initialize (single connection for multiple operations)
s3 = S3Helper()

# Read CSV directly from S3
df = s3.read_csv_from_s3("path/to/file.csv")

# Download CSV to local file
df = s3.download_csv_from_s3("s3_path/file.csv", "local_file.csv")

# Upload DataFrame to S3
s3.upload_csv_to_s3(df, "uploaded_data.csv")

# Upload local file to S3
s3.upload_file_to_s3("local_file.csv", "s3_path/file.csv")

# List files
files = s3.list_files("folder/")

# Check if file exists
if s3.file_exists("path/to/file.csv"):
    print("File exists")

# Delete file
s3.delete_file_from_s3("path/to/file.csv")
```

## IAM Policy

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::BUCKET_NAME",
                "arn:aws:s3:::BUCKET_NAME/*"
            ]
        }
    ]
}
```

## Features

- **Efficient connection management** - single S3 connection for multiple operations
- **Automatic credential detection** from `configuration/secrets.py` or environment variables
- **Pandas integration** - all CSV operations return DataFrames
- **Error handling** with descriptive error messages
- **Directory creation** - automatically creates local directories when downloading
- **Flexible parameters** - supports all pandas CSV options 