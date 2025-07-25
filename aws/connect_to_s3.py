import boto3
import pandas as pd
import io
import os
from typing import List

# Import secrets
try:
    from configuration.secrets import (
        AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY,
        AWS_REGION,
        S3_BUCKET_NAME,
    )
except ImportError:
    raise ImportError(
        "AWS credentials not found. Please create configuration/secrets.py with:\n"
        "AWS_ACCESS_KEY_ID = 'your_access_key'\n"
        "AWS_SECRET_ACCESS_KEY = 'your_secret_key'\n"
        "AWS_REGION = 'your_region'\n"
        "S3_BUCKET_NAME = 'your_bucket_name'"
    )


class S3Helper:
    """AWS S3 operations for CSV files."""

    def __init__(self, bucket_name: str = None, region_name: str = None):
        self.bucket_name = bucket_name or S3_BUCKET_NAME
        self.region_name = region_name or AWS_REGION
        self._connect_to_s3()

    def _connect_to_s3(self):
        """Establish S3 connection."""
        try:
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=self.region_name,
            )

            # Test connection
            self.s3_client.list_objects_v2(Bucket=self.bucket_name, MaxKeys=1)
        except Exception as e:
            raise ConnectionError(f"Failed to connect to S3: {str(e)}")

    def list_files(self, prefix: str = "") -> List[str]:
        """List files in bucket with optional prefix filter."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=prefix
            )
            return [obj["Key"] for obj in response.get("Contents", [])]
        except Exception as e:
            raise RuntimeError(f"Error listing files: {str(e)}")

    def read_csv_from_s3(self, key: str, **pandas_kwargs) -> pd.DataFrame:
        """Read CSV file from S3 into DataFrame."""
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            return pd.read_csv(obj["Body"], **pandas_kwargs)
        except Exception as e:
            raise RuntimeError(f"Error reading CSV from S3: {str(e)}")

    def download_csv_from_s3(
        self, key: str, local_path: str, **pandas_kwargs
    ) -> pd.DataFrame:
        """Download CSV from S3 to local path and return DataFrame."""
        try:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            self.s3_client.download_file(self.bucket_name, key, local_path)
            return pd.read_csv(local_path, **pandas_kwargs)
        except Exception as e:
            raise RuntimeError(f"Error downloading CSV from S3: {str(e)}")

    def upload_csv_to_s3(
        self, df: pd.DataFrame, key: str, index: bool = False, **pandas_kwargs
    ):
        """Upload DataFrame as CSV to S3."""
        try:
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=index, **pandas_kwargs)
            self.s3_client.put_object(
                Bucket=self.bucket_name, Key=key, Body=csv_buffer.getvalue()
            )
        except Exception as e:
            raise RuntimeError(f"Error uploading DataFrame to S3: {str(e)}")

    def upload_file_to_s3(self, local_path: str, key: str):
        """Upload local file to S3."""
        try:
            self.s3_client.upload_file(local_path, self.bucket_name, key)
        except Exception as e:
            raise RuntimeError(f"Error uploading file to S3: {str(e)}")

    def delete_file_from_s3(self, key: str):
        """Delete file from S3."""
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)
        except Exception as e:
            raise RuntimeError(f"Error deleting file from S3: {str(e)}")

    def file_exists(self, key: str) -> bool:
        """Check if file exists in S3."""
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except:
            return False


if __name__ == "__main__":
    # Example usage
    try:
        s3_helper = S3Helper()
        files = s3_helper.list_files()
        print(f"Files in bucket: {files}")
    except Exception as e:
        print(f"Error: {e}")
