"""
Template for secrets.py configuration file.
Copy this file to secrets.py and fill in your actual API keys and credentials.
This template file can be safely committed to version control.
"""

# "develop" or "production"
ENVIRONMENT = "develop"

# AWS Credentials
AWS_ACCESS_KEY_ID = "your_aws_access_key_id"
AWS_SECRET_ACCESS_KEY = "your_aws_secret_access_key"
AWS_REGION = "your_aws_region"  # e.g., "us-east-1"
S3_BUCKET_NAME = "your_s3_bucket_name"

# Google Sheets Configuration
GOOGLE_SHEET_ID = "your_google_sheet_id_here"  # Found in the URL of your sheet

# Google Cloud Platform Service Account Credentials
# Paste the contents of your GCP service account JSON key file here.
GCP_SERVICE_ACCOUNT_INFO = {
    "type": "service_account",
    "project_id": "your_project_id",
    "private_key_id": "your_private_key_id",
    "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
    "client_email": "your-service-account-email@your-project-id.iam.gserviceaccount.com",
    "client_id": "your_client_id",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/your-service-account-email%40your-project-id.iam.gserviceaccount.com",
}
