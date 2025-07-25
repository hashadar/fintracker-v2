# Google Cloud Platform (GCP) Utilities

## Google Sheets Helper

The `google_sheets_helper.py` module provides a simple interface for reading data from Google Sheets using the Google Sheets API.

### Features

- **Service Account Authentication**: Secure access using GCP service account credentials
- **Sheet Reading**: Read entire worksheets or specific ranges
- **DataFrame Integration**: Direct conversion to pandas DataFrames
- **Error Handling**: Robust error handling for API and authentication issues

### Usage

```python
from gcp.google_sheets_helper import GoogleSheetsHelper

# Initialize helper
sheets_helper = GoogleSheetsHelper()

# Read entire worksheet
df = sheets_helper.read_sheet_to_dataframe(sheet_id, "Sheet1")

# Read specific range
df = sheets_helper.read_sheet_to_dataframe(sheet_id, "Sheet1!A1:E100")
```

### Configuration

Requires `GCP_SERVICE_ACCOUNT_INFO` and `GOOGLE_SHEET_ID` to be configured in `configuration/secrets.py`. 