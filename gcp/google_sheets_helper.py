# gcp/google_sheets_helper.py
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials


class GoogleSheetsHelper:
    """Helper class for Google Sheets API operations."""

    def __init__(self):
        """Initializes the helper and authenticates."""
        self.scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
        ]
        self.client = self._authenticate()

    def _authenticate(self):
        """Authenticates with Google Sheets API using credentials from secrets.py."""
        try:
            from configuration.secrets import GCP_SERVICE_ACCOUNT_INFO

            creds = Credentials.from_service_account_info(
                GCP_SERVICE_ACCOUNT_INFO, scopes=self.scopes
            )
            return gspread.authorize(creds)
        except ImportError:
            raise ImportError(
                "GCP_SERVICE_ACCOUNT_INFO not found in `configuration/secrets.py`.\n"
                "Please add the service account dictionary to your secrets file. "
                "See `configuration/secrets_template.py` for the required structure."
            )
        except Exception as e:
            raise ConnectionError(f"Failed to authenticate with Google Sheets: {e}")

    def get_worksheet_as_dataframe(
        self, spreadsheet_id: str, worksheet_name: str
    ) -> pd.DataFrame:
        """Fetches a worksheet and returns it as a pandas DataFrame."""
        try:
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            worksheet = spreadsheet.worksheet(worksheet_name)
            data = worksheet.get_all_records()
            return pd.DataFrame(data)
        except gspread.exceptions.SpreadsheetNotFound:
            raise FileNotFoundError(
                f"Spreadsheet with ID '{spreadsheet_id}' not found."
            )
        except gspread.exceptions.WorksheetNotFound:
            raise FileNotFoundError(f"Worksheet '{worksheet_name}' not found.")
        except Exception as e:
            raise RuntimeError(
                f"Failed to fetch data from worksheet '{worksheet_name}': {e}"
            )
