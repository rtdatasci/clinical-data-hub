import pandas as pd
from pathlib import Path

from .base_connector import BaseConnector


class GoogleSheetsConnector(BaseConnector):
    """
    Mock Google Sheets connector.

    In production this would use google-auth + gspread to fetch a real sheet.
    Here it reads a pre-generated CSV that represents what the Sheets API
    would return, making the project self-contained without credentials.
    """

    def __init__(self, source_path: Path, sheet_name: str = "Study Metadata"):
        self.sheet_name = sheet_name
        super().__init__(source_path)

    def extract(self) -> pd.DataFrame:
        df = pd.read_csv(self.source_path)
        print(
            f"[GoogleSheetsConnector] Mock-fetched sheet '{self.sheet_name}' "
            f"-> {len(df)} rows from {self.source_name}"
        )
        return df
