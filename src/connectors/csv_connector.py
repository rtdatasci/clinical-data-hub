import pandas as pd
from pathlib import Path

from .base_connector import BaseConnector


class CSVConnector(BaseConnector):
    """Reads patient demographic data from a CSV file."""

    def __init__(self, source_path: Path, encoding: str = "utf-8"):
        self.encoding = encoding
        super().__init__(source_path)

    def extract(self) -> pd.DataFrame:
        df = pd.read_csv(self.source_path, encoding=self.encoding)
        print(f"[CSVConnector] Extracted {len(df)} rows from {self.source_name}")
        return df
