import json
from pathlib import Path
from typing import Any

from .base_connector import BaseConnector


class JSONConnector(BaseConnector):
    """
    Reads vendor-format JSON containing nested samples → assays → files.
    Returns the raw dict for flattening in the integration layer.
    """

    def extract(self) -> dict[str, Any]:
        with self.source_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        n_samples = len(data.get("samples", []))
        print(f"[JSONConnector] Loaded {n_samples} samples from {self.source_name}")
        return data
