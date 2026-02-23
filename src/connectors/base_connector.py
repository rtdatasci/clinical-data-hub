from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any


class BaseConnector(ABC):
    """Abstract base for all data source connectors."""

    def __init__(self, source_path: Path):
        self.source_path = Path(source_path)
        self._validate_source()

    def _validate_source(self) -> None:
        if not self.source_path.exists():
            raise FileNotFoundError(
                f"[{self.__class__.__name__}] Source not found: {self.source_path}"
            )

    @abstractmethod
    def extract(self) -> Any:
        """Return raw extracted data."""
        ...

    @property
    def source_name(self) -> str:
        return self.source_path.name

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(source={self.source_path})"
