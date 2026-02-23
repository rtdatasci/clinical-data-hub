"""
SQLite persistence layer.

Strategy: Pydantic model → model_dump() → pandas DataFrame → df.to_sql()
No ORM. Uses stdlib sqlite3 + pandas.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from src.schema.models import Assay, Biomarker, File, Patient, Sample, Study

# ---------------------------------------------------------------------------
# DDL — tables created in FK dependency order
# ---------------------------------------------------------------------------
DDL = [
    ("studies", """
        CREATE TABLE IF NOT EXISTS studies (
            study_id         TEXT PRIMARY KEY,
            name             TEXT NOT NULL,
            protocol_number  TEXT UNIQUE NOT NULL,
            phase            TEXT NOT NULL,
            therapeutic_area TEXT NOT NULL,
            start_date       TEXT NOT NULL,
            status           TEXT NOT NULL,
            pi_name          TEXT NOT NULL,
            sponsor          TEXT NOT NULL
        )
    """),
    ("patients", """
        CREATE TABLE IF NOT EXISTS patients (
            patient_id        TEXT PRIMARY KEY,
            study_id          TEXT NOT NULL REFERENCES studies(study_id),
            age               INTEGER NOT NULL,
            sex               TEXT NOT NULL,
            race              TEXT NOT NULL,
            ethnicity         TEXT NOT NULL,
            primary_diagnosis TEXT NOT NULL,
            enrollment_date   TEXT NOT NULL,
            site_id           TEXT NOT NULL,
            status            TEXT NOT NULL
        )
    """),
    ("samples", """
        CREATE TABLE IF NOT EXISTS samples (
            sample_id        TEXT PRIMARY KEY,
            patient_id       TEXT NOT NULL REFERENCES patients(patient_id),
            sample_type      TEXT NOT NULL,
            collection_date  TEXT NOT NULL,
            tissue_type      TEXT NOT NULL,
            quality_score    REAL NOT NULL,
            volume_ml        REAL NOT NULL,
            storage_location TEXT NOT NULL
        )
    """),
    ("assays", """
        CREATE TABLE IF NOT EXISTS assays (
            assay_id    TEXT PRIMARY KEY,
            sample_id   TEXT NOT NULL REFERENCES samples(sample_id),
            assay_type  TEXT NOT NULL,
            vendor      TEXT NOT NULL,
            platform    TEXT NOT NULL,
            run_date    TEXT NOT NULL,
            run_id      TEXT UNIQUE NOT NULL,
            status      TEXT NOT NULL,
            qc_pass     INTEGER NOT NULL
        )
    """),
    ("files", """
        CREATE TABLE IF NOT EXISTS files (
            file_id      TEXT PRIMARY KEY,
            assay_id     TEXT NOT NULL REFERENCES assays(assay_id),
            file_name    TEXT NOT NULL,
            file_type    TEXT NOT NULL,
            file_path    TEXT NOT NULL,
            file_size_mb REAL NOT NULL,
            checksum     TEXT NOT NULL,
            upload_date  TEXT NOT NULL
        )
    """),
    ("biomarkers", """
        CREATE TABLE IF NOT EXISTS biomarkers (
            biomarker_id     TEXT PRIMARY KEY,
            assay_id         TEXT NOT NULL REFERENCES assays(assay_id),
            gene_symbol      TEXT NOT NULL,
            ensembl_id       TEXT NOT NULL,
            log2_fold_change REAL,
            pvalue           REAL,
            padj             REAL,
            expression_tpm   REAL,
            variant_type     TEXT
        )
    """),
]


class SQLiteStore:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    # ------------------------------------------------------------------
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            for table_name, ddl in DDL:
                conn.execute(ddl)
        print(f"[SQLiteStore] Schema ready: {self.db_path.name}")

    # ------------------------------------------------------------------
    def _write(self, models: list, table: str, date_cols: list[str] | None = None) -> None:
        if not models:
            return
        records = []
        for m in models:
            d = m.model_dump()
            for col in (date_cols or []):
                if col in d and d[col] is not None:
                    d[col] = str(d[col])
            # SQLite stores booleans as integers
            for k, v in d.items():
                if isinstance(v, bool):
                    d[k] = int(v)
            records.append(d)
        df = pd.DataFrame(records)
        with self._connect() as conn:
            df.to_sql(table, conn, if_exists="append", index=False)

    # ------------------------------------------------------------------
    def write_studies(self, models: list[Study]) -> None:
        self._write(models, "studies", date_cols=["start_date"])

    def write_patients(self, models: list[Patient]) -> None:
        self._write(models, "patients", date_cols=["enrollment_date"])

    def write_samples(self, models: list[Sample]) -> None:
        self._write(models, "samples", date_cols=["collection_date"])

    def write_assays(self, models: list[Assay]) -> None:
        self._write(models, "assays", date_cols=["run_date"])

    def write_files(self, models: list[File]) -> None:
        self._write(models, "files", date_cols=["upload_date"])

    def write_biomarkers(self, models: list[Biomarker]) -> None:
        self._write(models, "biomarkers")

    def query(self, sql: str) -> pd.DataFrame:
        """General-purpose query used by the dashboard."""
        with self._connect() as conn:
            return pd.read_sql_query(sql, conn)

    def table_counts(self) -> dict[str, int]:
        tables = [t for t, _ in DDL]
        counts = {}
        with self._connect() as conn:
            for t in tables:
                counts[t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        return counts
