"""
ETL Pipeline orchestrator.

For each entity:
  1. Extract raw data via connector
  2. Validate + transform to Pydantic model
  3. Write to SQLite via SQLiteStore

Validation errors are collected but do not halt the pipeline.
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import ValidationError

from config.settings import BIOMARKER_TXT, DB_PATH, PATIENTS_CSV, STUDY_SHEETS_CSV, VENDOR_JSON
from src.connectors.csv_connector import CSVConnector
from src.connectors.json_connector import JSONConnector
from src.connectors.pdf_connector import PDFConnector
from src.connectors.sheets_connector import GoogleSheetsConnector
from src.integration.transformers import (
    transform_assay,
    transform_biomarker,
    transform_file,
    transform_patient,
    transform_sample,
    transform_study,
)
from src.schema.models import Assay, Biomarker, File, Patient, Sample, Study
from src.storage.sqlite_store import SQLiteStore

log = logging.getLogger(__name__)


class ETLPipeline:
    def __init__(self):
        self.store = SQLiteStore(DB_PATH)
        self.errors: list[dict] = []

    def _safe_transform(self, transformer, raw: dict, entity: str) -> Any | None:
        try:
            return transformer(raw)
        except (ValidationError, KeyError, ValueError, TypeError) as e:
            self.errors.append({"entity": entity, "error": str(e)})
            log.debug(f"[{entity}] validation skip: {e}")
            return None

    # ------------------------------------------------------------------
    # Phase 1: Studies (from mock Google Sheets CSV)
    # ------------------------------------------------------------------
    def run_studies(self) -> list[Study]:
        conn = GoogleSheetsConnector(STUDY_SHEETS_CSV)
        df = conn.extract()
        models = [
            m for row in df.to_dict(orient="records")
            if (m := self._safe_transform(transform_study, row, "Study")) is not None
        ]
        self.store.write_studies(models)
        print(f"[Pipeline] Studies : {len(models):>6,} written")
        return models

    # ------------------------------------------------------------------
    # Phase 2: Patients (from CSV)
    # ------------------------------------------------------------------
    def run_patients(self) -> list[Patient]:
        conn = CSVConnector(PATIENTS_CSV)
        df = conn.extract()
        models = [
            m for row in df.to_dict(orient="records")
            if (m := self._safe_transform(transform_patient, row, "Patient")) is not None
        ]
        self.store.write_patients(models)
        print(f"[Pipeline] Patients: {len(models):>6,} written")
        return models

    # ------------------------------------------------------------------
    # Phase 3: Samples + Assays + Files (from vendor JSON)
    # ------------------------------------------------------------------
    def run_samples_assays_files(self) -> tuple[list[Sample], list[Assay], list[File]]:
        conn = JSONConnector(VENDOR_JSON)
        data = conn.extract()
        samples, assays, files = [], [], []

        for s_raw in data.get("samples", []):
            assay_list = s_raw.pop("assays", [])
            s = self._safe_transform(transform_sample, s_raw, "Sample")
            if s is None:
                continue
            samples.append(s)

            for a_raw in assay_list:
                file_list = a_raw.pop("files", [])
                a_raw["sample_id"] = s.sample_id
                a = self._safe_transform(transform_assay, a_raw, "Assay")
                if a is None:
                    continue
                assays.append(a)

                for f_raw in file_list:
                    f_raw["assay_id"] = a.assay_id
                    f = self._safe_transform(transform_file, f_raw, "File")
                    if f is not None:
                        files.append(f)

        self.store.write_samples(samples)
        self.store.write_assays(assays)
        self.store.write_files(files)
        print(f"[Pipeline] Samples : {len(samples):>6,} written")
        print(f"[Pipeline] Assays  : {len(assays):>6,} written")
        print(f"[Pipeline] Files   : {len(files):>6,} written")
        return samples, assays, files

    # ------------------------------------------------------------------
    # Phase 4: Biomarkers (from mock PDF text)
    # ------------------------------------------------------------------
    def run_biomarkers(self) -> list[Biomarker]:
        conn = PDFConnector(BIOMARKER_TXT)
        raw_rows = conn.extract()
        models = [
            m for row in raw_rows
            if (m := self._safe_transform(transform_biomarker, row, "Biomarker")) is not None
        ]
        self.store.write_biomarkers(models)
        print(f"[Pipeline] Biomarkers: {len(models):>5,} written")
        return models

    # ------------------------------------------------------------------
    def run_all(self) -> None:
        print("\n=== ETL Pipeline Starting ===")
        self.run_studies()
        self.run_patients()
        self.run_samples_assays_files()
        self.run_biomarkers()
        print(f"\n=== Pipeline Complete | Errors collected: {len(self.errors)} ===")
        if self.errors:
            for err in self.errors[:10]:
                print(f"  [{err['entity']}] {err['error']}")
