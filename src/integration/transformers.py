"""
One transformer function per entity.
Each takes a raw dict and returns a validated Pydantic model instance.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any

from src.schema.models import Assay, Biomarker, File, Patient, Sample, Study


def _new_id() -> str:
    return str(uuid.uuid4())


def _to_date(val: Any) -> date:
    if isinstance(val, date):
        return val
    return date.fromisoformat(str(val))


def _to_datetime(val: Any) -> datetime:
    if isinstance(val, datetime):
        return val
    return datetime.fromisoformat(str(val))


def transform_study(raw: dict) -> Study:
    return Study(
        study_id=raw.get("study_id") or _new_id(),
        name=raw["name"],
        protocol_number=raw["protocol_number"],
        phase=raw["phase"],
        therapeutic_area=raw["therapeutic_area"],
        start_date=_to_date(raw["start_date"]),
        status=raw["status"],
        pi_name=raw["pi_name"],
        sponsor=raw["sponsor"],
    )


def transform_patient(raw: dict) -> Patient:
    return Patient(
        patient_id=raw.get("patient_id") or _new_id(),
        study_id=raw["study_id"],
        age=int(raw["age"]),
        sex=raw["sex"],
        race=raw["race"],
        ethnicity=raw["ethnicity"],
        primary_diagnosis=raw["primary_diagnosis"],
        enrollment_date=_to_date(raw["enrollment_date"]),
        site_id=raw["site_id"],
        status=raw["status"],
    )


def transform_sample(raw: dict) -> Sample:
    return Sample(
        sample_id=raw.get("sample_id") or _new_id(),
        patient_id=raw["patient_id"],
        sample_type=raw["sample_type"],
        collection_date=_to_date(raw["collection_date"]),
        tissue_type=raw["tissue_type"],
        quality_score=float(raw["quality_score"]),
        volume_ml=float(raw["volume_ml"]),
        storage_location=raw["storage_location"],
    )


def transform_assay(raw: dict) -> Assay:
    return Assay(
        assay_id=raw.get("assay_id") or _new_id(),
        sample_id=raw["sample_id"],
        assay_type=raw["assay_type"],
        vendor=raw["vendor"],
        platform=raw["platform"],
        run_date=_to_date(raw["run_date"]),
        run_id=raw["run_id"],
        status=raw["status"],
        qc_pass=bool(raw["qc_pass"]),
    )


def transform_file(raw: dict) -> File:
    return File(
        file_id=raw.get("file_id") or _new_id(),
        assay_id=raw["assay_id"],
        file_name=raw["file_name"],
        file_type=raw["file_type"],
        file_path=raw["file_path"],
        file_size_mb=float(raw["file_size_mb"]),
        checksum=raw["checksum"],
        upload_date=_to_datetime(raw["upload_date"]),
    )


def transform_biomarker(raw: dict) -> Biomarker:
    return Biomarker(
        biomarker_id=raw.get("biomarker_id") or _new_id(),
        assay_id=raw["assay_id"],
        gene_symbol=raw["gene_symbol"],
        ensembl_id=raw["ensembl_id"],
        log2_fold_change=raw.get("log2_fold_change"),
        pvalue=raw.get("pvalue"),
        padj=raw.get("padj"),
        expression_tpm=raw.get("expression_tpm"),
        variant_type=raw.get("variant_type"),
    )
