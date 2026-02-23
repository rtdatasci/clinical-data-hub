from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


def _new_id() -> str:
    return str(uuid.uuid4())


class Study(BaseModel):
    study_id: str = Field(default_factory=_new_id)
    name: str = Field(..., min_length=2, max_length=200)
    protocol_number: str
    phase: Literal["Phase I", "Phase II", "Phase III", "Phase IV"]
    therapeutic_area: str
    start_date: date
    status: Literal["Planning", "Recruiting", "Active", "Completed", "Terminated"]
    pi_name: str
    sponsor: str

    model_config = {"str_strip_whitespace": True}


class Patient(BaseModel):
    patient_id: str = Field(default_factory=_new_id)
    study_id: str
    age: int = Field(..., ge=18, le=100)
    sex: Literal["Male", "Female", "Other", "Unknown"]
    race: str
    ethnicity: Literal["Hispanic or Latino", "Not Hispanic or Latino", "Unknown"]
    primary_diagnosis: str
    enrollment_date: date
    site_id: str
    status: Literal["Active", "Withdrawn", "Completed", "Lost to Follow-up"]

    model_config = {"str_strip_whitespace": True}


class Sample(BaseModel):
    sample_id: str = Field(default_factory=_new_id)
    patient_id: str
    sample_type: Literal["Tumor", "Blood", "Plasma", "PBMC", "FFPE", "cfDNA"]
    collection_date: date
    tissue_type: str
    quality_score: float = Field(..., ge=0.0, le=10.0)
    volume_ml: float = Field(..., ge=0.0)
    storage_location: str

    @field_validator("quality_score")
    @classmethod
    def round_quality(cls, v: float) -> float:
        return round(v, 2)

    model_config = {"str_strip_whitespace": True}


class Assay(BaseModel):
    assay_id: str = Field(default_factory=_new_id)
    sample_id: str
    assay_type: Literal["RNA-seq", "WGS", "WES"]
    vendor: str
    platform: str
    run_date: date
    run_id: str
    status: Literal["Pending", "Running", "QC Review", "Passed", "Failed"]
    qc_pass: bool

    @model_validator(mode="after")
    def qc_pass_must_match_status(self) -> "Assay":
        if self.status == "Passed" and not self.qc_pass:
            raise ValueError("status='Passed' but qc_pass=False")
        if self.status == "Failed" and self.qc_pass:
            raise ValueError("status='Failed' but qc_pass=True")
        return self

    model_config = {"str_strip_whitespace": True}


class File(BaseModel):
    file_id: str = Field(default_factory=_new_id)
    assay_id: str
    file_name: str
    file_type: Literal["FASTQ", "BAM", "VCF", "counts"]
    file_path: str
    file_size_mb: float = Field(..., ge=0.0)
    checksum: str
    upload_date: datetime

    @field_validator("checksum")
    @classmethod
    def validate_checksum(cls, v: str) -> str:
        v = v.lower()
        if len(v) != 32 or not all(c in "0123456789abcdef" for c in v):
            raise ValueError(f"checksum must be 32-char hex, got: {v!r}")
        return v

    model_config = {"str_strip_whitespace": True}


class Biomarker(BaseModel):
    biomarker_id: str = Field(default_factory=_new_id)
    assay_id: str
    gene_symbol: str = Field(..., max_length=20)
    ensembl_id: str
    log2_fold_change: Optional[float] = None
    pvalue: Optional[float] = Field(None, ge=0.0, le=1.0)
    padj: Optional[float] = Field(None, ge=0.0, le=1.0)
    expression_tpm: Optional[float] = Field(None, ge=0.0)
    variant_type: Optional[Literal["SNV", "INDEL", "CNV", "Fusion"]] = None

    model_config = {"str_strip_whitespace": True}
