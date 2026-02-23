"""
Generates all synthetic raw data files into data/raw/.
Run this ONCE before main.py (or let main.py call it automatically).

Outputs:
  data/raw/study_metadata.csv      - 5 studies (mock Google Sheets)
  data/raw/patients.csv            - 150 patients
  data/raw/vendor_data.json        - 450 samples / 675 assays / 2025 files (nested)
  data/raw/biomarker_report.txt    - 13,500 biomarker lines (mock PDF extract)
"""

import csv
import json
import random
import sys
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

# Make config importable from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import (
    RAW_DIR,
    N_STUDIES,
    N_PATIENTS,
    N_SAMPLES_PER_PATIENT,
    N_FILES_PER_ASSAY,
    N_BIOMARKERS_PER_ASSAY,
)

SEED = 42
random.seed(SEED)

# ---------------------------------------------------------------------------
# Gene pool for biomarker generation
# ---------------------------------------------------------------------------
GENE_POOL = [
    ("TP53",    "ENSG00000141510"),
    ("BRCA1",   "ENSG00000012048"),
    ("EGFR",    "ENSG00000146648"),
    ("KRAS",    "ENSG00000133703"),
    ("PIK3CA",  "ENSG00000121879"),
    ("PTEN",    "ENSG00000171862"),
    ("MYC",     "ENSG00000136997"),
    ("CDH1",    "ENSG00000039068"),
    ("VHL",     "ENSG00000134086"),
    ("RB1",     "ENSG00000139687"),
    ("APC",     "ENSG00000134982"),
    ("NOTCH1",  "ENSG00000148400"),
    ("ERBB2",   "ENSG00000141736"),
    ("BRAF",    "ENSG00000157764"),
    ("ALK",     "ENSG00000171094"),
    ("RET",     "ENSG00000165731"),
    ("FGFR1",   "ENSG00000077782"),
    ("IDH1",    "ENSG00000138413"),
    ("NPM1",    "ENSG00000181163"),
    ("FLT3",    "ENSG00000122025"),
]

THERAPEUTIC_AREAS = ["Oncology", "Neurology", "Cardiology", "Immunology", "Rare Disease"]
PHASES = ["Phase I", "Phase II", "Phase III", "Phase IV"]
PHASE_WEIGHTS = [0.15, 0.35, 0.40, 0.10]
STUDY_STATUSES = ["Planning", "Recruiting", "Active", "Completed", "Terminated"]
SPONSORS = [
    "Pharma Alpha Inc.", "BioGen Therapeutics", "MedResearch Corp.",
    "Novius Pharma", "ClinTech Solutions",
]
PI_FIRSTNAMES = ["James", "Sarah", "Michael", "Emily", "David", "Lisa", "Robert", "Jennifer"]
PI_LASTNAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]
SITES = [f"SITE-{i:03d}" for i in range(1, 16)]

RACES = ["White", "Black or African American", "Asian", "Hispanic or Latino",
         "American Indian or Alaska Native", "Native Hawaiian or Pacific Islander", "Other"]
ETHNICITIES = ["Hispanic or Latino", "Not Hispanic or Latino", "Unknown"]
DIAGNOSES = {
    "Oncology":    ["Non-small cell lung cancer", "Breast adenocarcinoma",
                    "Colorectal carcinoma", "Glioblastoma multiforme", "Melanoma"],
    "Neurology":   ["Parkinson's disease", "Multiple sclerosis", "ALS",
                    "Alzheimer's disease", "Epilepsy"],
    "Cardiology":  ["Heart failure", "Atrial fibrillation", "Hypertrophic cardiomyopathy",
                    "Coronary artery disease", "Dilated cardiomyopathy"],
    "Immunology":  ["Rheumatoid arthritis", "Systemic lupus erythematosus",
                    "Crohn's disease", "Ulcerative colitis", "Psoriasis"],
    "Rare Disease": ["Fabry disease", "Gaucher disease", "Hunter syndrome",
                     "Pompe disease", "Niemann-Pick disease"],
}
PATIENT_STATUSES = ["Active", "Withdrawn", "Completed", "Lost to Follow-up"]
PAT_STATUS_WEIGHTS = [0.60, 0.10, 0.25, 0.05]

SAMPLE_TYPES = ["Tumor", "Blood", "Plasma", "PBMC", "FFPE", "cfDNA"]
SAMPLE_WEIGHTS = [0.30, 0.25, 0.15, 0.10, 0.15, 0.05]
TISSUE_TYPES = {
    "Tumor": ["Lung adenocarcinoma", "Breast ductal carcinoma", "Colon adenocarcinoma",
              "Glioblastoma", "Melanoma"],
    "Blood": ["Peripheral blood", "Whole blood"],
    "Plasma": ["Liquid biopsy plasma", "Cell-free plasma"],
    "PBMC": ["Peripheral blood mononuclear cells"],
    "FFPE": ["FFPE tumor block", "FFPE normal adjacent"],
    "cfDNA": ["Circulating tumor DNA", "Cell-free DNA"],
}

ASSAY_TYPES = ["RNA-seq", "WGS", "WES"]
ASSAY_WEIGHTS = [0.50, 0.25, 0.25]
VENDORS = [
    "Illumina Clinical Services", "Novogene", "BGI Genomics", "GeneDx"
]
PLATFORMS = {
    "RNA-seq": ["Illumina NovaSeq 6000", "Illumina NextSeq 2000"],
    "WGS":     ["Illumina NovaSeq X Plus", "PacBio Revio"],
    "WES":     ["Illumina NovaSeq 6000", "Illumina HiSeq 4000"],
}
FILE_TYPES = {
    "RNA-seq": ["FASTQ", "FASTQ", "counts"],
    "WGS":     ["FASTQ", "BAM", "VCF"],
    "WES":     ["FASTQ", "BAM", "VCF"],
}
ASSAY_STATUSES_PASS  = ["Passed"]
ASSAY_STATUSES_FAIL  = ["Failed"]
ASSAY_STATUSES_OTHER = ["Pending", "Running", "QC Review"]
VARIANT_TYPES = ["SNV", "INDEL", "CNV", "Fusion"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def rand_date(start: date, end: date) -> str:
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, delta))).isoformat()


def rand_datetime(start: date, end: date) -> str:
    d = date.fromisoformat(rand_date(start, end))
    hour = random.randint(8, 20)
    minute = random.randint(0, 59)
    return f"{d}T{hour:02d}:{minute:02d}:00"


def fake_md5() -> str:
    return "".join(random.choices("0123456789abcdef", k=32))


def fake_pi() -> str:
    return f"Dr. {random.choice(PI_FIRSTNAMES)} {random.choice(PI_LASTNAMES)}"


def fake_run_id(run_date_str: str, counter: int) -> str:
    d = run_date_str.replace("-", "")
    return f"RUN-{d}-{counter:03d}"


def fake_storage() -> str:
    freezer = random.choice(["A", "B", "C"])
    row = random.randint(1, 10)
    box = random.randint(1, 20)
    return f"Freezer-{freezer}-Row{row}-Box{box:02d}"


# ---------------------------------------------------------------------------
# Step 1: Studies
# ---------------------------------------------------------------------------

def generate_studies() -> list[dict]:
    studies = []
    for i in range(N_STUDIES):
        area = THERAPEUTIC_AREAS[i % len(THERAPEUTIC_AREAS)]
        phase = random.choices(PHASES, weights=PHASE_WEIGHTS)[0]
        phase_num = PHASES.index(phase) + 1
        protocol_year = random.randint(2020, 2023)
        protocol_seq = random.randint(1, 999)
        studies.append({
            "study_id":        f"STU-{1000 + i}",
            "name":            f"{area} {phase} Study {i + 1:02d}",
            "protocol_number": f"PROT-{protocol_year}-{protocol_seq:03d}",
            "phase":           phase,
            "therapeutic_area": area,
            "start_date":      rand_date(date(2020, 1, 1), date(2023, 6, 1)),
            "status":          random.choice(STUDY_STATUSES),
            "pi_name":         fake_pi(),
            "sponsor":         SPONSORS[i % len(SPONSORS)],
        })
    return studies


# ---------------------------------------------------------------------------
# Step 2: Patients
# ---------------------------------------------------------------------------

def generate_patients(studies: list[dict]) -> list[dict]:
    patients = []
    n_per_study = N_PATIENTS // N_STUDIES
    for study in studies:
        area = study["therapeutic_area"]
        diag_pool = DIAGNOSES[area]
        for j in range(n_per_study):
            pat_id = str(uuid.uuid4())
            enroll_date = rand_date(date(2021, 1, 1), date(2024, 6, 1))
            patients.append({
                "patient_id":       pat_id,
                "study_id":         study["study_id"],
                "age":              random.randint(18, 80),
                "sex":              random.choices(["Male", "Female", "Other", "Unknown"],
                                                   weights=[0.49, 0.49, 0.01, 0.01])[0],
                "race":             random.choice(RACES),
                "ethnicity":        random.choices(ETHNICITIES,
                                                   weights=[0.18, 0.72, 0.10])[0],
                "primary_diagnosis": random.choice(diag_pool),
                "enrollment_date":  enroll_date,
                "site_id":          random.choice(SITES),
                "status":           random.choices(PATIENT_STATUSES,
                                                   weights=PAT_STATUS_WEIGHTS)[0],
            })
    return patients


# ---------------------------------------------------------------------------
# Step 3: Vendor JSON (samples / assays / files)
# ---------------------------------------------------------------------------

def generate_vendor_json(patients: list[dict]) -> dict:
    all_samples = []
    run_counter = 1
    rna_seq_assay_ids = []  # collect for biomarker report

    for patient in patients:
        for _ in range(N_SAMPLES_PER_PATIENT):
            sample_type = random.choices(SAMPLE_TYPES, weights=SAMPLE_WEIGHTS)[0]
            tissue = random.choice(TISSUE_TYPES[sample_type])
            collect_date = rand_date(date(2021, 6, 1), date(2024, 3, 1))

            sample_id = str(uuid.uuid4())
            n_assays = random.choices([1, 2], weights=[0.67, 0.33])[0]
            assays = []

            for _ in range(n_assays):
                assay_type = random.choices(ASSAY_TYPES, weights=ASSAY_WEIGHTS)[0]
                vendor = random.choice(VENDORS)
                platform = random.choice(PLATFORMS[assay_type])
                run_date = rand_date(date(2021, 8, 1), date(2024, 6, 1))
                run_id = fake_run_id(run_date, run_counter)
                run_counter += 1

                # ~85% pass QC
                qc_pass = random.random() < 0.85
                if qc_pass:
                    status = "Passed"
                elif random.random() < 0.5:
                    status = "Failed"
                else:
                    status = random.choice(["Pending", "Running", "QC Review"])
                    qc_pass = False

                assay_id = str(uuid.uuid4())
                if assay_type == "RNA-seq" and qc_pass:
                    rna_seq_assay_ids.append(assay_id)

                files = []
                file_types = FILE_TYPES[assay_type]
                upload_date = rand_datetime(date(2021, 8, 1), date(2024, 6, 30))
                for k, ft in enumerate(file_types[:N_FILES_PER_ASSAY]):
                    suffix_map = {
                        "FASTQ": f"_R{k+1}.fastq.gz" if k < 2 else ".fastq.gz",
                        "BAM": ".bam",
                        "VCF": ".vcf.gz",
                        "counts": "_counts.tsv",
                    }
                    fname = f"{run_id}_{sample_id[:8]}{suffix_map.get(ft, '.dat')}"
                    size_map = {"FASTQ": (1000, 8000), "BAM": (5000, 30000),
                                "VCF": (10, 500), "counts": (1, 50)}
                    lo, hi = size_map.get(ft, (10, 100))
                    files.append({
                        "file_id":      str(uuid.uuid4()),
                        "file_name":    fname,
                        "file_type":    ft,
                        "file_path":    f"/data/genomics/runs/{run_id}/{fname}",
                        "file_size_mb": round(random.uniform(lo, hi), 2),
                        "checksum":     fake_md5(),
                        "upload_date":  upload_date,
                    })

                assays.append({
                    "assay_id":   assay_id,
                    "assay_type": assay_type,
                    "vendor":     vendor,
                    "platform":   platform,
                    "run_date":   run_date,
                    "run_id":     run_id,
                    "status":     status,
                    "qc_pass":    qc_pass,
                    "files":      files,
                })

            all_samples.append({
                "sample_id":       sample_id,
                "patient_id":      patient["patient_id"],
                "sample_type":     sample_type,
                "collection_date": collect_date,
                "tissue_type":     tissue,
                "quality_score":   round(random.uniform(3.0, 10.0), 2),
                "volume_ml":       round(random.uniform(0.2, 5.0), 2),
                "storage_location": fake_storage(),
                "assays":          assays,
            })

    return {"vendor": "Multi-Vendor Export", "export_date": str(date.today()),
            "samples": all_samples}, rna_seq_assay_ids


# ---------------------------------------------------------------------------
# Step 4: Biomarker report text (mock PDF)
# ---------------------------------------------------------------------------

def generate_biomarker_txt(rna_seq_assay_ids: list[str]) -> str:
    lines = [
        "BIOMARKER ANALYSIS REPORT",
        "=" * 60,
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Total assays: {len(rna_seq_assay_ids)}",
        "",
        "DIFFERENTIALLY EXPRESSED GENES & VARIANTS",
        "-" * 60,
        "",
    ]

    for assay_id in rna_seq_assay_ids:
        for _ in range(N_BIOMARKERS_PER_ASSAY):
            gene, ensembl = random.choice(GENE_POOL)
            # 70% RNA expression, 30% variant
            if random.random() < 0.70:
                log2fc = round(random.uniform(-5.0, 5.0), 4)
                pvalue = round(random.uniform(0.0001, 0.9999), 6)
                padj = min(round(pvalue * random.uniform(1.0, 20.0), 6), 1.0)
                tpm = round(random.uniform(0.1, 5000.0), 2)
                vtype = "None"
            else:
                log2fc = "None"
                pvalue = "None"
                padj = "None"
                tpm = "None"
                vtype = random.choice(VARIANT_TYPES)

            lines.append(
                f"assay_id={assay_id} gene={gene} ensembl={ensembl} "
                f"log2fc={log2fc} pvalue={pvalue} padj={padj} tpm={tpm} variant_type={vtype}"
            )

    lines.append("")
    lines.append("END OF REPORT")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print("Generating synthetic clinical data...")

    # Studies
    studies = generate_studies()
    study_csv_path = RAW_DIR / "study_metadata.csv"
    with study_csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=studies[0].keys())
        writer.writeheader()
        writer.writerows(studies)
    print(f"  [OK] {study_csv_path.name}: {len(studies)} studies")

    # Patients
    patients = generate_patients(studies)
    patient_csv_path = RAW_DIR / "patients.csv"
    with patient_csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=patients[0].keys())
        writer.writeheader()
        writer.writerows(patients)
    print(f"  [OK] {patient_csv_path.name}: {len(patients)} patients")

    # Vendor JSON
    vendor_data, rna_seq_assay_ids = generate_vendor_json(patients)
    n_samples = len(vendor_data["samples"])
    n_assays = sum(len(s["assays"]) for s in vendor_data["samples"])
    n_files = sum(
        len(a["files"]) for s in vendor_data["samples"] for a in s["assays"]
    )
    vendor_json_path = RAW_DIR / "vendor_data.json"
    with vendor_json_path.open("w", encoding="utf-8") as f:
        json.dump(vendor_data, f, indent=2)
    print(f"  [OK] {vendor_json_path.name}: {n_samples} samples, "
          f"{n_assays} assays, {n_files} files")

    # Biomarker report
    biomarker_txt = generate_biomarker_txt(rna_seq_assay_ids)
    bm_path = RAW_DIR / "biomarker_report.txt"
    bm_path.write_text(biomarker_txt, encoding="utf-8")
    n_bm_lines = biomarker_txt.count("assay_id=")
    print(f"  [OK] {bm_path.name}: {n_bm_lines} biomarker lines "
          f"({len(rna_seq_assay_ids)} RNA-seq assays)")

    print("\nSynthetic data generation complete.")


if __name__ == "__main__":
    main()
