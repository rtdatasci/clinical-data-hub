from pathlib import Path

# Root paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
OUTPUT_DIR = DATA_DIR / "outputs"

# Source file paths
PATIENTS_CSV = RAW_DIR / "patients.csv"
STUDY_SHEETS_CSV = RAW_DIR / "study_metadata.csv"
VENDOR_JSON = RAW_DIR / "vendor_data.json"
BIOMARKER_TXT = RAW_DIR / "biomarker_report.txt"

# Output paths
DB_PATH = OUTPUT_DIR / "clinical_hub.db"
DRAWIO_PATH = OUTPUT_DIR / "data_flow_diagram.xml"

# Synthetic data volumes
N_STUDIES = 5
N_PATIENTS = 150        # 30 per study
N_SAMPLES_PER_PATIENT = 3
N_ASSAYS_PER_SAMPLE = 1   # some samples get 2 assays via random selection
N_FILES_PER_ASSAY = 3
N_BIOMARKERS_PER_ASSAY = 20

# Streamlit dashboard
DASHBOARD_TITLE = "Clinical Data Hub"
