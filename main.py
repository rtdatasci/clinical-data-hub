"""
Clinical Data Hub — Main Entry Point

Run order:
  Step 0  Ensure data directories exist
  Step 1  Generate synthetic raw data  (scripts/generate_synthetic_data.py)
  Step 2  Run ETL pipeline             (src/integration/pipeline.py)
  Step 3  Generate Draw.io diagram     (src/visualization/drawio_generator.py)
  Step 4  Launch Streamlit dashboard   (src/dashboard/app.py)

Usage examples:
  python main.py                    # Full run: generate + ETL + diagram + dashboard
  python main.py --skip-generate    # Skip data generation (use existing raw files)
  python main.py --etl-only         # ETL + diagram only, skip dashboard launch
  python main.py --diagram-only     # Regenerate the Draw.io XML only
"""

import argparse
import logging
import subprocess
import sys
from pathlib import Path

# Ensure project root is on the Python path so all src.* imports resolve
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import DB_PATH, DRAWIO_PATH, OUTPUT_DIR, PROCESSED_DIR, RAW_DIR
from src.integration.pipeline import ETLPipeline
from src.visualization.drawio_generator import generate_drawio_xml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("main")


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------

def ensure_directories() -> None:
    for d in [RAW_DIR, PROCESSED_DIR, OUTPUT_DIR]:
        d.mkdir(parents=True, exist_ok=True)
    log.info("Directory structure verified.")


def step_generate() -> None:
    log.info("=== Step 1: Generating synthetic raw data ===")
    result = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "scripts" / "generate_synthetic_data.py")],
        check=False,
    )
    if result.returncode != 0:
        log.error("Synthetic data generation failed — aborting.")
        sys.exit(1)


def step_etl() -> None:
    log.info("=== Step 2: Running ETL pipeline ===")
    # Fresh run: remove existing DB so we start clean
    if DB_PATH.exists():
        DB_PATH.unlink()
        log.info(f"Removed existing DB: {DB_PATH.name}")
    pipeline = ETLPipeline()
    pipeline.run_all()
    counts = pipeline.store.table_counts()
    log.info("Table row counts after ETL:")
    for table, n in counts.items():
        log.info(f"  {table:<15} {n:>8,}")


def step_diagram() -> None:
    log.info("=== Step 3: Generating Draw.io diagram ===")
    generate_drawio_xml(DRAWIO_PATH)


def step_dashboard() -> None:
    log.info("=== Step 4: Launching Streamlit dashboard ===")
    log.info("Dashboard URL: http://localhost:8501")
    log.info("Press Ctrl+C to stop.")
    subprocess.run([
        sys.executable, "-m", "streamlit", "run",
        str(PROJECT_ROOT / "src" / "dashboard" / "app.py"),
        "--server.port", "8501",
        "--server.headless", "false",
    ])


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clinical Data Hub — orchestrates data generation, ETL, "
                    "diagram generation, and dashboard launch.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--skip-generate", action="store_true",
        help="Skip synthetic data generation (use existing raw files).",
    )
    parser.add_argument(
        "--etl-only", action="store_true",
        help="Run ETL and diagram generation only; do not launch dashboard.",
    )
    parser.add_argument(
        "--diagram-only", action="store_true",
        help="Regenerate the Draw.io XML only (skip ETL and dashboard).",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    ensure_directories()

    if args.diagram_only:
        step_diagram()
        return

    if not args.skip_generate:
        step_generate()

    step_etl()
    step_diagram()

    if not args.etl_only:
        step_dashboard()


if __name__ == "__main__":
    main()
