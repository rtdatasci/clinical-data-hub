# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Full pipeline: generate synthetic data + ETL + Draw.io diagram + launch dashboard
python main.py

# ETL + diagram only (no dashboard browser launch)
python main.py --etl-only

# Re-run ETL using existing raw files (skip regeneration)
python main.py --skip-generate --etl-only

# Regenerate Draw.io XML only
python main.py --diagram-only

# Launch dashboard independently (DB must already exist)
streamlit run src/dashboard/app.py

# Run tests
pytest tests/
```

**Important:** `main.py --etl-only` drops and recreates `data/outputs/clinical_hub.db` on every run. To preserve the DB, launch the dashboard directly with `streamlit run`.

## Architecture

This project is a **multi-source clinical data integration hub** with four distinct layers:

### 1. Source Data (`data/raw/`)
Four synthetic source files simulate real-world heterogeneous inputs:
- `patients.csv` — demographics (CSV connector)
- `study_metadata.csv` — study registry, treated as mock Google Sheets (Sheets connector)
- `vendor_data.json` — nested JSON: `samples → assays → files` per patient (JSON connector)
- `biomarker_report.txt` — structured text mimicking PDF extraction; one line per biomarker record (PDF connector)

Data volumes and file paths are centralised in `config/settings.py` — change them there, not in individual scripts.

### 2. Connectors (`src/connectors/`)
All connectors extend `BaseConnector` (validates source path, enforces `extract()` contract). Return types differ by connector: `DataFrame` for CSV/Sheets, `dict` for JSON, `list[dict]` for PDF. The integration layer handles normalisation, keeping connectors thin.

### 3. Integration (`src/integration/`)
- **`transformers.py`** — one function per entity (`transform_study`, `transform_patient`, etc.); each takes a plain `dict` and returns a validated Pydantic model.
- **`pipeline.py`** — `ETLPipeline` orchestrates in FK dependency order: Studies → Patients → Samples/Assays/Files → Biomarkers. `_safe_transform()` catches `ValidationError` and collects errors without halting. The JSON connector's nested structure (`samples[].assays[].files[]`) is flattened inside `run_samples_assays_files()` by popping nested lists before passing dicts to transformers.

### 4. Unified Schema (`src/schema/models.py`)
Six **Pydantic v2** models define the canonical entities:

```
Study --1:N--> Patient --1:N--> Sample --1:N--> Assay --1:N--> File
                                                        \--1:N--> Biomarker
```

Key model constraints:
- `Literal` types enforce all categoricals at parse time
- `Assay` has a cross-field `model_validator`: `status="Passed"` requires `qc_pass=True` and vice versa
- `Biomarker` fields (`log2_fold_change`, `pvalue`, `padj`, `expression_tpm`) are `Optional` — RNA-seq records carry expression data; WGS/WES records carry `variant_type`

### 5. Storage (`src/storage/sqlite_store.py`)
No ORM. Strategy: `model.model_dump()` → serialize `date`/`datetime` to ISO strings, `bool` to `int` → `pd.DataFrame.to_sql(if_exists="append")`. DDL is defined in creation order to satisfy FK constraints. `PRAGMA foreign_keys = ON` is set per connection. The public `query(sql)` method is used by the dashboard.

### 6. Outputs
- **`data/outputs/clinical_hub.db`** — SQLite database (6 tables)
- **`data/outputs/data_flow_diagram.xml`** — mxGraph XML for [app.diagrams.net](https://app.diagrams.net); import via *File > Import from > Device*. Shows 4 colour-coded swim lanes: Source Data (blue), ETL Layer (green), Unified Schema (orange), Output Layer (purple).
- **Streamlit dashboard** (`src/dashboard/app.py`) — 5 tabs: Overview, Patients, Samples, Assays, Biomarkers. Uses `@st.cache_resource` for the DB connection and `@st.cache_data(ttl=300)` for query results.

## Platform Notes (Windows)

- All `print()` calls must use ASCII only — the Windows cp1252 console cannot encode characters like `→`. Use `->` instead.
- Run scripts from the project root so that `config.settings` and `src.*` imports resolve correctly (both use `sys.path.insert(0, PROJECT_ROOT)`).
- `scripts/generate_synthetic_data.py` uses `random.seed(42)` for reproducibility.

## Adding a New Data Source

1. Create a connector in `src/connectors/` extending `BaseConnector`; implement `extract()` returning raw data.
2. Add a transformer function in `src/integration/transformers.py` mapping raw dict → Pydantic model.
3. Add a new phase method in `ETLPipeline` following the existing pattern, and call it from `run_all()` after its FK dependencies.
4. Add DDL to `src/storage/sqlite_store.py` `DDL` list (in dependency order) and a corresponding `write_*` method.
5. Add the new source node and edges to `NODES` / `EDGES` in `src/visualization/drawio_generator.py`.
