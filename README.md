# Clinical Data Hub

Multi-source clinical data integration pipeline that ingests heterogeneous data (CSV, Google Sheets, vendor JSON, PDF extracts), validates and transforms it through a unified Pydantic schema, and loads it into a SQLite database with an interactive Streamlit dashboard.

## Architecture

```
CSV / Sheets / JSON / PDF
        |
    Connectors (src/connectors/)
        |
    Transformers + Pydantic Validation (src/integration/)
        |
    SQLite Storage (src/storage/)
        |
    Streamlit Dashboard (src/dashboard/)
```

**Schema:** Studies -> Patients -> Samples -> Assays -> Files / Biomarkers

## Quick Start

```bash
pip install -r requirements.txt
python main.py            # generate data + ETL + launch dashboard
```

## Usage

```bash
python main.py                     # full pipeline + dashboard
python main.py --etl-only          # ETL + diagram, no browser launch
python main.py --skip-generate --etl-only  # re-run ETL with existing data
python main.py --diagram-only      # regenerate Draw.io XML only
streamlit run src/dashboard/app.py # launch dashboard independently
pytest tests/                      # run tests
```

## NL-to-SQL Chat

The dashboard includes a **Chat** tab where you can ask natural language questions about the data. It converts your question to SQL (shown for transparency), executes it, and returns results.

- Supports **Google Gemini** and **Anthropic Claude**
- Set `GOOGLE_API_KEY` in `.env` (see `.env.example`) or paste a key in the UI
- Only SELECT queries are allowed (safety-validated before execution)

## Outputs

- `data/outputs/clinical_hub.db` -- SQLite database (6 tables)
- `data/outputs/data_flow_diagram.xml` -- Draw.io diagram (import via File > Import)
- Streamlit dashboard at `http://localhost:8501`
