"""
NL-to-SQL engine — converts natural language questions to SQLite queries
using Google Gemini or Anthropic Claude APIs.
"""

from __future__ import annotations

import re

SCHEMA_PROMPT = """\
You are a SQL assistant for a clinical data hub stored in SQLite.

## Database Schema

```sql
CREATE TABLE studies (
    study_id         TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    protocol_number  TEXT UNIQUE NOT NULL,
    phase            TEXT NOT NULL,       -- 'Phase I','Phase II','Phase III','Phase IV'
    therapeutic_area TEXT NOT NULL,
    start_date       TEXT NOT NULL,       -- ISO date string
    status           TEXT NOT NULL,       -- 'Planning','Recruiting','Active','Completed','Terminated'
    pi_name          TEXT NOT NULL,
    sponsor          TEXT NOT NULL
);

CREATE TABLE patients (
    patient_id        TEXT PRIMARY KEY,
    study_id          TEXT NOT NULL REFERENCES studies(study_id),
    age               INTEGER NOT NULL,   -- 18-100
    sex               TEXT NOT NULL,       -- 'Male','Female','Other','Unknown'
    race              TEXT NOT NULL,
    ethnicity         TEXT NOT NULL,       -- 'Hispanic or Latino','Not Hispanic or Latino','Unknown'
    primary_diagnosis TEXT NOT NULL,
    enrollment_date   TEXT NOT NULL,       -- ISO date string
    site_id           TEXT NOT NULL,
    status            TEXT NOT NULL        -- 'Active','Withdrawn','Completed','Lost to Follow-up'
);

CREATE TABLE samples (
    sample_id        TEXT PRIMARY KEY,
    patient_id       TEXT NOT NULL REFERENCES patients(patient_id),
    sample_type      TEXT NOT NULL,       -- 'Tumor','Blood','Plasma','PBMC','FFPE','cfDNA'
    collection_date  TEXT NOT NULL,       -- ISO date string
    tissue_type      TEXT NOT NULL,
    quality_score    REAL NOT NULL,       -- 0.0-10.0
    volume_ml        REAL NOT NULL,
    storage_location TEXT NOT NULL
);

CREATE TABLE assays (
    assay_id    TEXT PRIMARY KEY,
    sample_id   TEXT NOT NULL REFERENCES samples(sample_id),
    assay_type  TEXT NOT NULL,            -- 'RNA-seq','WGS','WES'
    vendor      TEXT NOT NULL,
    platform    TEXT NOT NULL,
    run_date    TEXT NOT NULL,            -- ISO date string
    run_id      TEXT UNIQUE NOT NULL,
    status      TEXT NOT NULL,            -- 'Pending','Running','QC Review','Passed','Failed'
    qc_pass     INTEGER NOT NULL          -- 0=Fail, 1=Pass
);

CREATE TABLE files (
    file_id      TEXT PRIMARY KEY,
    assay_id     TEXT NOT NULL REFERENCES assays(assay_id),
    file_name    TEXT NOT NULL,
    file_type    TEXT NOT NULL,            -- 'FASTQ','BAM','VCF','counts'
    file_path    TEXT NOT NULL,
    file_size_mb REAL NOT NULL,
    checksum     TEXT NOT NULL,
    upload_date  TEXT NOT NULL             -- ISO datetime string
);

CREATE TABLE biomarkers (
    biomarker_id     TEXT PRIMARY KEY,
    assay_id         TEXT NOT NULL REFERENCES assays(assay_id),
    gene_symbol      TEXT NOT NULL,
    ensembl_id       TEXT NOT NULL,
    log2_fold_change REAL,                -- NULL for WGS/WES records
    pvalue           REAL,                -- 0.0-1.0, NULL for WGS/WES
    padj             REAL,                -- adjusted p-value, NULL for WGS/WES
    expression_tpm   REAL,                -- NULL for WGS/WES
    variant_type     TEXT                  -- 'SNV','INDEL','CNV','Fusion'; NULL for RNA-seq
);
```

## Relationships
studies 1:N patients 1:N samples 1:N assays 1:N files
                                         assays 1:N biomarkers

## Rules
1. Return ONLY a single valid SQLite SELECT statement. No INSERT, UPDATE, DELETE, DROP, or ALTER.
2. Always add LIMIT 1000 unless the user explicitly asks for all rows.
3. SQLite has NO math functions (LOG, EXP, SQRT, etc.). Return raw numeric columns and note any needed transformations.
4. Boolean column `qc_pass` is stored as INTEGER (0=Fail, 1=Pass).
5. All dates are ISO strings (YYYY-MM-DD). Use string comparison or SUBSTR for date filtering.
6. Use table aliases for readability.
7. Return ONLY the SQL query, no explanation or markdown fencing.
"""


def _strip_fences(text: str) -> str:
    """Remove markdown code fences from LLM output."""
    text = re.sub(r"^```(?:sql)?\s*", "", text.strip())
    text = re.sub(r"\s*```$", "", text)
    return text.strip()


def nl_to_sql_gemini(question: str, api_key: str) -> str:
    """Convert a natural language question to SQL using Google Gemini."""
    from google import genai

    client = genai.Client(api_key=api_key)
    response = client.models.generate_content(
        model="gemini-2.0-flash",
        contents=question,
        config=genai.types.GenerateContentConfig(
            system_instruction=SCHEMA_PROMPT,
            max_output_tokens=1024,
        ),
    )
    return _strip_fences(response.text)


def nl_to_sql_anthropic(question: str, api_key: str) -> str:
    """Convert a natural language question to SQL using Anthropic Claude."""
    import anthropic

    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        system=SCHEMA_PROMPT,
        messages=[{"role": "user", "content": question}],
    )
    return _strip_fences(response.content[0].text)


def nl_to_sql(question: str, api_key: str, provider: str = "Google Gemini") -> str:
    """Route to the appropriate LLM provider."""
    if provider == "Anthropic Claude":
        return nl_to_sql_anthropic(question, api_key)
    return nl_to_sql_gemini(question, api_key)


def validate_sql(sql: str) -> str | None:
    """Return an error message if the SQL is not a safe SELECT, else None."""
    normalized = sql.strip().upper()
    if not normalized.startswith("SELECT"):
        return "Only SELECT queries are allowed."
    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "REPLACE", "ATTACH"]
    for keyword in forbidden:
        if re.search(rf"\b{keyword}\b", normalized):
            return f"Query contains forbidden keyword: {keyword}"
    return None
