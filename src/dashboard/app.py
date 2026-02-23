"""
Clinical Data Hub — Streamlit Dashboard
Run with:  streamlit run src/dashboard/app.py

Tabs:
  1. Overview    — KPI cards, study status/phase charts, study registry
  2. Patients    — Demographics (age, sex, race, enrollment status)
  3. Samples     — Sample type distribution, quality scores
  4. Assays      — QC pass rate, assay types, vendor breakdown, file types
  5. Biomarkers  — Top DEGs, variant types, volcano plot
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Allow running from any working directory
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import DASHBOARD_TITLE, DB_PATH

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title=DASHBOARD_TITLE,
    page_icon="🧬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

@st.cache_resource
def _get_conn():
    if not DB_PATH.exists():
        return None
    return sqlite3.connect(str(DB_PATH), check_same_thread=False)


@st.cache_data(ttl=300)
def q(sql: str) -> pd.DataFrame:
    conn = _get_conn()
    if conn is None:
        return pd.DataFrame()
    return pd.read_sql_query(sql, conn)


# ---------------------------------------------------------------------------
# Guard: DB must exist
# ---------------------------------------------------------------------------
if not DB_PATH.exists():
    st.error(
        f"Database not found at `{DB_PATH}`.\n\n"
        "Run `python main.py` first to generate data and populate the database."
    )
    st.stop()

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
st.sidebar.title("Clinical Data Hub")
st.sidebar.caption("Multi-source clinical & genomic data")
st.sidebar.markdown("---")

all_areas = q("SELECT DISTINCT therapeutic_area FROM studies ORDER BY therapeutic_area")[
    "therapeutic_area"
].tolist()
sel_areas = st.sidebar.multiselect("Therapeutic Area", all_areas, default=[])

all_phases = ["Phase I", "Phase II", "Phase III", "Phase IV"]
sel_phases = st.sidebar.multiselect("Phase", all_phases, default=[])

st.sidebar.markdown("---")
st.sidebar.markdown("**Data Sources**")
st.sidebar.markdown("- CSV → Patients\n- Google Sheets → Studies\n- JSON → Samples/Assays/Files\n- PDF → Biomarkers")

# Build a WHERE clause fragment for study filtering
def _study_where() -> str:
    parts = []
    if sel_areas:
        escaped = ", ".join(f"'{a}'" for a in sel_areas)
        parts.append(f"therapeutic_area IN ({escaped})")
    if sel_phases:
        escaped = ", ".join(f"'{p}'" for p in sel_phases)
        parts.append(f"phase IN ({escaped})")
    return ("WHERE " + " AND ".join(parts)) if parts else ""


study_filter_sql = _study_where()

# Filtered study IDs for downstream joins
def _filtered_study_ids() -> list[str]:
    df = q(f"SELECT study_id FROM studies {study_filter_sql}")
    return df["study_id"].tolist() if not df.empty else []


def _sid_in(ids: list[str]) -> str:
    """Build an IN(...) clause or always-true placeholder."""
    if not ids:
        return "1=1"
    escaped = ", ".join(f"'{i}'" for i in ids)
    return f"study_id IN ({escaped})"


# ---------------------------------------------------------------------------
# Page title
# ---------------------------------------------------------------------------
st.title("🧬 Clinical Data Hub")
st.caption("Unified view across CSV · Google Sheets · Vendor JSON · Biomarker PDF")

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab_ov, tab_pat, tab_samp, tab_assay, tab_bm = st.tabs(
    ["Overview", "Patients", "Samples", "Assays", "Biomarkers"]
)


# ============================================================
# TAB 1 — OVERVIEW
# ============================================================
with tab_ov:
    st.header("Study Portfolio Overview")

    # KPI row
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    kpis = [
        ("Studies",    "SELECT COUNT(*) AS n FROM studies"),
        ("Patients",   "SELECT COUNT(*) AS n FROM patients"),
        ("Samples",    "SELECT COUNT(*) AS n FROM samples"),
        ("Assays",     "SELECT COUNT(*) AS n FROM assays"),
        ("Files",      "SELECT COUNT(*) AS n FROM files"),
        ("Biomarkers", "SELECT COUNT(*) AS n FROM biomarkers"),
    ]
    for col, (label, sql) in zip([col1, col2, col3, col4, col5, col6], kpis):
        val = q(sql).iloc[0]["n"]
        col.metric(label, f"{val:,}")

    st.markdown("---")
    cl, cr = st.columns(2)

    with cl:
        st.subheader("Studies by Status")
        df_status = q("SELECT status, COUNT(*) AS count FROM studies GROUP BY status ORDER BY count DESC")
        if not df_status.empty:
            st.bar_chart(df_status.set_index("status"))

    with cr:
        st.subheader("Studies by Phase")
        df_phase = q("SELECT phase, COUNT(*) AS count FROM studies GROUP BY phase ORDER BY phase")
        if not df_phase.empty:
            st.bar_chart(df_phase.set_index("phase"))

    st.markdown("---")
    st.subheader("Study Registry")
    df_studies = q(f"SELECT * FROM studies {study_filter_sql} ORDER BY start_date DESC")
    st.dataframe(df_studies, use_container_width=True, hide_index=True)


# ============================================================
# TAB 2 — PATIENTS
# ============================================================
with tab_pat:
    st.header("Patient Demographics")
    sid_clause = _sid_in(_filtered_study_ids()) if (sel_areas or sel_phases) else "1=1"

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Age Distribution")
        df_age = q(f"SELECT age FROM patients WHERE {sid_clause}")
        if not df_age.empty:
            bins = list(range(18, 91, 5))
            df_age["age_group"] = pd.cut(df_age["age"], bins=bins,
                                         labels=[f"{b}-{b+4}" for b in bins[:-1]])
            st.bar_chart(df_age["age_group"].value_counts().sort_index())

    with c2:
        st.subheader("Sex Distribution")
        df_sex = q(f"SELECT sex, COUNT(*) AS count FROM patients WHERE {sid_clause} GROUP BY sex")
        if not df_sex.empty:
            st.bar_chart(df_sex.set_index("sex"))

    c3, c4 = st.columns(2)
    with c3:
        st.subheader("Race")
        df_race = q(
            f"SELECT race, COUNT(*) AS count FROM patients WHERE {sid_clause} "
            "GROUP BY race ORDER BY count DESC"
        )
        if not df_race.empty:
            st.bar_chart(df_race.set_index("race"))

    with c4:
        st.subheader("Enrollment Status")
        df_enr = q(
            f"SELECT status, COUNT(*) AS count FROM patients WHERE {sid_clause} GROUP BY status"
        )
        if not df_enr.empty:
            st.bar_chart(df_enr.set_index("status"))

    st.markdown("---")
    st.subheader("Patient Table")
    df_pat_tbl = q(f"""
        SELECT p.patient_id, s.name AS study_name, s.therapeutic_area,
               p.age, p.sex, p.race, p.ethnicity,
               p.primary_diagnosis, p.enrollment_date, p.site_id, p.status
        FROM patients p
        JOIN studies s ON p.study_id = s.study_id
        WHERE {sid_clause.replace('study_id', 'p.study_id')}
        ORDER BY p.enrollment_date DESC
    """)
    st.dataframe(df_pat_tbl, use_container_width=True, hide_index=True)


# ============================================================
# TAB 3 — SAMPLES
# ============================================================
with tab_samp:
    st.header("Sample Inventory")

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Sample Types")
        df_stype = q("SELECT sample_type, COUNT(*) AS count FROM samples GROUP BY sample_type ORDER BY count DESC")
        if not df_stype.empty:
            st.bar_chart(df_stype.set_index("sample_type"))

    with c2:
        st.subheader("Quality Score Distribution")
        df_qual = q("SELECT quality_score FROM samples")
        if not df_qual.empty:
            labels = ["0–2", "2–4", "4–6", "6–8", "8–10"]
            df_qual["bin"] = pd.cut(df_qual["quality_score"],
                                    bins=[0, 2, 4, 6, 8, 10], labels=labels)
            st.bar_chart(df_qual["bin"].value_counts().reindex(labels).fillna(0))

    st.markdown("---")
    c3, c4 = st.columns(2)
    with c3:
        st.subheader("Volume (mL) Distribution")
        df_vol = q("SELECT volume_ml FROM samples")
        if not df_vol.empty:
            df_vol["bin"] = pd.cut(df_vol["volume_ml"],
                                   bins=[0, 1, 2, 3, 4, 5, 100],
                                   labels=["0–1", "1–2", "2–3", "3–4", "4–5", "5+"])
            st.bar_chart(df_vol["bin"].value_counts().sort_index())

    with c4:
        st.subheader("Sample Collection Timeline")
        df_time = q("""
            SELECT SUBSTR(collection_date, 1, 7) AS month, COUNT(*) AS count
            FROM samples GROUP BY month ORDER BY month
        """)
        if not df_time.empty:
            st.line_chart(df_time.set_index("month"))

    st.markdown("---")
    st.subheader("Sample Table (first 500)")
    df_samp_tbl = q("""
        SELECT s.sample_id, p.patient_id, st.name AS study_name,
               s.sample_type, s.tissue_type, s.collection_date,
               s.quality_score, s.volume_ml, s.storage_location
        FROM samples s
        JOIN patients p ON s.patient_id = p.patient_id
        JOIN studies st ON p.study_id = st.study_id
        ORDER BY s.collection_date DESC
        LIMIT 500
    """)
    st.dataframe(df_samp_tbl, use_container_width=True, hide_index=True)


# ============================================================
# TAB 4 — ASSAYS
# ============================================================
with tab_assay:
    st.header("Assay & File Summary")

    c1, c2, c3 = st.columns(3)
    with c1:
        st.subheader("Assay Types")
        df_atype = q("SELECT assay_type, COUNT(*) AS count FROM assays GROUP BY assay_type")
        if not df_atype.empty:
            st.bar_chart(df_atype.set_index("assay_type"))

    with c2:
        st.subheader("QC Pass / Fail")
        df_qc = q("SELECT qc_pass, COUNT(*) AS count FROM assays GROUP BY qc_pass")
        if not df_qc.empty:
            df_qc["qc_pass"] = df_qc["qc_pass"].map({1: "Pass", 0: "Fail"})
            st.bar_chart(df_qc.set_index("qc_pass"))

    with c3:
        st.subheader("Vendors")
        df_vendor = q("SELECT vendor, COUNT(*) AS count FROM assays GROUP BY vendor ORDER BY count DESC")
        if not df_vendor.empty:
            st.bar_chart(df_vendor.set_index("vendor"))

    c4, c5 = st.columns(2)
    with c4:
        st.subheader("File Types")
        df_ftype = q("SELECT file_type, COUNT(*) AS count FROM files GROUP BY file_type")
        if not df_ftype.empty:
            st.bar_chart(df_ftype.set_index("file_type"))

    with c5:
        st.subheader("Run Timeline")
        df_runs = q("""
            SELECT SUBSTR(run_date, 1, 7) AS month, COUNT(*) AS count
            FROM assays GROUP BY month ORDER BY month
        """)
        if not df_runs.empty:
            st.line_chart(df_runs.set_index("month"))

    st.markdown("---")
    st.subheader("Assay Table (first 500)")
    df_assay_tbl = q("""
        SELECT a.assay_id, a.sample_id, a.assay_type, a.vendor,
               a.platform, a.run_date, a.run_id,
               CASE a.qc_pass WHEN 1 THEN 'Pass' ELSE 'Fail' END AS qc,
               a.status
        FROM assays a
        ORDER BY a.run_date DESC
        LIMIT 500
    """)
    st.dataframe(df_assay_tbl, use_container_width=True, hide_index=True)

    st.subheader("File Table (first 500)")
    df_file_tbl = q("""
        SELECT f.file_id, f.assay_id, f.file_name, f.file_type,
               f.file_size_mb, f.upload_date, f.checksum
        FROM files f
        ORDER BY f.upload_date DESC
        LIMIT 500
    """)
    st.dataframe(df_file_tbl, use_container_width=True, hide_index=True)


# ============================================================
# TAB 5 — BIOMARKERS
# ============================================================
with tab_bm:
    st.header("Biomarker Analysis")

    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Top 20 Differentially Expressed Genes")
        df_deg = q("""
            SELECT gene_symbol, ensembl_id,
                   ROUND(AVG(log2_fold_change), 3) AS mean_log2fc,
                   ROUND(AVG(pvalue), 6)           AS mean_pvalue,
                   ROUND(AVG(padj), 6)             AS mean_padj,
                   ROUND(AVG(expression_tpm), 2)   AS mean_tpm,
                   COUNT(*)                         AS n_records
            FROM biomarkers
            WHERE log2_fold_change IS NOT NULL
            GROUP BY gene_symbol, ensembl_id
            ORDER BY ABS(mean_log2fc) DESC
            LIMIT 20
        """)
        st.dataframe(df_deg, use_container_width=True, hide_index=True)

    with c2:
        st.subheader("Variant Type Distribution")
        df_var = q("""
            SELECT variant_type, COUNT(*) AS count
            FROM biomarkers
            WHERE variant_type IS NOT NULL
            GROUP BY variant_type ORDER BY count DESC
        """)
        if not df_var.empty:
            st.bar_chart(df_var.set_index("variant_type"))
        else:
            st.info("No variant records found.")

    st.markdown("---")
    st.subheader("Volcano Plot  (log₂FC vs −log₁₀ p-value)")
    st.caption("Colour = adjusted p-value (padj); only RNA-seq expression records shown.")
    df_volc = q("""
        SELECT b.gene_symbol,
               b.log2_fold_change,
               (-1.0 * LOG(MAX(b.pvalue, 1e-300)) / LOG(10)) AS neg_log10_p,
               b.padj,
               a.assay_type
        FROM biomarkers b
        JOIN assays a ON b.assay_id = a.assay_id
        WHERE b.log2_fold_change IS NOT NULL
          AND b.pvalue IS NOT NULL
          AND b.pvalue > 0
        LIMIT 3000
    """)
    if not df_volc.empty:
        st.scatter_chart(
            df_volc,
            x="log2_fold_change",
            y="neg_log10_p",
            color="padj",
            size="neg_log10_p",
        )

    st.markdown("---")
    st.subheader("Biomarker Table (first 1000 by |log2FC|)")
    df_bm_tbl = q("""
        SELECT b.gene_symbol, b.ensembl_id,
               b.log2_fold_change, b.pvalue, b.padj,
               b.expression_tpm, b.variant_type,
               a.assay_type, a.vendor
        FROM biomarkers b
        JOIN assays a ON b.assay_id = a.assay_id
        ORDER BY ABS(b.log2_fold_change) DESC
        LIMIT 1000
    """)
    st.dataframe(df_bm_tbl, use_container_width=True, hide_index=True)
