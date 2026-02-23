"""
Generates a Draw.io compatible mxGraph XML file showing the full
clinical data hub architecture in 4 colour-coded swim-lane sections:

  Section 1 (Blue)   — Source Data Layer
  Section 2 (Green)  — Connector / ETL Layer
  Section 3 (Orange) — Unified Schema (entities + 1:N relationships)
  Section 4 (Purple) — Output Layer (Streamlit + Draw.io diagram)

Open the output XML in https://app.diagrams.net via
  File > Import from > Device …  or  Extras > Edit Diagram (paste XML).
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path
from xml.dom import minidom

# ---------------------------------------------------------------------------
# Layout constants (pixels)
# ---------------------------------------------------------------------------
LANE_Y       = 20       # top of swim-lane band
LANE_H       = 560      # height of each lane band
HEADER_H     = 30       # swim-lane title bar height
NODE_W       = 130
NODE_H       = 50
GAP          = 20

# X centres for each section
X_SOURCE = 60
X_ETL    = 270
X_SCHEMA = 510
X_SCHEMA2 = 680   # second column inside schema section
X_OUTPUT  = 880

# ---------------------------------------------------------------------------
# Colour palette
# ---------------------------------------------------------------------------
COLORS = {
    "source":  {"fill": "#dae8fc", "stroke": "#6c8ebf"},
    "etl":     {"fill": "#d5e8d4", "stroke": "#82b366"},
    "schema":  {"fill": "#ffe6cc", "stroke": "#d6b656"},
    "output":  {"fill": "#e1d5e7", "stroke": "#9673a6"},
    "db":      {"fill": "#f8cecc", "stroke": "#b85450"},
    "arrow":   {"stroke": "#666666"},
}

# ---------------------------------------------------------------------------
# Node definitions  (id, label, x, y, w, h, color_key)
# ---------------------------------------------------------------------------
NODES: list[tuple] = [
    # ---- Source Data -------------------------------------------------------
    ("src_csv",    "CSV\npatients.csv",          70,   80, NODE_W, NODE_H, "source"),
    ("src_sheets", "Google Sheets\n(mock CSV)",  70,  170, NODE_W, NODE_H, "source"),
    ("src_json",   "JSON\nvendor_data.json",     70,  260, NODE_W, NODE_H, "source"),
    ("src_pdf",    "PDF / TXT\nbiomarker_report",70,  350, NODE_W, NODE_H, "source"),

    # ---- ETL Layer ---------------------------------------------------------
    ("etl_patient",   "Patient\nTransformer",   275,  80, NODE_W, NODE_H, "etl"),
    ("etl_study",     "Study\nTransformer",     275, 170, NODE_W, NODE_H, "etl"),
    ("etl_sample",    "Sample/Assay/\nFile Trans.",275,260, NODE_W, NODE_H, "etl"),
    ("etl_biomarker", "Biomarker\nTransformer", 275, 350, NODE_W, NODE_H, "etl"),
    ("etl_pipeline",  "ETL Pipeline\norchestrator", 275, 460, NODE_W, 45, "etl"),

    # ---- Unified Schema — primary column ----------------------------------
    ("ent_study",     "Study",     510,  60, NODE_W, NODE_H, "schema"),
    ("ent_patient",   "Patient",   510, 150, NODE_W, NODE_H, "schema"),
    ("ent_sample",    "Sample",    510, 240, NODE_W, NODE_H, "schema"),
    ("ent_assay",     "Assay",     510, 330, NODE_W, NODE_H, "schema"),

    # ---- Unified Schema — secondary column --------------------------------
    ("ent_file",      "File",      680, 290, NODE_W, NODE_H, "schema"),
    ("ent_biomarker", "Biomarker", 680, 380, NODE_W, NODE_H, "schema"),

    # ---- SQLite DB --------------------------------------------------------
    ("ent_sqlite",    "SQLite DB\nclinical_hub.db", 580, 460, NODE_W + 20, 45, "db"),

    # ---- Output -----------------------------------------------------------
    ("out_dashboard", "Streamlit\nDashboard",   880, 150, NODE_W, NODE_H, "output"),
    ("out_drawio",    "Draw.io\nDiagram XML",   880, 300, NODE_W, NODE_H, "output"),
]

# ---------------------------------------------------------------------------
# Edge definitions  (from_id, to_id, label, exit_x, exit_y, entry_x, entry_y)
# (None = auto)
# ---------------------------------------------------------------------------
EDGES: list[tuple] = [
    # Sources → Transformers
    ("src_csv",    "etl_patient",   "extract",  None, None, None, None),
    ("src_sheets", "etl_study",     "extract",  None, None, None, None),
    ("src_json",   "etl_sample",    "extract",  None, None, None, None),
    ("src_pdf",    "etl_biomarker", "extract",  None, None, None, None),

    # Transformers → Pipeline
    ("etl_patient",   "etl_pipeline", "", None, None, None, None),
    ("etl_study",     "etl_pipeline", "", None, None, None, None),
    ("etl_sample",    "etl_pipeline", "", None, None, None, None),
    ("etl_biomarker", "etl_pipeline", "", None, None, None, None),

    # Pipeline → Schema entities
    ("etl_pipeline", "ent_study",     "write", None, None, None, None),
    ("etl_pipeline", "ent_patient",   "write", None, None, None, None),
    ("etl_pipeline", "ent_sample",    "write", None, None, None, None),
    ("etl_pipeline", "ent_assay",     "write", None, None, None, None),
    ("etl_pipeline", "ent_file",      "write", None, None, None, None),
    ("etl_pipeline", "ent_biomarker", "write", None, None, None, None),

    # 1:N entity relationships
    ("ent_study",   "ent_patient",   "1 : N", None, None, None, None),
    ("ent_patient", "ent_sample",    "1 : N", None, None, None, None),
    ("ent_sample",  "ent_assay",     "1 : N", None, None, None, None),
    ("ent_assay",   "ent_file",      "1 : N", None, None, None, None),
    ("ent_assay",   "ent_biomarker", "1 : N", None, None, None, None),

    # Entities → SQLite
    ("ent_study",     "ent_sqlite", "", None, None, None, None),
    ("ent_patient",   "ent_sqlite", "", None, None, None, None),
    ("ent_sample",    "ent_sqlite", "", None, None, None, None),
    ("ent_assay",     "ent_sqlite", "", None, None, None, None),
    ("ent_file",      "ent_sqlite", "", None, None, None, None),
    ("ent_biomarker", "ent_sqlite", "", None, None, None, None),

    # SQLite → Dashboard
    ("ent_sqlite",    "out_dashboard", "SQL queries", None, None, None, None),
    ("etl_pipeline",  "out_drawio",    "generates",   None, None, None, None),
]

# ---------------------------------------------------------------------------
# Swim-lane section banners  (id, label, x, y, w, h, color_key)
# ---------------------------------------------------------------------------
LANES: list[tuple] = [
    ("lane_source", "SOURCE DATA",    20,  LANE_Y, 210, LANE_H, "source"),
    ("lane_etl",    "ETL LAYER",     240,  LANE_Y, 210, LANE_H, "etl"),
    ("lane_schema", "UNIFIED SCHEMA",470,  LANE_Y, 370, LANE_H, "schema"),
    ("lane_output", "OUTPUT LAYER",  860,  LANE_Y, 190, LANE_H, "output"),
]


# ---------------------------------------------------------------------------
# Builder helpers
# ---------------------------------------------------------------------------

def _node_style(color_key: str) -> str:
    c = COLORS[color_key]
    return (
        f"rounded=1;whiteSpace=wrap;html=1;arcSize=20;"
        f"fillColor={c['fill']};strokeColor={c['stroke']};"
        f"fontStyle=1;fontSize=11;"
    )


def _lane_style(color_key: str) -> str:
    c = COLORS[color_key]
    return (
        f"swimlane;startSize={HEADER_H};fontStyle=1;fontSize=12;"
        f"fillColor={c['fill']};strokeColor={c['stroke']};"
    )


def _edge_style() -> str:
    return (
        "edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;"
        "jettySize=auto;exitX=1;exitY=0.5;exitDx=0;exitDy=0;"
        "entryX=0;entryY=0.5;entryDx=0;entryDy=0;"
        "fontSize=9;fontStyle=2;"
    )


def _add_cell(root: ET.Element, cell_id: str, label: str,
              x: float, y: float, w: float, h: float,
              style: str, vertex: bool = True) -> ET.Element:
    cell = ET.SubElement(root, "mxCell", attrib={
        "id": cell_id, "value": label, "style": style,
        "vertex": "1" if vertex else "0", "parent": "1",
    })
    ET.SubElement(cell, "mxGeometry", attrib={
        "x": str(x), "y": str(y), "width": str(w), "height": str(h),
        "as": "geometry",
    })
    return cell


def _add_edge(root: ET.Element, edge_id: str, label: str,
              src: str, tgt: str) -> ET.Element:
    cell = ET.SubElement(root, "mxCell", attrib={
        "id": edge_id, "value": label,
        "style": _edge_style(),
        "edge": "1", "source": src, "target": tgt, "parent": "1",
    })
    ET.SubElement(cell, "mxGeometry", attrib={"relative": "1", "as": "geometry"})
    return cell


# ---------------------------------------------------------------------------
# Main public function
# ---------------------------------------------------------------------------

def generate_drawio_xml(output_path: Path) -> None:
    """Build the mxGraph XML and write it to *output_path*."""

    mxfile = ET.Element("mxfile", attrib={
        "host": "app.diagrams.net", "version": "21.0.0",
        "type": "device",
    })
    diagram = ET.SubElement(mxfile, "diagram", attrib={
        "id": "clinical-hub-diagram", "name": "Clinical Data Hub",
    })
    graph_model = ET.SubElement(diagram, "mxGraphModel", attrib={
        "dx": "1500", "dy": "800",
        "grid": "1", "gridSize": "10",
        "guides": "1", "tooltips": "1",
        "connect": "1", "arrows": "1", "fold": "1",
        "page": "1", "pageScale": "1",
        "pageWidth": "1169", "pageHeight": "827",
        "math": "0", "shadow": "0",
    })
    root = ET.SubElement(graph_model, "root")

    # Required base cells
    ET.SubElement(root, "mxCell", attrib={"id": "0"})
    ET.SubElement(root, "mxCell", attrib={"id": "1", "parent": "0"})

    # Swim-lane banners
    for lane_id, label, x, y, w, h, ck in LANES:
        _add_cell(root, lane_id, label, x, y, w, h, _lane_style(ck))

    # Entity / process nodes
    for node_id, label, x, y, w, h, ck in NODES:
        _add_cell(root, node_id, label, x, y, w, h, _node_style(ck))

    # Directed edges
    for i, (src, tgt, label, *_) in enumerate(EDGES):
        _add_edge(root, f"e{i:03d}", label, src, tgt)

    # Pretty-print
    raw_xml = ET.tostring(mxfile, encoding="unicode")
    pretty_xml = minidom.parseString(raw_xml).toprettyxml(indent="  ")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(pretty_xml, encoding="utf-8")
    print(f"[DrawIO] Diagram written: {output_path}")
