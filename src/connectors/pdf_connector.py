import re
from pathlib import Path
from typing import Any

from .base_connector import BaseConnector


class PDFConnector(BaseConnector):
    """
    Biomarker report extractor.

    Supports two modes:
      - .txt  (mock): reads plain text directly
      - .pdf  (real): uses pypdf to extract text page by page

    Either way, the extracted text is parsed line-by-line with a regex
    to produce a list of biomarker dicts.
    """

    BIOMARKER_PATTERN = re.compile(
        r"assay_id=(?P<assay_id>\S+)\s+"
        r"gene=(?P<gene_symbol>\S+)\s+"
        r"ensembl=(?P<ensembl_id>\S+)\s+"
        r"log2fc=(?P<log2fc>\S+)\s+"
        r"pvalue=(?P<pvalue>\S+)\s+"
        r"padj=(?P<padj>\S+)\s+"
        r"tpm=(?P<tpm>\S+)\s+"
        r"variant_type=(?P<variant_type>\S+)"
    )

    def _read_text(self) -> str:
        suffix = self.source_path.suffix.lower()
        if suffix == ".pdf":
            try:
                from pypdf import PdfReader
                reader = PdfReader(str(self.source_path))
                return "\n".join(page.extract_text() or "" for page in reader.pages)
            except ImportError:
                raise RuntimeError("pypdf required: pip install pypdf")
        return self.source_path.read_text(encoding="utf-8")

    def _parse_value(self, raw: str) -> float | None:
        return None if raw == "None" else float(raw)

    def extract(self) -> list[dict[str, Any]]:
        text = self._read_text()
        rows = []
        for line in text.splitlines():
            m = self.BIOMARKER_PATTERN.search(line)
            if not m:
                continue
            g = m.groupdict()
            vtype = g["variant_type"]
            rows.append({
                "assay_id":          g["assay_id"],
                "gene_symbol":       g["gene_symbol"],
                "ensembl_id":        g["ensembl_id"],
                "log2_fold_change":  self._parse_value(g["log2fc"]),
                "pvalue":            self._parse_value(g["pvalue"]),
                "padj":              self._parse_value(g["padj"]),
                "expression_tpm":    self._parse_value(g["tpm"]),
                "variant_type":      None if vtype == "None" else vtype,
            })
        print(f"[PDFConnector] Parsed {len(rows)} biomarker rows from {self.source_name}")
        return rows
