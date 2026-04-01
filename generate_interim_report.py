#!/usr/bin/env python3
"""Generate the Thursday Interim PDF Report."""
import json, os, glob
from datetime import datetime, timezone
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.colors import HexColor
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib import colors

BASE = os.path.dirname(os.path.abspath(__file__))
OUT_PDF = os.path.join(BASE, "enforcer_report", "interim_report.pdf")


def load_validation_reports():
    reports = []
    vr_dir = os.path.join(BASE, "validation_reports")
    for f in glob.glob(os.path.join(vr_dir, "*.json")):
        if "schema_evolution" in f:
            continue
        with open(f) as fh:
            reports.append(json.load(fh))
    return reports


def build_pdf():
    os.makedirs(os.path.dirname(OUT_PDF), exist_ok=True)
    doc = SimpleDocTemplate(OUT_PDF, pagesize=A4,
                            leftMargin=0.75*inch, rightMargin=0.75*inch,
                            topMargin=0.75*inch, bottomMargin=0.75*inch)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('Title2', parent=styles['Title'], fontSize=20, spaceAfter=20)
    h1 = ParagraphStyle('H1', parent=styles['Heading1'], fontSize=16, spaceAfter=12, textColor=HexColor('#1a5276'))
    h2 = ParagraphStyle('H2', parent=styles['Heading2'], fontSize=13, spaceAfter=8, textColor=HexColor('#2c3e50'))
    body = styles['BodyText']
    body.fontSize = 10
    body.leading = 14

    elements = []

    # Title
    elements.append(Paragraph("Data Contract Enforcer — Interim Report", title_style))
    elements.append(Paragraph(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}", body))
    elements.append(Paragraph("Author: Shuaib (IbnuEyni) | Week 7 TRP Submission", body))
    elements.append(Spacer(1, 20))

    # Section 1: Data Flow Diagram
    elements.append(Paragraph("1. Data Flow Diagram", h1))
    elements.append(Paragraph(
        "The five systems communicate through structured JSONL outputs. Each arrow represents "
        "a data contract that the Data Contract Enforcer validates.", body))
    elements.append(Spacer(1, 10))

    flow_data = [
        ["Source System", "Output Schema", "Arrow →", "Consumer System"],
        ["Week 1: Intent-Code Correlator", "intent_record", "code_refs[].file →", "Week 2: Digital Courtroom"],
        ["Week 2: Digital Courtroom", "verdict_record", "scores, verdict →", "Week 7: AI Extensions"],
        ["Week 3: Document Refinery", "extraction_record", "doc_id, facts →", "Week 4: Cartographer"],
        ["Week 4: Brownfield Cartographer", "lineage_snapshot", "nodes, edges →", "Week 7: ViolationAttributor"],
        ["Week 5: Event Sourcing", "event_record", "payload, type →", "Week 7: Schema Validation"],
        ["All Weeks (LLM calls)", "trace_record", "tokens, cost →", "Week 7: AI Extensions"],
    ]
    t = Table(flow_data, colWidths=[1.8*inch, 1.3*inch, 1.3*inch, 1.8*inch])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), HexColor('#1a5276')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#eaf2f8')]),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ]))
    elements.append(t)
    elements.append(Spacer(1, 20))

    # Section 2: Contract Coverage Table
    elements.append(Paragraph("2. Contract Coverage Table", h1))

    coverage_data = [
        ["Inter-System Interface", "Contract Written?", "Clauses", "Notes"],
        ["Week 1 → Week 2 (code_refs)", "Yes", "12+", "confidence range, UUID format, non-empty arrays"],
        ["Week 2 → Week 7 (verdicts)", "Yes", "10+", "enum PASS/FAIL/WARN, score 1-5, rubric hash"],
        ["Week 3 → Week 4 (extractions)", "Yes", "14+", "confidence 0.0-1.0, entity cross-refs, SHA-256 hash"],
        ["Week 4 → Week 7 (lineage)", "Yes", "10+", "graph integrity, 40-char git commit, enum node types"],
        ["Week 5 → Week 7 (events)", "Yes", "12+", "temporal order, PascalCase types, monotonic sequence"],
        ["Traces → Week 7 (LangSmith)", "Yes", "10+", "token sum, temporal order, cost >= 0, enum run_type"],
    ]
    t2 = Table(coverage_data, colWidths=[1.8*inch, 1.0*inch, 0.7*inch, 2.7*inch])
    t2.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), HexColor('#1a5276')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#eaf2f8')]),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ]))
    elements.append(t2)
    elements.append(Spacer(1, 20))

    # Section 3: First Validation Run Results
    elements.append(Paragraph("3. First Validation Run Results", h1))

    reports = load_validation_reports()
    for report in reports:
        cid = report.get("contract_id", "unknown")
        elements.append(Paragraph(f"Contract: {cid}", h2))

        summary_data = [
            ["Metric", "Value"],
            ["Total Checks", str(report.get("total_checks", 0))],
            ["Passed", str(report.get("passed", 0))],
            ["Failed", str(report.get("failed", 0))],
            ["Warned", str(report.get("warned", 0))],
            ["Errored", str(report.get("errored", 0))],
        ]
        ts = Table(summary_data, colWidths=[2*inch, 1.5*inch])
        ts.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), HexColor('#2c3e50')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ]))
        elements.append(ts)
        elements.append(Spacer(1, 6))

        # List failures
        failures = [r for r in report.get("results", []) if r["status"] in ("FAIL", "ERROR")]
        if failures:
            elements.append(Paragraph("Violations found:", body))
            for f in failures:
                severity = f.get("severity", "UNKNOWN")
                msg = f.get("message", f.get("check_id", ""))
                color = "#e74c3c" if severity == "CRITICAL" else "#e67e22" if severity == "HIGH" else "#f39c12"
                elements.append(Paragraph(
                    f'<font color="{color}"><b>[{severity}]</b></font> {msg}', body))
        else:
            elements.append(Paragraph("No violations found — all checks passed.", body))
        elements.append(Spacer(1, 12))

    # Section 4: Reflection
    elements.append(PageBreak())
    elements.append(Paragraph("4. Reflection", h1))

    reflection = """
    Writing data contracts for my own systems revealed assumptions I never documented.
    The most significant discovery was that my Week 3 Document Refinery's extraction_ledger.jsonl
    contains duplicate doc_ids — the same document was processed multiple times with different
    strategies (vision_augmented, fast_text, layout_aware). I had assumed doc_id was unique,
    but the ledger is actually an append-only log of extraction attempts, not a deduplicated
    output table. This means any downstream consumer treating doc_id as a primary key would
    silently merge or overwrite records from different extraction strategies.

    The second surprise was the page_ref range. My contract auto-inferred a maximum of 15
    from the profiled data, but the Annual Report JUNE-2018 has 92 pages. The auto-inferred
    range was too tight because the profiling sample happened to miss the long documents.
    This taught me that statistical profiling alone is insufficient for range constraints —
    domain knowledge (a PDF can have hundreds of pages) must override sample statistics.

    The Week 5 event records exposed a design tension: aggregate_id is intentionally not unique
    (multiple events per aggregate) and not in UUID format (it uses domain-meaningful prefixes
    like "loan-demo-xxx"). The canonical schema expects UUID format, but the real domain model
    uses human-readable composite IDs for debuggability. This is a legitimate design choice
    that the contract should accommodate, not reject.

    The most valuable output was the lineage-driven blast radius. Seeing that a confidence
    field change in Week 3 affects 7 downstream nodes in the Week 4 graph made the cost of
    undocumented schema changes concrete. Before this exercise, "it might break something
    downstream" was abstract. Now it's "it breaks the cartographer's edge weighting, the
    onboarding brief's quality assessment, and the Week 7 violation attributor's confidence
    scoring — affecting 331 records."
    """
    for para in reflection.strip().split("\n\n"):
        elements.append(Paragraph(para.strip(), body))
        elements.append(Spacer(1, 8))

    doc.build(elements)
    print(f"Thursday PDF written to: {OUT_PDF}")


if __name__ == "__main__":
    build_pdf()
