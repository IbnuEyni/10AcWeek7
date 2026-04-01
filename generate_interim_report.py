#!/usr/bin/env python3
"""Generate the Thursday Interim PDF Report."""
import os
from datetime import datetime, timezone
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.colors import HexColor
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Image as RLImage
from reportlab.lib import colors

BASE = os.path.dirname(os.path.abspath(__file__))
OUT_PDF = os.path.join(BASE, "enforcer_report", "interim_report.pdf")



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
    elements.append(Paragraph("Author: Amir Ahmedin | Week 7 TRP Interim Submission", body))
    elements.append(Spacer(1, 20))

    # Section 1: Data Flow Diagram
    elements.append(Paragraph("1. Data Flow Diagram", h1))
    elements.append(Paragraph(
        "The five systems built over Weeks 1\u20135 communicate through structured JSONL outputs. "
        "Each arrow is annotated with the <b>schema name</b> and the <b>specific fields</b> consumed "
        "by the downstream system. Solid arrows (\u2500\u2500\u25B6) represent data flow contracts enforced by "
        "the ValidationRunner. Dashed arrows (- -\u25B6) represent contract enforcement flow (Week 7 "
        "components consuming data for validation, not production logic).", body))
    elements.append(Spacer(1, 10))

    diagram_path = os.path.join(BASE, "Flow Diagram.png")
    if os.path.exists(diagram_path):
        img_w = doc.width
        img_h = img_w * (2108 / 2823)
        elements.append(RLImage(diagram_path, width=img_w, height=img_h))
        elements.append(Spacer(1, 10))

    key_text = (
        "<b>Diagram key:</b> "
        "<font color='#27ae60'>\u25CF Green nodes</font> = Weeks 1, 2, 5 (data producers) | "
        "<font color='#2980b9'>\u25CF Blue nodes</font> = Weeks 3, 4 (critical downstream dependencies) | "
        "<font color='#f1c40f'>\u25CF Yellow node</font> = LangSmith traces | "
        "<font color='#e74c3c'>\u25CF Red nodes</font> = Week 7 enforcement components | "
        "Solid arrows = Production data flow contracts | Dashed arrows = Enforcement-only flow"
    )
    elements.append(Paragraph(key_text, body))
    elements.append(Spacer(1, 8))
    elements.append(Paragraph(
        "<b>Data provenance:</b> All outputs are migrated from real Week 1\u20135 implementations using "
        "migration scripts. Week 3 data comes from the actual extraction ledger (64 documents "
        "including Ethiopian financial reports, CBE annual reports, and CPI indices). Week 4 data "
        "comes from real lineage graphs of dbt-core, jaffle-shop, and ol-data-platform codebases "
        "(179 nodes, 65 edges). Week 5 events are generated from the real LoanApplication domain "
        "model with 16 event types.", body))
    elements.append(Spacer(1, 20))

    # Section 2: Contract Coverage Table
    elements.append(Paragraph("2. Contract Coverage Table", h1))

    cell = ParagraphStyle('Cell', parent=body, fontSize=7, leading=9)
    cell_bold = ParagraphStyle('CellBold', parent=cell, fontName='Helvetica-Bold')

    def C(text):
        return Paragraph(text, cell)
    def CB(text):
        return Paragraph(text, cell_bold)

    coverage_data = [
        [CB('#'), CB('Inter-System Interface'), CB('Contract?'), CB('Structural'), CB('Statistical'), CB('Cross-field'), CB('Rationale / Gap')],
        [C('1'), C('Week 1 → Week 2: intent_record.code_refs[].file used as verdict.target_ref'),
         CB('Partial'),
         C('15 clauses: UUID on intent_id, ISO 8601 on created_at, confidence 0.0–1.0'),
         C('4 clauses: confidence mean/stddev baseline, cardinality checks'),
         C('0'),
         C('<b>Gap:</b> Cross-system join validation not implemented. verdict.target_ref not verified against intent_records.code_refs[].file.')],
        [C('2'), C('Week 2 → Week 7: verdict_record consumed by AI Contract Extensions'),
         CB('Yes'),
         C('12 clauses: overall_verdict ∈ {PASS, FAIL, WARN}, score 1–5, rubric_id SHA-256, confidence 0.0–1.0'),
         C('3 clauses: score distribution, confidence baseline'),
         C('1 clause: overall_score = weighted mean of scores{}'),
         C('Full coverage. AI Extensions consume verdict records for LLM output schema violation rate tracking.')],
        [C('3'), C('Week 3 → Week 4: extraction_record fields become Cartographer nodes'),
         CB('Yes'),
         C('16 clauses: confidence 0.0–1.0 (BREAKING if 0–100), source_hash SHA-256, extraction_model ^(claude|gpt)-, UUID on doc_id and fact_id'),
         C('5 clauses: confidence drift baseline, processing_time_ms range, token_count ranges'),
         C('2 clauses: entity_refs ⊆ entities[].entity_id, fact_id unique within record'),
         C('Full coverage. Highest-risk interface — confidence field change propagates silently to Cartographer edge weights.')],
        [C('4'), C('Week 4 → Week 7: lineage_snapshot used by ViolationAttributor'),
         CB('Yes'),
         C('14 clauses: git_commit 40-char hex, node.type ∈ 6 values, edge.relationship ∈ 6 values, UUID on snapshot_id'),
         C('2 clauses: node/edge count baselines'),
         C('2 clauses: edge.source ∈ nodes[].node_id, edge.target ∈ nodes[].node_id'),
         C('Full coverage. Graph integrity checks ensure blame chain traversal won\'t hit dangling references.')],
        [C('5'), C('Week 5 → Week 7: event_record.payload validated against event_type schema'),
         CB('Partial'),
         C('12 clauses: recorded_at ≥ occurred_at, event_type PascalCase, UUID on event_id'),
         C('3 clauses: sequence_number monotonicity, payload size baseline'),
         C('0'),
         C('<b>Gap:</b> Payload-level validation against per-event-type JSON Schema not yet implemented. Requires schema registry mapping event_type → JSON Schema.')],
        [C('6'), C('Traces → Week 7: trace_record consumed by AI Contract Extensions'),
         CB('Yes'),
         C('12 clauses: end_time > start_time, run_type ∈ 5 values, UUID on id, total_cost ≥ 0'),
         C('3 clauses: total_tokens baseline, cost baseline'),
         C('1 clause: total_tokens = prompt_tokens + completion_tokens'),
         C('Full coverage. Validates LangSmith trace integrity for AI contract extension metrics.')],
    ]

    avail = doc.width
    t2 = Table(coverage_data, colWidths=[
        avail*0.04, avail*0.18, avail*0.07, avail*0.20, avail*0.16, avail*0.14, avail*0.21
    ])
    t2.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), HexColor('#1a5276')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTSIZE', (0, 0), (-1, -1), 7),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#eaf2f8')]),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('LEFTPADDING', (0, 0), (-1, -1), 3),
        ('RIGHTPADDING', (0, 0), (-1, -1), 3),
        ('TOPPADDING', (0, 0), (-1, -1), 2),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
    ]))
    elements.append(t2)
    elements.append(Spacer(1, 6))
    elements.append(Paragraph(
        '<b>Coverage: 4/6 full, 2/6 partial.</b> The two partial contracts (Week 1→2 and Week 5→7) '
        'have identified gaps with specific remediation plans. Cross-system join validation and '
        'per-event-type payload schemas are the two remaining implementation targets for the Sunday submission.', cell))
    elements.append(Spacer(1, 20))

    # Section 3: First Validation Run Results
    elements.append(Paragraph("3. First Validation Run Results", h1))

    code = ParagraphStyle('Code', parent=body, fontName='Courier', fontSize=8, leading=10,
                          leftIndent=12, backColor=HexColor('#f8f9fa'))
    sev_text = (
        '<b>Severity framework:</b> '
        '<font color="#e74c3c"><b>CRITICAL</b></font> = Structural/type violation | '
        '<font color="#e67e22"><b>HIGH</b></font> = Statistical drift &gt; 3σ, uniqueness/format | '
        '<b>MEDIUM</b> = Drift 2–3σ | <b>LOW</b> = Passed | <b>WARNING</b> = Near-threshold'
    )
    elements.append(Paragraph(sev_text, body))
    elements.append(Spacer(1, 10))

    def summary_table(total, passed, failed, warned, errored):
        rate = f"{passed/total*100:.1f}%" if total else "N/A"
        data = [
            ["Metric", "Value"],
            ["Total Checks", str(total)], ["Passed", str(passed)],
            ["Failed", str(failed)], ["Warned", str(warned)],
            ["Errored", str(errored)], ["Pass Rate", rate],
        ]
        t = Table(data, colWidths=[1.5*inch, 1.2*inch])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), HexColor('#2c3e50')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('BACKGROUND', (0, -1), (-1, -1), HexColor('#eaf2f8')),
            ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
        ]))
        return t

    # 3.1 Week 3
    elements.append(Paragraph("3.1 Week 3 — Document Refinery Extractions", h2))
    elements.append(summary_table(57, 55, 2, 0, 0))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph(
        '<font color="#e67e22"><b>Violation 1 — doc_id.unique (HIGH):</b></font> '
        'Found 48 duplicate doc_id values out of 64 records. Root cause: the extraction ledger '
        'is an append-only log of extraction <i>attempts</i>. The same document was processed '
        'multiple times with different strategies (vision_augmented, fast_text, layout_aware).', body))
    elements.append(Paragraph(
        '<i>Impact:</i> Any downstream consumer treating doc_id as a primary key would silently '
        'merge records from different extraction strategies.', body))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph('Contract correction:', body))
    elements.append(Paragraph(
        '# BEFORE: unique: true  ← WRONG: ledger is append-only<br/>'
        '# AFTER:  unique: false, description: "Use (doc_id, extraction_model) as composite key."', code))
    elements.append(Spacer(1, 8))

    elements.append(Paragraph(
        '<font color="#e74c3c"><b>Violation 2 — extracted_facts[*].page_ref.range (CRITICAL):</b></font> '
        '331 fact records have page_ref values outside auto-inferred range [0, 15]. '
        'Actual range is [0, 91] because Annual_Report_JUNE-2018.pdf has 92 pages.', body))
    elements.append(Paragraph(
        '<i>Impact:</i> Any consumer filtering facts by page range would silently drop facts from long documents.', body))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph('Contract correction:', body))
    elements.append(Paragraph(
        '# BEFORE: maximum: 15.0  ← auto-inferred, too tight<br/>'
        '# AFTER:  maximum: 10000, description: "PDFs can have thousands of pages."', code))
    elements.append(Spacer(1, 12))

    # 3.2 Week 5
    elements.append(Paragraph("3.2 Week 5 — Event Sourcing Platform Events", h2))
    elements.append(summary_table(88, 86, 2, 0, 0))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph(
        '<font color="#e67e22"><b>Violation 1 — aggregate_id.unique (HIGH):</b></font> '
        'Found 45 duplicate aggregate_id values. This is <b>by design</b> — the event sourcing '
        'pattern stores multiple events per aggregate (e.g., ApplicationSubmitted, '
        'CreditAnalysisRequested, DecisionGenerated all share aggregate_id: loan-demo-2a7df24b).', body))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph('Contract correction:', body))
    elements.append(Paragraph(
        '# BEFORE: unique: true, format: uuid  ← WRONG: multiple events per aggregate<br/>'
        '# AFTER:  unique: false, pattern: "^(loan|agent-session|compliance|audit)-"', code))
    elements.append(Spacer(1, 8))

    elements.append(Paragraph(
        '<font color="#e67e22"><b>Violation 2 — aggregate_id.format (HIGH):</b></font> '
        'All 65 records fail UUID format validation. The real domain model uses composite IDs '
        'like loan-demo-2a7df24b — human-readable prefixes for debuggability, a deliberate '
        'design choice from LoanApplicationAggregate.', body))
    elements.append(Spacer(1, 12))

    # 3.3 Injected Violation
    elements.append(Paragraph("3.3 Injected Violation — Confidence Scale Change", h2))
    elements.append(Paragraph(
        'The ValidationRunner was run against extractions_violated.jsonl where confidence was '
        'intentionally changed from float 0.0–1.0 to integer 0–100:', body))
    elements.append(Spacer(1, 4))
    elements.append(summary_table(56, 53, 3, 0, 0))
    elements.append(Spacer(1, 6))

    fail_data = [
        ["Check", "Status", "Actual", "Expected"],
        ["extracted_facts[*].confidence.range", "FAIL", "max=98.8, mean=73.9", "max≤1.0, min≥0.0"],
        ["extracted_facts[*].confidence.drift", "FAIL", "2,324σ deviation", "within 3σ"],
        ["extracted_facts[*].page_ref.range", "FAIL", "max=20.0", "max≤15.0"],
    ]
    tf = Table(fail_data, colWidths=[2.2*inch, 0.6*inch, 1.6*inch, 1.4*inch])
    tf.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), HexColor('#e74c3c')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ]))
    elements.append(tf)
    elements.append(Spacer(1, 8))

    elements.append(Paragraph('<b>Validation report JSON excerpt</b> (from validation_reports/week3_violated.json):', body))
    json_excerpt = (
        '{<br/>'
        '&nbsp;&nbsp;"check_id": "week3-...extracted_facts[*].confidence.range",<br/>'
        '&nbsp;&nbsp;"check_type": "range", "status": "FAIL",<br/>'
        '&nbsp;&nbsp;"actual_value": "min=50.0, max=98.8, mean=73.9253",<br/>'
        '&nbsp;&nbsp;"expected": "min&gt;=0.0, max&lt;=1.0",<br/>'
        '&nbsp;&nbsp;"severity": "CRITICAL", "records_failing": 178,<br/>'
        '&nbsp;&nbsp;"message": "confidence is in 0–100 range, not 0.0–1.0. Breaking change detected."<br/>'
        '}'
    )
    elements.append(Paragraph(json_excerpt, code))
    elements.append(Spacer(1, 8))
    elements.append(Paragraph(
        'The ViolationAttributor traced this to commit <b>cd5737c</b> ("feat: change confidence to '
        'percentage scale") in src/week3/extractor.py, with a blast radius of <b>178 affected records</b> '
        'across <b>7 downstream nodes</b>.', body))
    elements.append(Spacer(1, 12))

    # Section 4: Reflection
    elements.append(PageBreak())
    elements.append(Paragraph("4. Reflection", h1))

    elements.append(Paragraph(
        'Writing data contracts for my own five systems revealed four wrong assumptions and '
        'one architectural gap. More importantly, it changed how I will build systems going forward.', body))
    elements.append(Spacer(1, 8))

    elements.append(Paragraph(
        '<b>Assumption 1: doc_id is unique.</b> It isn\'t. My Week 3 extraction ledger is an '
        'append-only log of extraction <i>attempts</i>, not a deduplicated output table. The '
        'contract caught 48 duplicates out of 64 records. The fix isn\'t just updating the '
        'contract \u2014 it\'s deciding whether the ledger should be deduplicated before feeding '
        'Week 4, or whether Week 4 should handle duplicates. For Week 8\'s Sentinel, this means '
        'the violation signal must distinguish "duplicate by design" from "duplicate by error."', body))
    elements.append(Spacer(1, 8))

    elements.append(Paragraph(
        '<b>Assumption 2: page_ref has a small range.</b> The auto-profiler inferred max=15, '
        'but Annual_Report_JUNE-2018.pdf has 92 pages. I never tested on documents longer than '
        '~20 pages during development. The lesson: statistical profiling from a non-representative '
        'sample produces contracts that reject valid production data. Domain knowledge must '
        'override sample statistics.', body))
    elements.append(Spacer(1, 8))

    elements.append(Paragraph(
        '<b>Assumption 3: aggregate_id is a UUID.</b> My Week 5 event store uses composite IDs '
        'like loan-demo-2a7df24b for debuggability. The contract should use a pattern match, not '
        'UUID format. This taught me that "standard format" assumptions break when applied to '
        'domain-specific design choices.', body))
    elements.append(Spacer(1, 8))

    elements.append(Paragraph(
        '<b>Assumption 4: confidence will always be 0.0\u20131.0.</b> It is today, but nothing '
        'prevents a future change. The injected test proved the contract catches this at '
        '2,324\u03c3 \u2014 but only because I wrote the contract. Without it, the Week 4 '
        'Cartographer would silently produce corrupted edge weights, and the error would surface '
        'weeks later in a downstream report.', body))
    elements.append(Spacer(1, 8))

    elements.append(Paragraph(
        '<b>The architectural gap:</b> There was no formal interface between any of my five '
        'systems. The schema was implicit \u2014 encoded in Python function signatures, not in a '
        'machine-checkable contract. Going forward, I will write the contract <i>before</i> the '
        'producer code, not after. The most valuable output is the blast radius: "this affects '
        '7 nodes and 178 records" is actionable; "something might break" is not.', body))
    elements.append(Spacer(1, 8))

    elements.append(Paragraph(
        '<b>Process reflection:</b> Writing contracts took longer than expected \u2014 roughly '
        '60% of the time was spent understanding my own data, not writing YAML. The auto-profiler '
        'was a useful starting point but required manual override for every domain-specific '
        'constraint. If I were starting over, I would co-locate the contract file next to the '
        'producer code and validate it in CI, so the contract evolves with the schema rather than '
        'being written retroactively.', body))
    elements.append(Spacer(1, 8))

    doc.build(elements)
    print(f"Thursday PDF written to: {OUT_PDF}")


if __name__ == "__main__":
    build_pdf()
