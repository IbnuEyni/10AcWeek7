# Data Contract Enforcer — Interim Submission Report

**Author:** Shuaib (IbnuEyni)  
**Date:** 2026-04-01  
**Repository:** [github.com/IbnuEyni/10AcWeek7](https://github.com/IbnuEyni/10AcWeek7)  
**Week 7 TRP Submission — Thursday Interim**

---

## 1. Data Flow Diagram

The five systems built over Weeks 1–5 communicate through structured JSONL outputs. Each arrow below is a data contract enforced by the Data Contract Enforcer. The schema name on each arrow is the canonical record type validated by the ValidationRunner.

```
┌─────────────────────────┐
│  Week 1: Intent-Code    │
│  Correlator             │
│  (211 intent_records)   │
└──────────┬──────────────┘
           │ intent_record
           │ code_refs[].file ──────────────────────┐
           ▼                                        ▼
┌─────────────────────────┐              ┌─────────────────────────┐
│  Week 2: Digital        │              │  Week 7: AI Contract    │
│  Courtroom              │──────────────│  Extensions             │
│  (16 verdict_records)   │ verdict_record│ (embedding drift,      │
└──────────┬──────────────┘  scores,     │  output schema,         │
           │                 verdict     │  prompt validation)     │
           │                             └─────────────────────────┘
           │                                        ▲
           │                                        │ trace_record
┌─────────────────────────┐              ┌─────────────────────────┐
│  Week 3: Document       │              │  LangSmith Traces       │
│  Refinery               │              │  (160 trace_records)    │
│  (64 extraction_records)│              │  from Weeks 2, 3, 4    │
└──────────┬──────────────┘              └─────────────────────────┘
           │ extraction_record
           │ doc_id, extracted_facts,
           │ confidence (0.0–1.0)
           ▼
┌─────────────────────────┐
│  Week 4: Brownfield     │
│  Cartographer           │
│  (8 lineage_snapshots)  │
│  179 nodes, 65 edges    │
└──────────┬──────────────┘
           │ lineage_snapshot
           │ nodes[], edges[], git_commit
           ▼
┌─────────────────────────┐
│  Week 7: Violation      │
│  Attributor             │
│  (blame chain +         │
│   blast radius)         │
└─────────────────────────┘
           ▲
           │ event_record
           │ event_type, payload,
           │ recorded_at >= occurred_at
┌─────────────────────────┐
│  Week 5: Event Sourcing │
│  Platform               │
│  (65 event_records)     │
│  LoanApplication domain │
└─────────────────────────┘
```

**Data sources:** All outputs are migrated from real Week 1–5 implementations using migration scripts (`migrate_week1.py` through `migrate_week5.py` + `migrate_traces.py`). Week 3 data comes from the actual extraction ledger (64 documents including Ethiopian financial reports, CBE annual reports, and CPI indices). Week 4 data comes from real lineage graphs of dbt-core, jaffle-shop, and ol-data-platform codebases. Week 5 events are generated from the real `LoanApplication` domain model defined in `10Acweek5/ledger/src/models/events.py`.

---

## 2. Contract Coverage Table

| # | Inter-System Interface | Contract? | Clauses | Key Enforcement Rules |
|---|---|---|---|---|
| 1 | Week 1 → Week 2: `intent_record.code_refs[].file` used as `verdict.target_ref` | **Yes** | 47 | confidence 0.0–1.0, UUID format on intent_id, code_refs non-empty, ISO 8601 timestamps |
| 2 | Week 2 → Week 7: `verdict_record` consumed by AI Contract Extensions | **Yes** | 47 | overall_verdict ∈ {PASS, FAIL, WARN}, score integer 1–5, rubric_id SHA-256 pattern, confidence 0.0–1.0 |
| 3 | Week 3 → Week 4: `extraction_record.doc_id` and `extracted_facts` become Cartographer nodes | **Yes** | 47 | confidence 0.0–1.0 (BREAKING if 0–100), entity_refs ⊆ entities[].entity_id, source_hash SHA-256, extraction_model matches `^(claude\|gpt)-` |
| 4 | Week 4 → Week 7: `lineage_snapshot` used by ViolationAttributor for blame chains | **Yes** | 47 | edge.source/target ∈ nodes[].node_id, git_commit 40-char hex, node.type ∈ {FILE, TABLE, SERVICE, MODEL, PIPELINE, EXTERNAL}, edge.relationship ∈ {IMPORTS, CALLS, READS, WRITES, PRODUCES, CONSUMES} |
| 5 | Week 5 → Week 7: `event_record.payload` validated against event_type schema | **Yes** | 47 | recorded_at ≥ occurred_at, event_type PascalCase, sequence_number monotonic per aggregate_id |
| 6 | Traces → Week 7: `trace_record` consumed by AI Contract Extensions | **Yes** | 47 | end_time > start_time, total_tokens = prompt_tokens + completion_tokens, run_type ∈ {llm, chain, tool, retriever, embedding}, total_cost ≥ 0 |

**Coverage: 6/6 interfaces have contracts (100%).** Every inter-system arrow in the data flow diagram has a corresponding Bitol-compatible YAML contract in `generated_contracts/` with a parallel dbt `schema.yml` counterpart.

---

## 3. First Validation Run Results

### Week 3 — Document Refinery Extractions

| Metric | Value |
|---|---|
| Total Checks | 57 |
| Passed | 55 |
| Failed | 2 |
| Warned | 0 |
| Errored | 0 |
| **Pass Rate** | **96.5%** |

**Violations found (both real, from actual data):**

1. **`doc_id.unique` — FAIL (HIGH):** Found 48 duplicate `doc_id` values. The extraction ledger is an append-only log — the same document (e.g., `2018_Audited_Financial_Statement_Report.pdf`) was processed multiple times with different strategies (`vision_augmented`, `fast_text`, `layout_aware`). Each re-extraction produces a new ledger entry with the same `doc_id`. This is a real design issue: downstream consumers treating `doc_id` as a primary key would silently merge records from different extraction strategies.

2. **`extracted_facts[*].page_ref.range` — FAIL (CRITICAL):** 331 fact records have `page_ref` values outside the auto-inferred range [0, 15]. The actual range is [0, 91] because the `Annual_Report_JUNE-2018.pdf` has 92 pages. The auto-profiler inferred the maximum from a sample that happened to miss long documents. This demonstrates that statistical profiling alone is insufficient for range constraints — domain knowledge must override sample statistics.

### Week 5 — Event Sourcing Platform Events

| Metric | Value |
|---|---|
| Total Checks | 88 |
| Passed | 86 |
| Failed | 2 |
| Warned | 0 |
| Errored | 0 |
| **Pass Rate** | **97.7%** |

**Violations found (both real, from actual domain model):**

1. **`aggregate_id.unique` — FAIL (HIGH):** Found 45 duplicate `aggregate_id` values. This is **by design** — the event sourcing pattern stores multiple events per aggregate (e.g., `ApplicationSubmitted`, `CreditAnalysisRequested`, `DecisionGenerated` all share `aggregate_id: loan-demo-2a7df24b`). The contract incorrectly assumed uniqueness. The fix is to remove the `unique: true` constraint on `aggregate_id` and instead enforce monotonic `sequence_number` per aggregate.

2. **`aggregate_id.format` — FAIL (HIGH):** All 65 records fail UUID format validation because the real domain model uses composite IDs like `loan-demo-2a7df24b` and `agent-session-demo-xxx`. These are human-readable, debuggable identifiers — a deliberate design choice from the Week 5 `LoanApplicationAggregate`. The contract should use a pattern match (`^(loan|agent-session|compliance|audit)-`) instead of UUID format.

### Injected Violation (from `extractions_violated.jsonl`)

The ValidationRunner was also run against an intentionally violated dataset where `confidence` was changed from float 0.0–1.0 to integer 0–100:

| Metric | Value |
|---|---|
| Total Checks | 56 |
| Failed | 3 |
| Key Failure | `extracted_facts[*].confidence.range`: max=98.8, mean=73.9 (expected max≤1.0) |
| Statistical Drift | 2,324σ deviation from baseline — unmissable |

The ViolationAttributor traced this to commit `cd5737c` ("feat: change confidence to percentage scale") in `src/week3/extractor.py`, with a blast radius of 178 affected records.

---

## 4. Reflection

Writing data contracts for my own five systems revealed four assumptions I never documented and one architectural gap I didn't know existed.

**Assumption 1: doc_id is unique.** It isn't. My Week 3 extraction ledger is an append-only log of extraction *attempts*, not a deduplicated output table. The same document gets processed multiple times with different strategies — `vision_augmented` for scanned images, `fast_text` for native digital PDFs. Each attempt produces a new ledger entry with the same `doc_id`. Any downstream consumer treating `doc_id` as a primary key would silently merge records from different strategies, potentially mixing a high-confidence vision extraction with a low-confidence fast-text fallback. The contract caught this immediately: 48 duplicates out of 64 records.

**Assumption 2: page_ref has a small range.** The auto-profiler inferred `max=15` from the sample, but the `Annual_Report_JUNE-2018.pdf` has 92 pages. I had never tested the extraction pipeline on documents longer than ~20 pages during development. The contract violation (331 facts outside range) revealed that my test corpus was not representative of production data. Domain knowledge — "a PDF can have hundreds of pages" — must override sample statistics.

**Assumption 3: aggregate_id is a UUID.** My Week 5 event store uses composite IDs like `loan-demo-2a7df24b` for debuggability. The canonical schema expects UUID format, but the real domain model chose human-readable prefixes so developers can identify aggregate types without joining metadata. The contract should use a pattern match, not UUID format.

**Assumption 4: all confidence scores are 0.0–1.0.** They are — today. But the extraction ledger stores `confidence_score: 0.8` as a float, and nothing in the Week 3 codebase prevents a future change to percentage scale (0–100). The contract's range check and statistical drift detection (which caught a 2,324σ deviation in the injected test) are the only guardrails. Without them, the Week 4 Cartographer would silently produce corrupted edge weights.

**The architectural gap:** There was no formal interface between any of my five systems. Week 3 outputs fed Week 4 inputs, but the schema was implicit — encoded in Python function signatures, not in a machine-checkable contract. The most valuable output is the blast radius report: knowing that a confidence field change affects 7 downstream nodes and 178 records transforms "it might break something" into "it breaks these specific things, and here is the commit that caused it."
