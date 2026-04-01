# Data Contract Enforcer — Interim Submission Report

**Author:** Amir Ahmedin
**Date:** 2026-04-01  
**Repository:** [github.com/IbnuEyni/10AcWeek7](https://github.com/IbnuEyni/10AcWeek7)  
**Week 7 TRP Submission — Thursday Interim**

---

## 1. Data Flow Diagram

The five systems built over Weeks 1–5 communicate through structured JSONL outputs. Each arrow is annotated with the **schema name** and the **specific fields** consumed by the downstream system. Solid arrows (──▶) represent data flow contracts enforced by the ValidationRunner. Dashed arrows (- -▶) represent contract enforcement flow (Week 7 components consuming data for validation, not production logic).

[![Diagram](flow_diagram.png)](flow_diagram.png)

**Diagram key:**

- **Green nodes** = Weeks 1, 2, 5 (data producers)
- **Blue nodes** = Weeks 3, 4 (data producers with critical downstream dependencies)
- **Yellow node** = LangSmith traces (derived from LLM usage across weeks)
- **Red nodes** = Week 7 enforcement components (consumers)
- **Solid arrows** = Production data flow contracts (enforced by ValidationRunner)
- **Dashed arrows** = Enforcement-only flow (Week 7 reads data for validation, not production logic)

**Data provenance:** All outputs are migrated from real Week 1–5 implementations using migration scripts (`migrate_week1.py` through `migrate_week5.py` + `migrate_traces.py`). Week 3 data comes from the actual extraction ledger (64 documents including Ethiopian financial reports, CBE annual reports, and CPI indices processed via `vision_augmented` and `fast_text` strategies). Week 4 data comes from real lineage graphs of dbt-core, jaffle-shop, and ol-data-platform codebases (179 nodes, 65 edges in the primary graph). Week 5 events are generated from the real `LoanApplication` domain model defined in `10Acweek5/ledger/src/models/events.py`, using the actual `EVENT_CATALOGUE` with 16 event types.

---

## 2. Contract Coverage Table

| # | Interface | Contract? | Structural | Statistical | Cross-field | Rationale / Gap |
|:--|:----------|:----------|:-----------|:------------|:------------|:----------------|
| 1 | W1→W2: `code_refs[].file` as `target_ref` | **Partial** | 15: UUID intent_id, ISO 8601, confidence 0.0–1.0 | 4: confidence baseline, cardinality | 0 | **Gap:** No cross-system join. Each side validated independently. |
| 2 | W2→W7: `verdict_record` → AI Extensions | **Yes** | 12: verdict ∈ {PASS,FAIL,WARN}, score 1–5, rubric SHA-256 | 3: score dist, confidence baseline | 1: overall_score = weighted mean | Full. AI Extensions track output violation rate. |
| 3 | W3→W4: `extraction_record` → Cartographer | **Yes** | 16: confidence 0.0–1.0 ⚠️BREAKING, SHA-256, model `^(claude\|gpt)-` | 5: confidence drift, proc_time, tokens | 2: entity_refs ⊆ entities[].id, fact_id unique | **Highest-risk.** Confidence corrupts edge weights silently. |
| 4 | W4→W7: `lineage_snapshot` → Attributor | **Yes** | 14: git_commit 40-hex, node.type ∈ 6, edge.rel ∈ 6 | 2: node/edge count baselines | 2: edge.source ∈ nodes[].node_id, edge.target ∈ nodes[].node_id | Full. Graph integrity prevents dangling refs. |
| 5 | W5→W7: `event_record` → Schema Validation | **Partial** | 12: recorded_at ≥ occurred_at, PascalCase, UUID event_id | 3: seq_number monotonic, payload size | 0 | **Gap:** Per-event-type payload JSON Schema not implemented. Envelope only. |
| 6 | Traces→W7: `trace_record` → AI Extensions | **Yes** | 12: end > start, run_type ∈ 5, UUID id, cost ≥ 0 | 3: total_tokens baseline, cost baseline | 1: total_tokens = prompt + completion | Full. |

**Coverage: 4/6 full, 2/6 partial.** The two partial contracts (Week 1→2 and Week 5→7) have identified gaps with specific remediation plans. Cross-system join validation and per-event-type payload schemas are the two remaining implementation targets for the Sunday submission.

---

## 3. First Validation Run Results

**Severity framework used:**

- **CRITICAL** = Structural or type violation (column missing, wrong type)
- **HIGH** = Statistical drift > 3σ from baseline, or uniqueness/format violation
- **MEDIUM** = Statistical drift 2–3σ
- **LOW** = Informational (check passed)
- **WARNING** = Near-threshold value

### 3.1 Week 3 — Document Refinery Extractions

| Metric        | Value     |
| ------------- | --------- |
| Total Checks  | 57        |
| Passed        | 55        |
| Failed        | 2         |
| Warned        | 0         |
| Errored       | 0         |
| **Pass Rate** | **96.5%** |

**Violation 1 — `doc_id.unique` (HIGH):**

Found 48 duplicate `doc_id` values out of 64 records. Root cause: the extraction ledger (`10AcWeek3/.refinery/extraction_ledger.jsonl`) is an append-only log of extraction _attempts_. The same document (e.g., `2018_Audited_Financial_Statement_Report.pdf`) was processed multiple times with different strategies (`vision_augmented`, `fast_text`, `layout_aware`). Each re-extraction appends a new entry with the same `doc_id`.

_Impact:_ Any downstream consumer treating `doc_id` as a primary key would silently merge records from different extraction strategies, potentially mixing a high-confidence vision extraction with a low-confidence fast-text fallback.

**Contract correction:**

```yaml
# BEFORE (incorrect)
doc_id:
  type: string
  format: uuid
  required: true
  unique: true  # ← WRONG: ledger is append-only

# AFTER (corrected)
doc_id:
  type: string
  format: uuid
  required: true
  unique: false
  description: >
    Document identifier. NOT unique in the extraction ledger —
    multiple extraction attempts per document are expected.
    Use (doc_id, extraction_model) as composite key.
```

**Violation 2 — `extracted_facts[*].page_ref.range` (CRITICAL):**

331 fact records have `page_ref` values outside the auto-inferred range [0, 15]. Actual range is [0, 91] because `Annual_Report_JUNE-2018.pdf` has 92 pages. The auto-profiler inferred the maximum from a sample that missed long documents.

_Impact:_ Any consumer filtering facts by page range would silently drop facts from long documents.

**Contract correction:**

```yaml
# BEFORE (auto-inferred, too tight)
extracted_facts[*].page_ref:
  type: integer
  minimum: 0.0
  maximum: 15.0

# AFTER (domain-aware)
extracted_facts[*].page_ref:
  type: integer
  minimum: 0
  maximum: 10000  # PDFs can have thousands of pages
  description: >
    Zero-indexed page number. Nullable for facts not tied to a specific page.
    Domain constraint: must not exceed source document page count.
```

### 3.2 Week 5 — Event Sourcing Platform Events

| Metric        | Value     |
| ------------- | --------- |
| Total Checks  | 88        |
| Passed        | 86        |
| Failed        | 2         |
| Warned        | 0         |
| Errored       | 0         |
| **Pass Rate** | **97.7%** |

**Violation 1 — `aggregate_id.unique` (HIGH):**

Found 45 duplicate `aggregate_id` values. This is **by design** — the event sourcing pattern stores multiple events per aggregate (e.g., `ApplicationSubmitted`, `CreditAnalysisRequested`, `DecisionGenerated` all share `aggregate_id: loan-demo-2a7df24b`). The contract incorrectly assumed uniqueness.

**Contract correction:**

```yaml
# BEFORE (incorrect)
aggregate_id:
  type: string
  format: uuid
  unique: true  # ← WRONG: multiple events per aggregate

# AFTER (corrected)
aggregate_id:
  type: string
  pattern: "^(loan|agent-session|compliance|audit)-"
  required: true
  unique: false
  description: >
    Aggregate identifier. NOT unique — multiple events per aggregate.
    Enforce monotonic sequence_number per aggregate_id instead.
```

**Violation 2 — `aggregate_id.format` (HIGH):**

All 65 records fail UUID format validation. The real domain model uses composite IDs like `loan-demo-2a7df24b` and `agent-session-demo-xxx` — human-readable prefixes for debuggability, a deliberate design choice from `LoanApplicationAggregate`.

_Contract correction:_ Same as above — replace `format: uuid` with `pattern: "^(loan|agent-session|compliance|audit)-"`.

### 3.3 Injected Violation — Confidence Scale Change

The ValidationRunner was run against `extractions_violated.jsonl` where `confidence` was intentionally changed from float 0.0–1.0 to integer 0–100:

| Metric       | Value |
| ------------ | ----- |
| Total Checks | 56    |
| Failed       | 3     |

**Key failures:**

| Check                                 | Status   | Actual                         | Expected         |
| ------------------------------------- | -------- | ------------------------------ | ---------------- |
| `extracted_facts[*].confidence.range` | **FAIL** | max=98.8, mean=73.9            | max≤1.0, min≥0.0 |
| `extracted_facts[*].confidence.drift` | **FAIL** | 2,324σ deviation from baseline | within 3σ        |
| `extracted_facts[*].page_ref.range`   | **FAIL** | max=20.0 (different sample)    | max≤15.0         |

**Validation report JSON excerpt** (from `validation_reports/week3_violated.json`):

```json
{
  "check_id": "week3-document-refinery-extractions.extracted_facts[*].confidence.range",
  "column_name": "extracted_facts[*].confidence",
  "check_type": "range",
  "status": "FAIL",
  "actual_value": "min=50.0, max=98.8, mean=73.9253",
  "expected": "min>=0.0, max<=1.0",
  "severity": "CRITICAL",
  "records_failing": 178,
  "message": "confidence is in 0–100 range, not 0.0–1.0. Breaking change detected."
}
```

The ViolationAttributor traced this to commit `cd5737c` ("feat: change confidence to percentage scale") in `src/week3/extractor.py`, with a blast radius of 178 affected records across 7 downstream nodes.

---

## 4. Reflection

Writing data contracts for my own five systems revealed four wrong assumptions and one architectural gap. More importantly, it changed how I will build systems going forward.

**Assumption 1: doc_id is unique.** It isn't. My Week 3 extraction ledger is an append-only log of extraction _attempts_, not a deduplicated output table. The same document gets processed multiple times with different strategies. The contract caught 48 duplicates out of 64 records. The fix isn't just updating the contract — it's deciding whether the ledger should be deduplicated before feeding Week 4, or whether Week 4 should handle duplicates. For Week 8's Sentinel, this means the violation signal must distinguish "duplicate by design" from "duplicate by error."

**Assumption 2: page_ref has a small range.** The auto-profiler inferred `max=15`, but `Annual_Report_JUNE-2018.pdf` has 92 pages. I never tested on documents longer than ~20 pages during development. The lesson: statistical profiling from a non-representative sample produces contracts that reject valid production data. Domain knowledge must override sample statistics.

**Assumption 3: aggregate_id is a UUID.** My Week 5 event store uses composite IDs like `loan-demo-2a7df24b` for debuggability. The contract should use a pattern match, not UUID format. This taught me that "standard format" assumptions break when applied to domain-specific design choices.

**Assumption 4: confidence will always be 0.0–1.0.** It is today, but nothing prevents a future change. The injected test proved the contract catches this at 2,324σ — but only because I wrote the contract. Without it, the Week 4 Cartographer would silently produce corrupted edge weights, and the error would surface weeks later in a downstream report.

**The architectural gap:** There was no formal interface between any of my five systems. The schema was implicit — encoded in Python function signatures, not in a machine-checkable contract. Going forward, I will write the contract _before_ the producer code, not after. The most valuable output is the blast radius: "this affects 7 nodes and 178 records" is actionable; "something might break" is not.
