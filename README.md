# Data Contract Enforcer — Week 7

Schema Integrity & Lineage Attribution System for a five-system AI platform (Weeks 1–5).

## Setup

```bash
pip install pandas numpy pyyaml
# Optional: pip install anthropic   (enables LLM annotation in ContractGenerator)
```

All commands are run from the repo root.

---

## 1. ContractGenerator

Profiles JSONL data and auto-generates Bitol-compatible YAML contracts + dbt schema.yml files.

```bash
# Generate contract for a single source
python contracts/generator.py --source outputs/week3/extractions.jsonl --output generated_contracts/

# Generate contracts for all six sources
python contracts/generator.py --all
```

**Expected output:**
```
Profiling 64 records from outputs/week3/extractions.jsonl...
  Wrote contract: generated_contracts/week3_extractions.yaml
  Wrote dbt schema: generated_contracts/week3_extractions_dbt.yml
  Wrote schema snapshot: schema_snapshots/week3-document-refinery-extractions/<timestamp>.yaml
Contract generation complete.
```

Generated files: `generated_contracts/week3_extractions.yaml` (20+ schema fields, quality checks, lineage section), `generated_contracts/week3_extractions_dbt.yml`.

---

## 2. ValidationRunner

Executes all contract checks against a data snapshot and produces a structured JSON report.

```bash
# Validate clean data
python contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions.jsonl

# Validate violated data (confidence changed to 0-100 scale — injected breaking change)
python contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions_violated.jsonl \
  --output validation_reports/week3_violated.json

# With quarantine mode (writes failing records to outputs/quarantine/)
python contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions_violated.jsonl \
  --quarantine
```

**Expected output (violated data):**
```
Validation report written to: validation_reports/week3-document-refinery-extractions_<timestamp>.json
  Total checks: 28
  Passed: 24
  Failed: 3
  Warned: 0
  Errored: 1
  [FAIL] week3-document-refinery-extractions.extracted_facts[*].confidence.range: ...confidence has 178 values outside [0.0, 1.0]...
  [FAIL] week3-document-refinery-extractions.extracted_facts[*].confidence.drift: Statistical drift detected...
```

Report schema: `{ report_id, contract_id, snapshot_id, run_timestamp, total_checks, passed, failed, warned, errored, results[] }`.

---

## 3. ViolationAttributor

Traces a validation failure to the upstream git commit that caused it, using the Week 4 lineage graph and the contract registry for blast radius.

```bash
python contracts/attributor.py --violation-report validation_reports/week3_violated.json
```

**Expected output:**
```
Attributing: week3-document-refinery-extractions.extracted_facts[*].confidence.range...
  Blame chain: 2 candidates
  Blast radius: 0 affected nodes

--- Violation: week3-document-refinery-extractions.extracted_facts[*].confidence.range ---
  #1 src/week3/extractor.py | cd5737c01914 | developer@example.com | conf=0.05
       "feat: change confidence to percentage scale"
  Blast radius: 0 nodes, 178 records
```

Violations are appended to `violation_log/violations.jsonl`. Each entry includes `blame_chain[]` with ranked candidates, `blast_radius.registry_subscribers` (from `contract_registry/subscriptions.yaml`), and `blast_radius.affected_nodes` (from lineage graph enrichment).

---

## 4. SchemaEvolutionAnalyzer

Diffs two schema snapshots, classifies every change using the backward/forward/full compatibility taxonomy, and generates a migration impact report.

```bash
# Auto-diff the two most recent snapshots for a contract
python contracts/schema_analyzer.py --contract-id week3-document-refinery-extractions

# Diff two specific snapshots
python contracts/schema_analyzer.py \
  --snapshot-a schema_snapshots/week3-document-refinery-extractions/20260401_200810.yaml \
  --snapshot-b schema_snapshots/week3-document-refinery-extractions/20260401_204732.yaml
```

**Expected output:**
```
Schema evolution report: validation_reports/schema_evolution_week3-document-refinery-extractions_<timestamp>.json
  Compatibility: BREAKING
  Total changes: 3
  Breaking: 2
  Compatible: 1
  [✗ BREAKING] type_narrowing: extracted_facts[*].confidence (number → integer)
  [✗ BREAKING] add_required_column: extraction_version (None → string)
  [✓] stats_shift: extracted_facts[*].confidence (mean=0.8000 → mean=73.5000)

  Migration checklist:
    1. Address type_narrowing on column 'extracted_facts[*].confidence'
    2. Address add_required_column on column 'extraction_version'
    3. Run ValidationRunner on all downstream consumers
    4. Update schema snapshots and baselines
```

---

## 5. AI Contract Extensions

Runs three AI-specific contract checks: embedding drift detection, prompt input schema validation, and LLM output schema violation rate.

```bash
# Run all three extensions
python contracts/ai_extensions.py --all

# Individual extensions
python contracts/ai_extensions.py --embedding-drift outputs/week3/extractions.jsonl
python contracts/ai_extensions.py --prompt-validation outputs/week3/extractions.jsonl
python contracts/ai_extensions.py --output-schema outputs/week2/verdicts.jsonl
```

**Expected output:**
```
Extension 1: Embedding drift on Week 3 extractions...
  PASS: drift=0.0000
Extension 2: Prompt input schema validation on Week 3...
  PASS: 0/64 records failed prompt input validation
Extension 3: LLM output schema enforcement on Week 2 verdicts...
  PASS: 0/16 LLM outputs failed schema validation (trend: stable)

AI metrics written to: validation_reports/ai_metrics.json
```

Metrics written to `validation_reports/ai_metrics.json`. Non-conforming prompt input records are quarantined to `outputs/quarantine/` — never silently dropped.

---

## 6. ReportGenerator

Auto-generates the Enforcer Report from live validation data, violation log, and AI metrics.

```bash
python contracts/report_generator.py
```

**Expected output:**
```
Enforcer Report generated: enforcer_report/report_data.json
  Data Health Score: 60/100
  Total checks: 401, Passed: 388, Failed: 13
  Violations logged: 5
  Top 1: [CRITICAL] week3-document-refinery-extractions.extracted_facts[*].page_ref.range
  Top 2: [CRITICAL] week3-document-refinery-extractions.extracted_facts[*].page_ref.range
  Top 3: [CRITICAL] week3-document-refinery-extractions.extracted_facts[*].confidence.range
```

Report written to `enforcer_report/report_data.json`. Contains: `data_health_score` (0–100), `violations_this_week`, `schema_changes_detected`, `ai_system_risk_assessment`, and `recommended_actions` in plain language.

---

## Directory Structure

```
contracts/           # Five entry-point scripts
generated_contracts/ # Auto-generated Bitol YAML + dbt schema.yml
contract_registry/   # subscriptions.yaml — inter-system dependency registry
outputs/             # Input JSONL data (weeks 1–5 + traces)
validation_reports/  # Structured validation report JSON
violation_log/       # violations.jsonl — blame chain + blast radius records
schema_snapshots/    # Timestamped schema snapshots per contract + baselines.json
enforcer_report/     # report_data.json + PDF report
DOMAIN_NOTES.md      # Phase 0 domain reconnaissance
```

## Injected Violation

`outputs/week3/extractions_violated.jsonl` contains a copy of the Week 3 data with `extracted_facts[*].confidence` changed from float `0.0–1.0` to integer `0–100`. Running the ValidationRunner against this file demonstrates detection of the canonical breaking change described in the project spec. The injection is documented at the top of `violation_log/violations.jsonl`.
