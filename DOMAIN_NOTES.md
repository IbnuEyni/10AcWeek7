# DOMAIN_NOTES.md — Data Contract Enforcer

## Core Concepts Mastered

### Data Contracts — Three Dimensions
A data contract is a formal specification of what a dataset promises to provide. It operates across three dimensions:

- **Structural**: Column names, types, nullability, uniqueness. Example from my Week 3 extraction_record: `doc_id` is type `string`, format `uuid`, required `true`, unique `true`. The structural dimension is what JSON Schema and dbt's `not_null`/`unique` tests enforce.
- **Statistical**: Value ranges, distribution shapes, cardinality. Example: `extracted_facts[*].confidence` has baseline mean=0.80, stddev=0.0, min=0.80, max=0.80 (all documents in my extraction ledger were processed with `confidence_score: 0.8` by the vision_augmented strategy). A shift to mean=73.9 is a statistical violation even if the type remains `number`.
- **Temporal**: Freshness SLA, update frequency. Example: Week 5 event_records enforce `recorded_at >= occurred_at` — a temporal ordering contract. If events arrive with `recorded_at` before `occurred_at`, the causal chain is broken.

The Bitol Open Data Contract Standard (bitol-io/open-data-contract-standard) formalises these three dimensions into a single YAML specification with `schema`, `quality`, and `lineage` sections. My ContractGenerator outputs this format directly.

### Schema Evolution Taxonomy — Confluent Model
The Confluent Schema Registry defines three compatibility modes:

- **Backward compatible**: New schema can read data written by old schema. Consumers can upgrade first. Example: adding a nullable `governance_tags` field to Week 1 intent_records.
- **Forward compatible**: Old schema can read data written by new schema. Producers can upgrade first. Example: a consumer that ignores unknown fields can handle a new `extraction_version` column in Week 3.
- **Full compatible**: Both backward and forward compatible simultaneously. The safest but most restrictive mode.

My SchemaEvolutionAnalyzer implements a subset: it classifies every detected change using the taxonomy table (add_nullable_column, add_required_column, remove_column, rename_column, type_widening, type_narrowing, enum_addition, enum_removal) and produces a compatibility verdict of BREAKING or COMPATIBLE.

### dbt Test Architecture
dbt's schema tests are the most widely-deployed contract enforcement in practice. The mapping from contract clauses to dbt tests:

| Contract Clause | dbt Test | My Example |
|---|---|---|
| `required: true` | `not_null` | `doc_id` in Week 3 extractions |
| `unique: true` | `unique` | `intent_id` in Week 1 records |
| `enum: [PASS, FAIL, WARN]` | `accepted_values` | `overall_verdict` in Week 2 verdicts |
| `entity_refs ⊆ entities` | `relationships` | `extracted_facts[*].entity_refs` → `entities[*].entity_id` in Week 3 |

My ContractGenerator outputs a parallel `{name}_dbt.yml` for every contract YAML, with these test definitions auto-generated from the contract schema.

### AI-Specific Contract Extensions
Standard data contracts cover tabular data. AI systems introduce three new contract requirements that no existing framework handles:

1. **Embedding drift detection**: The semantic meaning of text inputs can shift even when structural checks pass. My `ai_extensions.py` embeds a sample of `extracted_facts[*].text` values, computes a centroid, and measures cosine distance from baseline.
2. **Prompt input schema validation**: Structured data interpolated into prompts must conform to a JSON Schema before entering the LLM. Non-conforming records are quarantined, not silently dropped.
3. **Structured LLM output enforcement**: Week 2 verdict records are structured LLM outputs. The `output_schema_violation_rate` metric tracks how often the LLM returns malformed JSON — a rising rate signals prompt degradation.

### Statistical vs. Structural Violations
A column renamed from `confidence` to `confidence_score` is a **structural violation** — the column is missing, the check fails immediately, the error message is clear.

A column whose mean shifts from 0.80 to 73.9 because someone changed the scale from 0.0–1.0 to 0–100 is a **statistical violation**. The column still exists, the type is still `number`, and every structural check passes. But the data is wrong. My ValidationRunner catches this with baseline-relative drift detection: it stores the first-run mean and stddev in `schema_snapshots/baselines.json` and emits FAIL when the current mean deviates by more than 3σ. In my test, the confidence change triggered a 2324σ deviation — unmissable.

---

## Question 1: Backward-Compatible vs. Breaking Schema Changes

A **backward-compatible** change allows existing consumers to continue reading data without modification. A **breaking** change forces downstream consumers to update or fail silently.

### Backward-Compatible Examples (from my systems)

1. **Adding `governance_tags` to Week 1 intent_records**: The original Roo-Code `agent_trace.jsonl` had no governance tags. The migration script added `governance_tags` as a new nullable array derived from `toolName` (e.g., `write_to_file` → `["mutation", "code-gen"]`). Week 2's courtroom, which consumes intent data to evaluate code references via `code_refs[].file`, can safely ignore this new field — it never read it before. Under the Confluent model, this is backward compatible because the new schema can still read old data (the field is nullable).

2. **Widening `processing_time_ms` from int to float in Week 3**: The extraction ledger originally stored `processing_time_ms` as a float like `0.25` (seconds). The canonical schema expects an integer (milliseconds). My migration script converts `0.25` → `250`. Widening from int to float is safe under the Confluent model — any consumer doing `value > threshold` still works because float subsumes int. No downstream consumer breaks.

3. **Adding `"EXTERNAL"` to Week 4 node types**: The original cartographer lineage graph from `10AcWeek4/.cartography/lineage_graph.json` used `node_type: "module"` and `"function"`. My migration added `"EXTERNAL"` for nodes like `os.py` that are referenced by imports but live outside the codebase. This is an additive enum change — existing consumers that filter on `"module"` or `"function"` simply never encounter the new value. Under the Confluent model, additive enum changes are backward compatible.

### Breaking Examples (from my systems)

1. **Week 3 `confidence` float 0.0–1.0 → integer 0–100**: This is the canonical breaking change. My extraction ledger stores `confidence_score: 0.8`. If an update changed this to `confidence_score: 80`, the Week 4 Cartographer — which uses confidence to weight lineage edges — would treat `80` as an impossibly high confidence, corrupting all downstream graph traversals. The ValidationRunner catches this: `extracted_facts[*].confidence` range check fails with `max=98.8, mean=73.9` against the contract clause `max<=1.0`. Under the Confluent model, this is a type narrowing (float → int) combined with a semantic scale change — doubly breaking.

2. **Renaming `doc_id` to `document_id` in Week 3**: The Week 4 Cartographer references `doc_id` to create lineage nodes (`table::extractions`). My migration script uses `uuid.uuid5(uuid.NAMESPACE_DNS, doc_id)` to generate stable UUIDs from the original `doc_id` string. A rename to `document_id` breaks the foreign key relationship silently — the Cartographer would create nodes with null document references, and no error would be raised because the field simply wouldn't exist. The ValidationRunner catches this as an ERROR status: "column not found."

3. **Removing `metadata.causation_id` from Week 5 events**: My Week 5 event sourcing platform (from `10Acweek5/ledger/src/models/events.py`) uses `causation_id` in the `BaseEvent.metadata` dict to build causal chains between events. The `ComplianceAuditViewProjection` traces decision lineage through these causation chains — from `ApplicationSubmitted` through `CreditAnalysisCompleted` to `DecisionGenerated`. Removing `causation_id` breaks this projection silently: it would produce incomplete audit trails where compliance decisions appear disconnected from their triggering events. Under the Confluent model, column removal is never backward compatible.

---

## Question 2: Confidence Field Failure Trace

### The Scenario
The Week 3 Document Refinery's `extracted_facts[*].confidence` field is defined as `float 0.0–1.0`. An update changes it to `integer 0–100`.

### Failure Propagation to Week 4 Cartographer
My Week 4 Cartographer (`10AcWeek4/src/agents/semanticist.py`) ingests Week 3 extraction records and uses `confidence` to:
- Weight edges in the lineage graph (`edge.confidence = fact.confidence`)
- Filter low-confidence facts (`if confidence < 0.6: skip`)
- Generate the `onboarding_brief.md` with quality assessments

When confidence becomes `80` instead of `0.80`:
1. **Filter bypass**: The filter `confidence < 0.6` passes (80 > 0.6), so no facts are filtered — even garbage facts with confidence `50` (which should be `0.50`, a borderline score) are included in the lineage graph.
2. **Edge weight corruption**: Edge weights become `80.0` instead of `0.80`, breaking any normalized graph algorithm (PageRank, shortest path) that assumes weights in [0, 1]. The `knowledge_graph.py` module's `pagerank` computation produces nonsensical results.
3. **False positive reporting**: The Cartographer's `onboarding_brief.md` reports "all extractions high confidence" — a false positive that masks real quality issues.
4. **Silent propagation**: No exception is raised. The pipeline completes successfully. The output is wrong. This is the most dangerous class of failure.

### The Data Contract Clause (Bitol YAML)

```yaml
kind: DataContract
apiVersion: v3.0.0
id: week3-confidence-guard
info:
  title: Week 3 Confidence Field Contract
  version: 1.0.0
  owner: week3-team
  description: >
    Guards the extracted_facts.confidence field against scale changes.
    This field is consumed by Week 4 Cartographer for edge weighting.
servers:
  local:
    type: local
    path: outputs/week3/extractions.jsonl
    format: jsonl
terms:
  usage: Internal inter-system data contract.
  limitations: confidence must remain in 0.0-1.0 float range.
schema:
  extracted_facts:
    type: array
    items:
      confidence:
        type: number
        minimum: 0.0
        maximum: 1.0
        required: true
        description: >
          Confidence score. MUST be float 0.0-1.0.
          BREAKING CHANGE if changed to 0-100.
          Consumed by Week 4 Cartographer for edge.confidence weighting.
quality:
  type: SodaChecks
  specification:
    checks for extractions:
      - min(extracted_facts.confidence) >= 0.0
      - max(extracted_facts.confidence) <= 1.0
      - avg(extracted_facts.confidence) between 0.3 and 0.99
      - row_count >= 1
lineage:
  upstream: []
  downstream:
    - id: week4-brownfield-cartographer
      description: Cartographer uses confidence for edge weighting and fact filtering
      fields_consumed: [extracted_facts.confidence]
      breaking_if_changed: [extracted_facts.confidence]
```

This clause catches the change in three ways:
1. **Structural**: The `maximum: 1.0` check fails immediately when values exceed 1.0.
2. **Statistical**: The drift check (baseline mean ~0.80, new mean ~73.9) triggers a FAIL at >3σ deviation (actual: 2324σ).
3. **Quality**: The `avg(extracted_facts.confidence) between 0.3 and 0.99` check fails because the new mean is 73.9.

---

## Question 3: Blame Chain Construction via Lineage Graph

When the ValidationRunner detects a contract violation, the ViolationAttributor constructs a blame chain through these specific steps:

### Step 1 — Identify the Failing Schema Element
The violation report contains `check_id: "week3-document-refinery-extractions.extracted_facts[*].confidence.range"`. This identifies the failing column as `extracted_facts[*].confidence` in the Week 3 extraction output. The report also contains `records_failing: 178` and `actual_value: "max=98.8, mean=73.9"`.

### Step 2 — Map to Lineage Node
The attributor maps the contract prefix `week3` to candidate lineage nodes using a static mapping:
```python
node_mapping = {
    "week3": ["table::extractions", "service::week3-refinery", "file::src/week3/extractor.py"],
}
```
These are the nodes in the Week 4 lineage graph that produce the failing data. In my actual lineage graph (from `10AcWeek4/.cartography/lineage_graph.json`, 179 nodes, 65 edges), the relevant nodes include `file::src/week3/extractor.py` and the downstream consumers.

### Step 3 — BFS Upstream Traversal
The attributor builds a **reverse adjacency list** from the lineage graph's edges:
```python
reverse_adj = {}
for edge in graph["edges"]:
    tgt = edge["target"]
    src = edge["source"]
    reverse_adj.setdefault(tgt, []).append({"node_id": src, "relationship": edge["relationship"]})
```

Starting from `table::extractions`, it performs breadth-first search on this reverse adjacency:
- **Hop 0**: `table::extractions` (starting node)
- **Hop 1**: `service::week3-refinery` ← PRODUCES `table::extractions`
- **Hop 2**: `file::src/week3/extractor.py` ← WRITES `service::week3-refinery`
- **Hop 3**: `file::src/utils/fact_extractor.py` ← IMPORTS `file::src/week3/extractor.py`

The traversal uses a `visited` set to prevent cycles and stops at the first external boundary or file-system root. Maximum depth is unbounded but practically limited by the graph structure.

### Step 4 — Git Blame Integration
For each upstream file identified, the attributor shells out to git:
```bash
git log --follow --since="14 days ago" --format='%H|%an|%ae|%ai|%s' -- src/week3/extractor.py
```
This returns commits that modified the file. For the confidence change, it finds:
- `cd5737c` — `"feat: change confidence to percentage scale"` — `developer@example.com` — `2025-01-14 09:00:00`
- `3b82515` — `"feat: initial project setup with sample data and source files"` — `developer@example.com` — `2025-01-10 09:00:00`

For targeted line-level blame, it can also run:
```bash
git blame -L 7,8 --porcelain src/week3/extractor.py
```
This identifies the exact lines where `confidence` was changed from `0.0-1.0` to `0-100`.

### Step 5 — Confidence Scoring
Each candidate gets a score using the formula:
```
confidence = 1.0 - (days_since_commit × 0.1) - (hops × 0.2)
```
Clamped to `[0.05, 1.0]`. Never fewer than 1 candidate, never more than 5.

- Commit `cd5737c` (1 day ago, hop 0): `1.0 - 0.1 - 0.0 = 0.90` — highest confidence
- Commit `3b82515` (5 days ago, hop 0): `1.0 - 0.5 - 0.0 = 0.50` — lower confidence

The candidates are sorted by confidence descending. The top candidate is the most likely cause.

### Step 6 — Blast Radius (Downstream BFS)
From the failing node, the attributor traverses **forward** through the lineage graph's forward adjacency to find all affected downstream consumers:
```python
adj = {}
for edge in graph["edges"]:
    adj.setdefault(edge["source"], []).append(edge["target"])
```
BFS from `table::extractions` yields: `service::week4-cartographer`, `file::src/week4/cartographer.py`, and any nodes they feed into. The blast radius report includes:
- `affected_nodes`: list of downstream node IDs
- `affected_pipelines`: inferred pipeline names from service nodes
- `estimated_records`: count of failing records from the validation report (178)

### Output
The complete blame chain is written to `violation_log/violations.jsonl`:
```json
{
  "violation_id": "uuid-v4",
  "check_id": "week3-document-refinery-extractions.extracted_facts[*].confidence.range",
  "detected_at": "ISO 8601",
  "blame_chain": [
    {
      "rank": 1,
      "file_path": "src/week3/extractor.py",
      "commit_hash": "cd5737c...",
      "author": "developer@example.com",
      "commit_message": "feat: change confidence to percentage scale",
      "confidence_score": 0.90
    }
  ],
  "blast_radius": {
    "affected_nodes": ["service::week4-cartographer"],
    "affected_pipelines": ["week4-cartographer-pipeline"],
    "estimated_records": 178
  }
}
```

---

## Question 4: LangSmith Trace Record Data Contract

```yaml
kind: DataContract
apiVersion: v3.0.0
id: langsmith-trace-records
info:
  title: LangSmith Trace Export — Run Records
  version: 1.0.0
  owner: platform-team
  description: >
    One record per LLM/chain/tool run. Contains token counts,
    costs, and timing for all AI operations across Weeks 1-5.
    Derived from real Week 2 audit runs (14 streamlit sessions),
    Week 3 extraction pipeline (64 document extractions), and
    Week 4 cartography traces (12 agent actions).
servers:
  local:
    type: local
    path: outputs/traces/runs.jsonl
    format: jsonl
terms:
  usage: Internal inter-system data contract. Do not publish.
  limitations: >
    total_tokens must equal prompt_tokens + completion_tokens.
    total_cost must be non-negative. end_time must be after start_time.

# ── STRUCTURAL CLAUSES ──
schema:
  id:
    type: string
    format: uuid
    required: true
    unique: true
    description: Unique run identifier. UUIDv4.
  name:
    type: string
    required: true
    description: Chain or LLM name (e.g., "judge_prosecutor", "extraction_vision_augmented").
  run_type:
    type: string
    required: true
    enum: [llm, chain, tool, retriever, embedding]
    description: Type of run. Must be one of the five allowed values.
  inputs:
    type: object
    required: true
  outputs:
    type: object
    required: true
  error:
    type: string
    required: false
    description: Error message if run failed. Null for successful runs.
  start_time:
    type: string
    format: iso8601
    required: true
  end_time:
    type: string
    format: iso8601
    required: true
    description: Must be strictly after start_time.
  total_tokens:
    type: integer
    required: true
    minimum: 0
    description: Must equal prompt_tokens + completion_tokens.
  prompt_tokens:
    type: integer
    required: true
    minimum: 0
  completion_tokens:
    type: integer
    required: true
    minimum: 0
  total_cost:
    type: number
    minimum: 0.0
    required: true
    description: Cost in USD. Must be non-negative.
  tags:
    type: array
    required: true
    description: Tags identifying the source week and operation type.
  parent_run_id:
    type: string
    format: uuid
    required: false
    description: Parent run ID for nested chains. Null for top-level runs.
  session_id:
    type: string
    format: uuid
    required: true

# ── STATISTICAL CLAUSE ──
quality:
  type: SodaChecks
  specification:
    checks for traces:
      - avg(total_tokens) between 500 and 20000
      - avg(total_cost) between 0.001 and 0.10
      - max(total_cost) < 1.0
      - stddev(total_tokens) < 10000
      - row_count >= 1
      - missing_count(id) = 0
      - duplicate_count(id) = 0

# ── AI-SPECIFIC CLAUSES ──
ai_extensions:
  embedding_drift:
    description: >
      Monitor semantic drift in LLM inputs across runs.
      Embed a random sample of 200 input text values using
      text-embedding-3-small. Store the centroid vector in
      schema_snapshots/embedding_baselines.npz. On each subsequent
      run, embed a fresh sample and compute cosine distance from
      the stored centroid. If distance exceeds 0.15, trigger WARN.
      This detects prompt template changes or input distribution
      shifts that may degrade model performance.
    metric: cosine_distance_from_baseline_centroid
    threshold: 0.15
    sample_size: 200
    baseline_path: schema_snapshots/embedding_baselines.npz
    applies_to: inputs
  output_schema_enforcement:
    description: >
      Track the output_schema_violation_rate metric per prompt version.
      A rising rate signals prompt degradation or model behaviour change.
      Validate every LLM response against the expected output JSON Schema.
      Write violations to violation_log/ as type = "llm_output_schema".
    metric: output_schema_violation_rate
    warn_threshold: 0.05
    fail_threshold: 0.15
    trend_detection: true
    applies_to: outputs
  token_budget:
    description: >
      Track token consumption trends. A rising mean of total_tokens
      per run_type=llm indicates prompt bloat or model behavior change.
      Trigger WARN if mean increases by >20% from baseline.
    baseline_metric: mean(total_tokens) where run_type='llm'
    warn_threshold_pct: 20

lineage:
  upstream:
    - id: week2-digital-courtroom
      description: >
        Verdict generation produces LLM traces. 14 audit sessions
        with 5 judge LLM calls each = 70 child traces.
    - id: week3-document-refinery
      description: >
        Extraction pipeline produces LLM traces. 64 document
        extractions, primarily via vision_augmented strategy.
    - id: week4-brownfield-cartographer
      description: >
        Cartography agent actions produce 12 trace records
        from Orchestrator, Surveyor, and Semanticist agents.
  downstream:
    - id: week7-ai-contract-extensions
      description: AI extensions consume traces for drift detection and cost monitoring
      fields_consumed: [total_tokens, total_cost, run_type, start_time, end_time, inputs, outputs]
      breaking_if_changed: [total_tokens, run_type, total_cost]
    - id: week8-sentinel
      description: Week 8 Sentinel consumes trace quality signals for alerting
      fields_consumed: [total_tokens, total_cost, error, run_type]
      breaking_if_changed: [total_tokens, error]
```

---

## Question 5: Why Contract Enforcement Systems Fail

The most common failure mode is **contract staleness** — contracts are written once and never updated as the underlying data evolves.

### Why Contracts Get Stale

1. **No automated enforcement loop**: Contracts exist as documentation but are not executed on every data pipeline run. When a schema changes, the contract YAML sits unchanged in a repo while the data silently diverges. In my own system, the Week 3 extraction ledger originally stored `processing_time_ms` as a float in seconds (`0.25`). The canonical schema expects an integer in milliseconds (`250`). Without automated validation, this mismatch would persist indefinitely — the contract says "integer" but the data says "float", and nobody checks.

2. **Ownership ambiguity**: The producer team writes the contract, but the consumer team depends on it. Neither team owns the enforcement step. In my platform, "past me" (Week 3) is the producer and "present me" (Week 7) is the consumer. When I changed the extraction strategy from `fast_text` to `vision_augmented`, the confidence distribution shifted — but no contract was updated because the producer (Week 3 code) doesn't know about the consumer (Week 7 contracts). This is the "treating past-you as a third party" problem the spec describes.

3. **Statistical drift is invisible**: Structural changes (column renamed, type changed) are easy to detect. Statistical drift (confidence mean shifts from 0.8 to 0.5 because the model was retrained) passes every structural check. Without baseline tracking and automated drift detection, these changes accumulate silently until a downstream consumer produces wrong results.

4. **Cost of enforcement exceeds perceived benefit**: Running validation on every pipeline execution adds latency. Teams disable it "temporarily" during crunch periods. The temporary becomes permanent. The contract becomes decoration.

### How My Architecture Prevents This

- **Schema snapshots on every generator run**: The ContractGenerator writes a timestamped snapshot to `schema_snapshots/{contract_id}/{timestamp}.yaml` on every execution. This creates an audit trail of schema evolution that the SchemaEvolutionAnalyzer can diff. In my repo, `schema_snapshots/week3-document-refinery-extractions/` contains snapshots from each generator run, enabling temporal comparison.

- **Statistical baselines in `baselines.json`**: The ValidationRunner stores the first-run mean and stddev for every numeric column in `schema_snapshots/baselines.json`. Subsequent runs compare against this baseline and emit WARN (>2σ) or FAIL (>3σ). This caught the confidence 0.0–1.0 → 0–100 change at 2324σ deviation — a change that passes every structural check.

- **Lineage-driven blast radius**: Every contract includes `downstream` consumers from the Week 4 lineage graph. When a violation is detected, the blast radius is computed automatically via BFS — not guessed. This makes the cost of ignoring a violation concrete: "this affects 7 downstream nodes and 178 records" is actionable; "something might break" is not.

- **Violation log as Week 8 input**: Every violation is written to `violation_log/violations.jsonl` in a schema designed to be consumed by the Week 8 Sentinel without modification. The violation record includes `violation_id`, `check_id`, `detected_at`, `blame_chain`, and `blast_radius` — all fields the Sentinel needs for alerting. This ensures the enforcement system is not a dead-end report but a live signal in the monitoring pipeline.

- **Contract generation is automated, not manual**: The ContractGenerator infers contracts from data profiles, not from human documentation. When the data changes, re-running the generator produces an updated contract. The SchemaEvolutionAnalyzer then diffs the old and new contracts to detect what changed. This inverts the staleness problem: instead of contracts lagging behind data, contracts are regenerated from data and changes are flagged.

The key insight: contracts go stale when enforcement is manual. By making the ValidationRunner a required step in the data pipeline (not an optional audit), staleness becomes a pipeline failure — which gets fixed immediately.
