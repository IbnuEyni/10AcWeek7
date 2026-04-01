# DOMAIN_NOTES.md — Data Contract Enforcer

## Question 1: Backward-Compatible vs. Breaking Schema Changes

A **backward-compatible** change allows existing consumers to continue reading data without modification. A **breaking** change forces downstream consumers to update or fail.

### Backward-Compatible Examples (from my systems)

1. **Adding `governance_tags` to Week 1 intent_records**: The original Roo-Code `agent_trace.jsonl` had no governance tags. The migration script added `governance_tags` as a new nullable array. Week 2's courtroom (which consumes intent data to evaluate code references) can safely ignore this new field — it never read it before.

2. **Widening `processing_time_ms` from int to float in Week 3**: The extraction ledger originally stored `processing_time_ms` as a float like `0.25` (seconds). The canonical schema expects an integer (milliseconds). Widening from int to float is safe — any consumer doing `value > threshold` still works because float subsumes int.

3. **Adding `"EXTERNAL"` to Week 4 node types**: The original cartographer lineage graph used `node_type: "module"` and `"function"`. Adding `"EXTERNAL"` as a new enum value is additive — existing consumers that filter on `"module"` or `"function"` simply never encounter the new value.

### Breaking Examples (from my systems)

1. **Week 3 `confidence` float 0.0–1.0 → integer 0–100**: This is the canonical breaking change. The extraction ledger stores `confidence_score: 0.8`. If an update changed this to `confidence_score: 80`, the Week 4 Cartographer — which uses confidence to weight lineage edges — would treat `80` as an impossibly high confidence, corrupting all downstream graph traversals. The ValidationRunner catches this: `extracted_facts[*].confidence` range check fails with `max=98.8, mean=73.9` against the contract clause `max<=1.0`.

2. **Renaming `doc_id` to `document_id` in Week 3**: The Week 4 Cartographer references `doc_id` to create lineage nodes (`table::extractions`). A rename breaks the foreign key relationship silently — the Cartographer would create nodes with null document references, and no error would be raised because the field simply wouldn't exist.

3. **Removing `metadata.causation_id` from Week 5 events**: The event sourcing platform uses `causation_id` to build causal chains between events. Removing it breaks the `ComplianceAuditViewProjection` which traces decision lineage through causation chains. The projection would silently produce incomplete audit trails.

## Question 2: Confidence Field Failure Trace

### The Scenario
The Week 3 Document Refinery's `extracted_facts[*].confidence` field is defined as `float 0.0–1.0`. An update changes it to `integer 0–100`.

### Failure Propagation to Week 4 Cartographer
The Cartographer ingests Week 3 extraction records and uses `confidence` to:
- Weight edges in the lineage graph (`edge.confidence = fact.confidence`)
- Filter low-confidence facts (`if confidence < 0.6: skip`)

When confidence becomes `80` instead of `0.80`:
1. The filter `confidence < 0.6` passes (80 > 0.6), so no facts are filtered — even garbage facts with confidence `50` (which should be `0.50`, a borderline score) are included.
2. Edge weights become `80.0` instead of `0.80`, breaking any normalized graph algorithm (PageRank, shortest path) that assumes weights in [0, 1].
3. The Cartographer's `onboarding_brief.md` reports "all extractions high confidence" — a false positive that masks real quality issues.

### The Contract Clause (Bitol YAML)

```yaml
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
quality:
  type: SodaChecks
  specification:
    checks for extractions:
      - min(extracted_facts.confidence) >= 0.0
      - max(extracted_facts.confidence) <= 1.0
      - avg(extracted_facts.confidence) between 0.3 and 0.99
lineage:
  downstream:
    - id: week4-brownfield-cartographer
      fields_consumed: [extracted_facts.confidence]
      breaking_if_changed: [extracted_facts.confidence]
```

This clause catches the change in two ways: the structural `maximum: 1.0` check fails immediately when values exceed 1.0, and the statistical drift check (baseline mean ~0.80, new mean ~73.9) triggers a FAIL at >3σ deviation.

## Question 3: Blame Chain Construction via Lineage Graph

When the ValidationRunner detects a contract violation, the ViolationAttributor constructs a blame chain through these steps:

### Step 1 — Identify the Failing Schema Element
The violation report contains `check_id: "week3-document-refinery-extractions.extracted_facts[*].confidence.range"`. This identifies the failing column as `extracted_facts[*].confidence` in the Week 3 extraction output.

### Step 2 — Map to Lineage Node
The attributor maps `week3` to lineage nodes: `table::extractions`, `service::week3-refinery`, and `file::src/week3/extractor.py`. These are the nodes in the Week 4 lineage graph that produce the failing data.

### Step 3 — BFS Upstream Traversal
Starting from `table::extractions`, the attributor performs breadth-first search on the **reverse** adjacency of the lineage graph:
- `table::extractions` ← `service::week3-refinery` (PRODUCES, hop=1)
- `service::week3-refinery` ← `file::src/week3/extractor.py` (WRITES, hop=2)
- `file::src/week3/extractor.py` ← `file::src/utils/fact_extractor.py` (IMPORTS, hop=3)

The traversal stops at the first external boundary or when no more upstream nodes exist.

### Step 4 — Git Blame Integration
For each upstream file identified, the attributor runs:
```
git log --follow --since="14 days ago" --format='%H|%an|%ae|%ai|%s' -- src/week3/extractor.py
```
This returns commits that modified the file. For the confidence change, it finds:
- `cd5737c` — "feat: change confidence to percentage scale" (2025-01-14)

### Step 5 — Confidence Scoring
Each candidate gets a score: `1.0 - (days_since_commit × 0.1) - (hops × 0.2)`. The commit from 1 day ago at hop 0 scores `1.0 - 0.1 - 0.0 = 0.90`. A commit from 7 days ago at hop 2 scores `1.0 - 0.7 - 0.4 = 0.0` (clamped to 0.05).

### Step 6 — Blast Radius (Downstream BFS)
From the failing node, the attributor traverses **forward** through the lineage graph to find all affected downstream consumers: `service::week4-cartographer`, `file::src/week4/cartographer.py`, and any nodes they feed into.

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

# Structural clause
schema:
  id:
    type: string
    format: uuid
    required: true
    unique: true
  run_type:
    type: string
    required: true
    enum: [llm, chain, tool, retriever, embedding]
  start_time:
    type: string
    format: iso8601
    required: true
  end_time:
    type: string
    format: iso8601
    required: true
  total_tokens:
    type: integer
    required: true
    minimum: 0
    description: Must equal prompt_tokens + completion_tokens
  total_cost:
    type: number
    minimum: 0.0
    required: true

# Statistical clause
quality:
  type: SodaChecks
  specification:
    checks for traces:
      - avg(total_tokens) between 500 and 20000
      - avg(total_cost) between 0.001 and 0.10
      - max(total_cost) < 1.0
      - row_count >= 1

# AI-specific clause
ai_extensions:
  embedding_drift:
    description: >
      Monitor semantic drift in LLM inputs across runs.
      If the centroid cosine distance of input embeddings exceeds 0.15
      from baseline, trigger WARN. This detects prompt template changes
      or input distribution shifts that may degrade model performance.
    threshold: 0.15
    sample_size: 200
    baseline_path: schema_snapshots/embedding_baselines.npz
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
      description: Verdict generation produces LLM traces
    - id: week3-document-refinery
      description: Extraction pipeline produces LLM traces
  downstream:
    - id: week7-ai-contract-extensions
      fields_consumed: [total_tokens, total_cost, run_type, start_time, end_time]
      breaking_if_changed: [total_tokens, run_type]
```

## Question 5: Why Contract Enforcement Systems Fail

The most common failure mode is **contract staleness** — contracts are written once and never updated as the underlying data evolves. This happens because:

1. **No automated enforcement loop**: Contracts exist as documentation but are not executed on every data pipeline run. When a schema changes, the contract YAML sits unchanged in a repo while the data silently diverges.

2. **Ownership ambiguity**: The producer team writes the contract, but the consumer team depends on it. Neither team owns the enforcement step. When the producer changes their output, they don't update the contract because "it's the consumer's problem." The consumer doesn't update it because "the producer promised this format."

3. **Statistical drift is invisible**: Structural changes (column renamed, type changed) are easy to detect. Statistical drift (confidence mean shifts from 0.8 to 0.5 because the model was retrained) passes every structural check. Without baseline tracking and automated drift detection, these changes accumulate silently.

### How My Architecture Prevents This

- **Schema snapshots on every generator run**: The ContractGenerator writes a timestamped snapshot to `schema_snapshots/{contract_id}/{timestamp}.yaml` on every execution. This creates an audit trail of schema evolution that the SchemaEvolutionAnalyzer can diff.

- **Statistical baselines in `baselines.json`**: The ValidationRunner stores the first-run mean and stddev for every numeric column. Subsequent runs compare against this baseline and emit WARN (>2σ) or FAIL (>3σ). This caught the confidence 0.0–1.0 → 0–100 change at 2324σ deviation.

- **Lineage-driven blast radius**: Every contract includes `downstream` consumers from the Week 4 lineage graph. When a violation is detected, the blast radius is computed automatically — not guessed. This makes the cost of ignoring a violation concrete and visible.

- **Violation log as Week 8 input**: Every violation is written to `violation_log/violations.jsonl` in a schema designed to be consumed by the Week 8 Sentinel without modification. This ensures the enforcement system is not a dead-end report but a live signal in the monitoring pipeline.

The key insight: contracts go stale when enforcement is manual. By making the ValidationRunner a required step in the data pipeline (not an optional audit), staleness becomes a pipeline failure — which gets fixed immediately.
