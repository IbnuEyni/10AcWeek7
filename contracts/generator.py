#!/usr/bin/env python3
"""ContractGenerator: Auto-generates Bitol-compatible YAML contracts from JSONL data.

Usage:
    python contracts/generator.py --source outputs/week3/extractions.jsonl --output generated_contracts/
    python contracts/generator.py --all  # Generate contracts for all known sources
"""
import argparse, json, os, re, sys, uuid, hashlib, yaml
from datetime import datetime, timezone
from collections import Counter
import pandas as pd
import numpy as np

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Known source configs
SOURCES = {
    "week1_intent_records": {
        "path": "outputs/week1/intent_records.jsonl",
        "id": "week1-intent-code-correlator",
        "title": "Week 1 Intent-Code Correlator — Intent Records",
        "owner": "week1-team",
        "description": "One record per intent-code mapping. Links plain-English intent descriptions to code references.",
    },
    "week2_verdicts": {
        "path": "outputs/week2/verdicts.jsonl",
        "id": "week2-digital-courtroom-verdicts",
        "title": "Week 2 Digital Courtroom — Verdict Records",
        "owner": "week2-team",
        "description": "One record per evaluation verdict. Contains rubric scores and overall pass/fail/warn.",
    },
    "week3_extractions": {
        "path": "outputs/week3/extractions.jsonl",
        "id": "week3-document-refinery-extractions",
        "title": "Week 3 Document Refinery — Extraction Records",
        "owner": "week3-team",
        "description": "One record per processed document. Contains extracted facts and entities.",
    },
    "week4_lineage": {
        "path": "outputs/week4/lineage_snapshots.jsonl",
        "id": "week4-brownfield-cartographer-lineage",
        "title": "Week 4 Brownfield Cartographer — Lineage Snapshots",
        "owner": "week4-team",
        "description": "One record per lineage snapshot. Contains nodes and edges of the codebase dependency graph.",
    },
    "week5_events": {
        "path": "outputs/week5/events.jsonl",
        "id": "week5-event-sourcing-platform-events",
        "title": "Week 5 Event Sourcing Platform — Event Records",
        "owner": "week5-team",
        "description": "One record per domain event. Immutable event log with causation and correlation tracking.",
    },
    "langsmith_traces": {
        "path": "outputs/traces/runs.jsonl",
        "id": "langsmith-trace-records",
        "title": "LangSmith Trace Export — Run Records",
        "owner": "platform-team",
        "description": "One record per LLM/chain/tool run. Contains token counts, costs, and timing.",
    },
}

# Contract-specific enforcement rules
ENFORCEMENT_RULES = {
    "week1-intent-code-correlator": {
        "confidence_fields": ["code_refs[*].confidence"],
        "timestamp_fields": ["created_at"],
        "uuid_fields": ["intent_id"],
        "nonempty_arrays": ["code_refs"],
        "enum_fields": {},
    },
    "week2-digital-courtroom-verdicts": {
        "confidence_fields": ["confidence"],
        "timestamp_fields": ["evaluated_at"],
        "uuid_fields": ["verdict_id"],
        "enum_fields": {"overall_verdict": ["PASS", "FAIL", "WARN"]},
        "int_range_fields": {"scores[*].score": (1, 5)},
        "float_range_fields": {"overall_score": (1.0, 5.0)},
    },
    "week3-document-refinery-extractions": {
        "confidence_fields": ["extracted_facts[*].confidence"],
        "timestamp_fields": ["extracted_at"],
        "uuid_fields": ["extracted_facts[*].fact_id"],
        "non_unique_id_fields": ["doc_id"],
        "enum_fields": {"entities[*].type": ["PERSON", "ORG", "LOCATION", "DATE", "AMOUNT", "OTHER"]},
        "positive_int_fields": ["processing_time_ms"],
        "cross_ref_fields": {"extracted_facts[*].entity_refs": "entities[*].entity_id"},
        "pattern_fields": {"source_hash": r"^[a-f0-9]{64}$"},
    },
    "week4-brownfield-cartographer-lineage": {
        "timestamp_fields": ["captured_at"],
        "uuid_fields": ["snapshot_id"],
        "enum_fields": {
            "nodes[*].type": ["FILE", "TABLE", "SERVICE", "MODEL", "PIPELINE", "EXTERNAL"],
            "edges[*].relationship": ["IMPORTS", "CALLS", "READS", "WRITES", "PRODUCES", "CONSUMES"],
        },
        "pattern_fields": {"git_commit": r"^[a-f0-9]{40}$"},
        "graph_integrity": {"edges[*].source": "nodes[*].node_id", "edges[*].target": "nodes[*].node_id"},
    },
    "week5-event-sourcing-platform-events": {
        "timestamp_fields": ["occurred_at", "recorded_at"],
        "uuid_fields": ["event_id"],
        "temporal_order": {"recorded_at": "occurred_at"},
        "pascal_case_fields": ["event_type", "aggregate_type"],
        "non_unique_id_fields": ["aggregate_id"],
        "pattern_fields": {"aggregate_id": r"^(loan|agent-session|compliance|audit)-"},
    },
    "langsmith-trace-records": {
        "timestamp_fields": ["start_time", "end_time"],
        "uuid_fields": ["id"],
        "temporal_order": {"end_time": "start_time"},
        "enum_fields": {"run_type": ["llm", "chain", "tool", "retriever", "embedding"]},
        "token_sum": {"total_tokens": ["prompt_tokens", "completion_tokens"]},
        "non_negative_fields": ["total_cost"],
        "non_unique_id_fields": ["parent_run_id", "session_id", "inputs.doc_id"],
        "confidence_fields": ["outputs.confidence"],
    },
}

# Downstream dependency map from lineage
DOWNSTREAM_MAP = {
    "week1-intent-code-correlator": [
        {"id": "week2-digital-courtroom", "fields_consumed": ["code_refs"], "breaking_if_changed": ["code_refs[*].file", "intent_id"]},
    ],
    "week3-document-refinery-extractions": [
        {"id": "week4-brownfield-cartographer", "fields_consumed": ["doc_id", "extracted_facts", "extraction_model"],
         "breaking_if_changed": ["extracted_facts[*].confidence", "doc_id"]},
    ],
    "week4-brownfield-cartographer-lineage": [
        {"id": "week7-violation-attributor", "fields_consumed": ["nodes", "edges", "git_commit"],
         "breaking_if_changed": ["nodes[*].node_id", "edges[*].source", "edges[*].target"]},
    ],
    "week5-event-sourcing-platform-events": [
        {"id": "week7-contract-enforcer", "fields_consumed": ["event_type", "payload", "schema_version"],
         "breaking_if_changed": ["payload", "event_type"]},
    ],
}


def load_jsonl(path):
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def flatten_record(record, prefix=""):
    """Flatten nested dict/list for profiling — all array items, not just the first."""
    flat = {}
    if isinstance(record, dict):
        for k, v in record.items():
            key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                flat.update(flatten_record(v, key))
            elif isinstance(v, list):
                flat[key] = v
                # Collect scalar values from all items under the [*] key
                array_key = f"{key}[*]"
                for item in v:
                    if isinstance(item, dict):
                        sub = flatten_record(item, array_key)
                        for sk, sv in sub.items():
                            if sk not in flat:
                                flat[sk] = []
                            if isinstance(flat[sk], list):
                                flat[sk].append(sv)
                            else:
                                flat[sk] = [flat[sk], sv]
            else:
                flat[key] = v
    return flat


def infer_type(values):
    """Infer JSON Schema type from sample values."""
    types = set()
    for v in values:
        if v is None:
            continue
        elif isinstance(v, bool):
            types.add("boolean")
        elif isinstance(v, int):
            types.add("integer")
        elif isinstance(v, float):
            types.add("number")
        elif isinstance(v, str):
            types.add("string")
        elif isinstance(v, list):
            types.add("array")
        elif isinstance(v, dict):
            types.add("object")
    if len(types) == 0:
        return "null"
    if types == {"integer", "number"} or types == {"number"}:
        return "number"
    if len(types) == 1:
        return types.pop()
    return "string"


def profile_column(values):
    """Structural + statistical profiling for a column."""
    non_null = [v for v in values if v is not None]
    null_count = len(values) - len(non_null)
    col_type = infer_type(non_null)
    profile = {
        "type": col_type,
        "null_fraction": round(null_count / max(len(values), 1), 4),
        "cardinality": len(set(str(v) for v in non_null)),
        "total_count": len(values),
    }
    if col_type in ("number", "integer") and non_null:
        nums = [float(v) for v in non_null if isinstance(v, (int, float))]
        if nums:
            arr = np.array(nums)
            profile["stats"] = {
                "min": round(float(np.min(arr)), 4),
                "max": round(float(np.max(arr)), 4),
                "mean": round(float(np.mean(arr)), 4),
                "stddev": round(float(np.std(arr)), 4),
                "p25": round(float(np.percentile(arr, 25)), 4),
                "p50": round(float(np.percentile(arr, 50)), 4),
                "p75": round(float(np.percentile(arr, 75)), 4),
                "p95": round(float(np.percentile(arr, 95)), 4),
                "p99": round(float(np.percentile(arr, 99)), 4),
            }
    if col_type == "string" and non_null:
        sample = list(set(str(v) for v in non_null))[:5]
        profile["sample_values"] = sample
        # Dominant character pattern detection
        patterns = {"uuid": r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
                    "iso8601": r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',
                    "sha256": r'^[a-f0-9]{64}$',
                    "sha40": r'^[a-f0-9]{40}$',
                    "url": r'^https?://',
                    "path": r'^[/.]'}
        check_sample = [str(v) for v in non_null[:20]]
        for fmt_name, pat in patterns.items():
            if all(re.match(pat, s) for s in check_sample):
                profile["format"] = fmt_name
                profile["dominant_pattern"] = pat
                break
        if "dominant_pattern" not in profile and check_sample:
            # Infer dominant pattern from character classes
            alpha_frac = sum(c.isalpha() for s in check_sample for c in s) / max(sum(len(s) for s in check_sample), 1)
            digit_frac = sum(c.isdigit() for s in check_sample for c in s) / max(sum(len(s) for s in check_sample), 1)
            if digit_frac > 0.6:
                profile["dominant_pattern"] = "mostly_numeric"
            elif alpha_frac > 0.6:
                profile["dominant_pattern"] = "mostly_alpha"
            else:
                profile["dominant_pattern"] = "mixed"
    return profile


def profile_records(records):
    """Profile all columns from JSONL records."""
    all_flat = [flatten_record(r) for r in records]
    all_keys = set()
    for f in all_flat:
        all_keys.update(f.keys())
    profiles = {}
    for key in sorted(all_keys):
        raw_values = [f.get(key) for f in all_flat]
        # For [*] keys each record contributes a list; flatten one level
        if "[*]" in key:
            values = []
            for v in raw_values:
                if isinstance(v, list):
                    values.extend(v)
                elif v is not None:
                    values.append(v)
        else:
            values = raw_values
        profiles[key] = profile_column(values)
    return profiles


def load_lineage_graph():
    """Step 3: Load the latest snapshot from outputs/week4/lineage_snapshots.jsonl."""
    path = os.path.join(BASE_DIR, "outputs/week4/lineage_snapshots.jsonl")
    if not os.path.exists(path):
        return None
    records = load_jsonl(path)
    return records[-1] if records else None


def query_lineage_downstream(contract_id, lineage_graph):
    """Step 3: Query lineage graph for downstream consumers of a contract's table."""
    # Start with hardcoded map as fallback
    result = list(DOWNSTREAM_MAP.get(contract_id, []))
    if not lineage_graph:
        return result

    # Map contract_id to likely node patterns in the lineage graph
    contract_to_node = {
        "week1-intent-code-correlator": ["intent", "week1"],
        "week2-digital-courtroom-verdicts": ["verdict", "week2", "courtroom", "audit"],
        "week3-document-refinery-extractions": ["extraction", "week3", "refinery", "extractor"],
        "week4-brownfield-cartographer-lineage": ["lineage", "week4", "cartographer"],
        "week5-event-sourcing-platform-events": ["event", "week5", "ledger"],
        "langsmith-trace-records": ["trace", "langsmith"],
    }
    keywords = contract_to_node.get(contract_id, [])
    if not keywords:
        return result

    # Find source nodes matching this contract
    source_node_ids = set()
    for node in lineage_graph.get("nodes", []):
        nid = node.get("node_id", "").lower()
        label = node.get("label", "").lower()
        path = node.get("metadata", {}).get("path", "").lower()
        for kw in keywords:
            if kw in nid or kw in label or kw in path:
                source_node_ids.add(node["node_id"])
                break

    # BFS forward to find downstream consumers
    adj = {}
    for edge in lineage_graph.get("edges", []):
        adj.setdefault(edge["source"], []).append(edge["target"])

    visited = set()
    queue = list(source_node_ids)
    downstream_nodes = []
    while queue:
        nid = queue.pop(0)
        if nid in visited:
            continue
        visited.add(nid)
        if nid not in source_node_ids:
            downstream_nodes.append(nid)
        for neighbor in adj.get(nid, []):
            if neighbor not in visited:
                queue.append(neighbor)

    # Add dynamically discovered downstream consumers
    if downstream_nodes:
        existing_ids = {d["id"] for d in result}
        for dn in downstream_nodes[:5]:
            dn_label = dn.split("::")[-1] if "::" in dn else dn
            dn_id = f"lineage-{dn_label}"
            if dn_id not in existing_ids:
                result.append({
                    "id": dn_id,
                    "fields_consumed": ["*"],
                    "breaking_if_changed": ["*"],
                    "description": f"Downstream consumer discovered via Week 4 lineage graph: {dn}",
                })

    return result


def _call_llm_for_annotation(col_name, table_name, sample_values, adjacent_cols, col_type):
    """Call Claude or OpenAI to annotate an ambiguous column. Returns dict or None."""
    api_key = os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return None

    prompt = (
        f"You are a data contract expert. Given this column from a data pipeline, "
        f"provide a JSON object with exactly three keys: "
        f"\"description\" (one sentence plain-English), "
        f"\"business_rule\" (a validation expression), "
        f"\"cross_column\" (any relationship to adjacent columns, or null).\n\n"
        f"Table: {table_name}\n"
        f"Column: {col_name}\n"
        f"Type: {col_type}\n"
        f"Sample values: {sample_values[:5]}\n"
        f"Adjacent columns: {adjacent_cols[:8]}\n\n"
        f"Respond with only the JSON object, no markdown."
    )

    try:
        if os.environ.get("ANTHROPIC_API_KEY"):
            import anthropic
            client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
            msg = client.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=256,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = msg.content[0].text.strip()
        else:
            import openai
            client = openai.OpenAI(api_key=os.environ["OPENAI_API_KEY"])
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                max_tokens=256,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = resp.choices[0].message.content.strip()

        # Strip markdown fences if present
        raw = re.sub(r'^```(?:json)?\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)
        result = json.loads(raw)
        result["annotation_method"] = "llm"
        return result
    except Exception as e:
        print(f"    LLM annotation failed for {col_name}: {e}")
        return None


def generate_llm_annotations(col_name, profile, source_name, all_col_names=None):
    """Step 4: LLM annotation for ambiguous columns.
    Tries real LLM call first (if API key set), falls back to heuristic.
    """
    # Columns with clear meaning from name/format don't need annotation
    clear_names = {"id", "doc_id", "intent_id", "verdict_id", "event_id", "snapshot_id",
                   "created_at", "extracted_at", "evaluated_at", "occurred_at", "recorded_at",
                   "captured_at", "start_time", "end_time", "source_hash", "git_commit"}
    base_name = col_name.split(".")[-1].split("[*]")[-1].strip(".")
    if base_name in clear_names:
        return None
    if profile.get("format") in ("uuid", "iso8601", "sha256"):
        return None

    annotation = {"column": col_name, "source": source_name}

    # Try real LLM call first
    adjacent = [c for c in (all_col_names or []) if c != col_name][:8]
    llm_result = _call_llm_for_annotation(
        col_name, source_name,
        profile.get("sample_values", []),
        adjacent,
        profile.get("type", "unknown")
    )
    if llm_result:
        annotation.update(llm_result)
        return annotation

    # Infer description from name patterns
    if "confidence" in col_name:
        annotation["description"] = "Confidence score for the parent operation. Float 0.0-1.0 where 1.0 is highest confidence."
        annotation["business_rule"] = "0.0 <= value <= 1.0"
    elif "token" in col_name:
        annotation["description"] = "Token count metric for LLM operations. Must be non-negative integer."
        annotation["business_rule"] = "value >= 0"
    elif "cost" in col_name:
        annotation["description"] = "Monetary cost in USD for the operation."
        annotation["business_rule"] = "value >= 0.0"
    elif "fraud" in col_name:
        annotation["description"] = "Fraud probability score. Float 0.0-1.0 where higher means more suspicious."
        annotation["business_rule"] = "0.0 <= value <= 1.0"
    elif col_name.endswith(".score") and profile.get("type") in ("integer", "number"):
        # Only apply score rule to leaf .score fields, not .evidence or .notes siblings
        annotation["description"] = "Rubric criterion score. Integer 1-5 where 5 is highest."
        annotation["business_rule"] = "1 <= value <= 5 (integer)"
    elif "score" in col_name and not col_name.endswith((".evidence", ".notes")) and profile.get("type") in ("integer", "number"):
        annotation["description"] = "Evaluation score from rubric-based assessment."
        annotation["business_rule"] = "1 <= value <= 5 (integer)"
    elif "type" in col_name and profile.get("type") == "string":
        sample = profile.get("sample_values", [])
        annotation["description"] = f"Categorical type field. Observed values: {sample[:5]}"
        annotation["business_rule"] = f"value in {sample[:10]}"
    elif "path" in col_name:
        annotation["description"] = "File system or URL path reference."
        annotation["business_rule"] = "non-empty string"
    elif "model" in col_name:
        annotation["description"] = "Model identifier. Must match known model naming pattern."
        annotation["business_rule"] = "matches pattern ^(claude|gpt)-"
        annotation["cross_column"] = "Determines extraction strategy and cost profile"
    elif "payload" in col_name:
        annotation["description"] = "Event-type-specific data payload. Schema varies by event_type."
        annotation["business_rule"] = "must validate against event_type JSON Schema"
        annotation["cross_column"] = "Schema determined by event_type and schema_version fields"
    else:
        # Generic annotation for truly ambiguous columns
        sample = profile.get("sample_values", [])
        annotation["description"] = f"Auto-profiled column. Type: {profile['type']}, cardinality: {profile.get('cardinality', 'unknown')}."
        if sample:
            annotation["description"] += f" Sample values: {sample[:3]}"
        annotation["business_rule"] = "See contract schema for constraints"

    annotation["annotation_method"] = "heuristic (no LLM API key configured)"
    return annotation


# Field descriptions for known schemas (makes contracts human-readable)
FIELD_DESCRIPTIONS = {
    "doc_id": "Primary key. UUIDv4. Stable across re-extractions of the same source.",
    "source_hash": "SHA-256 of the source file. Changes iff the source content changes.",
    "source_path": "Absolute path or URL to the source document.",
    "extraction_model": "Model identifier. Must match pattern claude-* or gpt-*.",
    "processing_time_ms": "Wall-clock extraction time in milliseconds. Positive integer.",
    "extracted_at": "ISO 8601 timestamp of when extraction completed.",
    "intent_id": "Primary key. UUIDv4. One per intent-code mapping.",
    "description": "Plain-English statement of the developer intent.",
    "created_at": "ISO 8601 timestamp of when the intent was recorded.",
    "verdict_id": "Primary key. UUIDv4. One per evaluation verdict.",
    "target_ref": "Relative path or document ID of the evaluation target.",
    "rubric_id": "SHA-256 hash of the rubric YAML used for evaluation.",
    "rubric_version": "Semantic version of the rubric (e.g. 3.0.0).",
    "overall_verdict": "Final verdict. Must be exactly one of: PASS, FAIL, WARN.",
    "overall_score": "Weighted average of all criterion scores. Float.",
    "evaluated_at": "ISO 8601 timestamp of when evaluation completed.",
    "snapshot_id": "Primary key. UUIDv4. One per lineage snapshot.",
    "codebase_root": "Absolute path to the root of the analysed codebase.",
    "git_commit": "40-character hex SHA of the git commit analysed.",
    "captured_at": "ISO 8601 timestamp of when the snapshot was taken.",
    "event_id": "Primary key. UUIDv4. Globally unique event identifier.",
    "event_type": "PascalCase event type. Must be registered in schema registry.",
    "aggregate_id": "Identifier of the aggregate this event belongs to.",
    "aggregate_type": "PascalCase aggregate type (e.g. LoanApplication, AgentSession).",
    "sequence_number": "Monotonically increasing per aggregate_id. No gaps, no duplicates.",
    "schema_version": "Schema version of the event payload.",
    "occurred_at": "ISO 8601 timestamp of when the event occurred in the domain.",
    "recorded_at": "ISO 8601 timestamp of when the event was persisted. Must be >= occurred_at.",
}

FIELD_PATTERNS = {
    "extraction_model": r"^(claude|gpt)-",
    "source_hash": r"^[a-f0-9]{64}$",
    "git_commit": r"^[a-f0-9]{40}$",
}

# Domain-aware range overrides: (contract_id, col_name) -> {minimum, maximum, description}
# These override auto-inferred stats ranges when the sample-based range is too tight.
DOMAIN_RANGE_OVERRIDES = {
    ("week3-document-refinery-extractions", "extracted_facts[*].page_ref"): {
        "minimum": 0, "maximum": 10000,
        "description": "Zero-indexed page number. Nullable. Domain max: PDFs can have thousands of pages.",
    },
    ("week3-document-refinery-extractions", "processing_time_ms"): {
        "minimum": 1,
        "description": "Wall-clock extraction time in milliseconds. Positive integer, no upper bound.",
    },
    ("week5-event-sourcing-platform-events", "sequence_number"): {
        "minimum": 1,
        "description": "Monotonically increasing per aggregate_id. Starts at 1.",
    },
    ("langsmith-trace-records", "total_cost"): {
        "minimum": 0.0,
        "description": "Monetary cost in USD. Must be non-negative.",
    },
    ("langsmith-trace-records", "prompt_tokens"): {
        "minimum": 0,
        "description": "Prompt token count. Must be non-negative integer.",
    },
    ("langsmith-trace-records", "completion_tokens"): {
        "minimum": 0,
        "description": "Completion token count. Must be non-negative integer.",
    },
    ("langsmith-trace-records", "total_tokens"): {
        "minimum": 0,
        "description": "Total token count. Must equal prompt_tokens + completion_tokens.",
    },
}


def build_schema_section(profiles, contract_id):
    """Build the schema section of the contract YAML."""
    rules = ENFORCEMENT_RULES.get(contract_id, {})
    schema = {}
    for col_name, prof in profiles.items():
        if "[*]" in col_name and col_name.count("[*]") > 1:
            continue  # skip deeply nested for top-level schema
        entry = {"type": prof["type"]}
        if prof["null_fraction"] == 0:
            entry["required"] = True
        if prof.get("format"):
            entry["format"] = prof["format"]
        if "stats" in prof:
            entry["minimum"] = prof["stats"]["min"]
            entry["maximum"] = prof["stats"]["max"]
        # Add human-readable description from known fields
        base_name = col_name.split("[*].")[-1] if "[*]" in col_name else col_name
        if base_name in FIELD_DESCRIPTIONS:
            entry["description"] = FIELD_DESCRIPTIONS[base_name]
        # Add pattern from known fields
        if base_name in FIELD_PATTERNS and "pattern" not in entry:
            entry["pattern"] = FIELD_PATTERNS[base_name]
        # Check if this is a confidence field
        conf_fields = rules.get("confidence_fields", [])
        for cf in conf_fields:
            if col_name == cf or col_name.endswith(cf.split("[*].")[-1] if "[*]" in cf else cf):
                if "confidence" in col_name:
                    entry["type"] = "number"
                    entry["minimum"] = 0.0
                    entry["maximum"] = 1.0
                    entry["description"] = "Confidence score. MUST be float 0.0-1.0. BREAKING CHANGE if changed to 0-100."
        # UUID fields
        uuid_fields = rules.get("uuid_fields", [])
        for uf in uuid_fields:
            if col_name == uf or col_name.endswith(uf.split("[*].")[-1] if "[*]" in uf else uf):
                entry["format"] = "uuid"
                entry["unique"] = True
        # Non-unique ID fields (e.g. aggregate_id in event sourcing)
        for nuf in rules.get("non_unique_id_fields", []):
            if col_name == nuf:
                entry.pop("unique", None)
                entry.pop("format", None)
        # Enum fields
        enum_fields = rules.get("enum_fields", {})
        for ef, vals in enum_fields.items():
            if col_name == ef or col_name.endswith(ef.split("[*].")[-1] if "[*]" in ef else ef):
                entry["enum"] = vals
        # Pattern fields
        pattern_fields = rules.get("pattern_fields", {})
        for pf, pattern in pattern_fields.items():
            if col_name == pf:
                entry["pattern"] = pattern
        # Int range fields
        int_range = rules.get("int_range_fields", {})
        for irf, (lo, hi) in int_range.items():
            if col_name == irf or col_name.endswith(irf.split("[*].")[-1] if "[*]" in irf else irf):
                entry["minimum"] = lo
                entry["maximum"] = hi
                entry["type"] = "integer"
        # Float range fields (e.g. overall_score is a float, not integer)
        float_range = rules.get("float_range_fields", {})
        for frf, (lo, hi) in float_range.items():
            if col_name == frf or col_name.endswith(frf.split("[*].")[-1] if "[*]" in frf else frf):
                entry["minimum"] = lo
                entry["maximum"] = hi
                entry["type"] = "number"
        # Domain-aware range overrides — applied last, always win over auto-inferred stats
        override = DOMAIN_RANGE_OVERRIDES.get((contract_id, col_name))
        if override:
            if "minimum" in override:
                entry["minimum"] = override["minimum"]
            if "maximum" in override:
                entry["maximum"] = override["maximum"]
            else:
                entry.pop("maximum", None)  # remove auto-inferred max when domain has no upper bound
            if "description" in override:
                entry["description"] = override["description"]
        schema[col_name] = entry
    return schema


def build_quality_section(profiles, contract_id):
    """Build quality checks section."""
    rules = ENFORCEMENT_RULES.get(contract_id, {})
    non_unique = set(rules.get("non_unique_id_fields", []))
    checks = []
    for col, prof in profiles.items():
        if prof["null_fraction"] == 0 and "[*]" not in col:
            checks.append(f"missing_count({col}) = 0")
    for uf in rules.get("uuid_fields", []):
        if "[*]" not in uf and uf not in non_unique:
            checks.append(f"duplicate_count({uf}) = 0")
    for cf in rules.get("confidence_fields", []):
        base = cf.replace("[*].", "_")
        checks.append(f"min({base}_min) >= 0.0")
        checks.append(f"max({base}_max) <= 1.0")
    checks.append("row_count >= 1")
    return {"type": "SodaChecks", "specification": {"checks": checks}}


def generate_contract(source_name, source_path, output_dir):
    """Generate a Bitol-compatible YAML contract for a JSONL source."""
    config = SOURCES.get(source_name, {})
    contract_id = config.get("id", source_name)
    full_path = os.path.join(BASE_DIR, source_path)
    if not os.path.exists(full_path):
        print(f"  SKIP: {full_path} not found")
        return None

    records = load_jsonl(full_path)
    if not records:
        print(f"  SKIP: {full_path} is empty")
        return None

    print(f"  Profiling {len(records)} records from {source_path}...")
    profiles = profile_records(records)
    schema = build_schema_section(profiles, contract_id)
    quality = build_quality_section(profiles, contract_id)

    # Step 3: Dynamic lineage context injection
    lineage_graph = load_lineage_graph()
    downstream = query_lineage_downstream(contract_id, lineage_graph)

    # Step 4: LLM annotations for ambiguous columns
    llm_annotations = []
    all_col_names = list(profiles.keys())
    for col_name, prof in profiles.items():
        ann = generate_llm_annotations(col_name, prof, source_name, all_col_names)
        if ann:
            llm_annotations.append(ann)

    contract = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": contract_id,
        "info": {
            "title": config.get("title", source_name),
            "version": "1.0.0",
            "owner": config.get("owner", "platform-team"),
            "description": config.get("description", ""),
        },
        "servers": {
            "local": {
                "type": "local",
                "path": source_path,
                "format": "jsonl",
            }
        },
        "terms": {
            "usage": "Internal inter-system data contract. Do not publish.",
            "limitations": "All confidence fields must remain in 0.0-1.0 float range.",
        },
        "schema": schema,
        "quality": quality,
        "lineage": {
            "upstream": [],
            "downstream": [
                {
                    "id": d["id"],
                    "description": d.get("description", f"{d['id']} consumes fields from this contract"),
                    "fields_consumed": d["fields_consumed"],
                    "breaking_if_changed": d["breaking_if_changed"],
                }
                for d in downstream
            ],
        },
    }

    # Step 4: Append LLM annotations if any
    if llm_annotations:
        contract["llm_annotations"] = llm_annotations

    # Write contract YAML
    os.makedirs(output_dir, exist_ok=True)
    yaml_path = os.path.join(output_dir, f"{source_name}.yaml")
    with open(yaml_path, "w") as f:
        yaml.dump(contract, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    print(f"  Wrote contract: {yaml_path}")

    # Write dbt schema.yml
    generate_dbt_schema(source_name, schema, profiles, contract_id, output_dir)

    # Write schema snapshot
    save_schema_snapshot(contract_id, profiles)

    return contract


def generate_dbt_schema(source_name, schema, profiles, contract_id, output_dir):
    """Step 5: Generate dbt-compatible schema.yml with not_null, unique, accepted_values, relationships."""
    rules = ENFORCEMENT_RULES.get(contract_id, {})
    columns = []
    for col_name, col_schema in schema.items():
        if "[*]" in col_name:
            continue
        # Skip payload sub-fields with no tests (noise reduction)
        if col_name.startswith("payload.") and not col_schema.get("required") and "enum" not in col_schema:
            continue
        # Convert dot notation to underscore for dbt compatibility
        dbt_name = col_name.replace(".", "_")
        col_def = {"name": dbt_name, "description": col_schema.get("description", ""), "tests": []}
        if col_schema.get("required"):
            col_def["tests"].append("not_null")
        if col_schema.get("unique"):
            col_def["tests"].append("unique")
        if "enum" in col_schema:
            col_def["tests"].append({"accepted_values": {"values": col_schema["enum"]}})
        columns.append(col_def)

    # Add relationships tests for cross-reference fields
    cross_refs = rules.get("cross_ref_fields", {})
    for ref_field, target_field in cross_refs.items():
        ref_base = ref_field.replace("[*].", "_").replace(".", "_")
        tgt_base = target_field.replace("[*].", "_").replace(".", "_")
        columns.append({
            "name": ref_base,
            "description": f"Foreign key: {ref_field} references {target_field}",
            "tests": [{"relationships": {"to": f"ref('{source_name}')", "field": tgt_base}}],
        })

    # Model-level expression_is_true tests for cross-field constraints
    model_tests = []
    if "recorded_at" in schema and "occurred_at" in schema:
        model_tests.append({"dbt_utils.expression_is_true": {"expression": "recorded_at >= occurred_at"}})
    if "end_time" in schema and "start_time" in schema:
        model_tests.append({"dbt_utils.expression_is_true": {"expression": "end_time >= start_time"}})
    if "total_tokens" in schema and "prompt_tokens" in schema and "completion_tokens" in schema:
        model_tests.append({"dbt_utils.expression_is_true": {"expression": "total_tokens = prompt_tokens + completion_tokens"}})
    if "overall_score" in schema:
        model_tests.append({"dbt_utils.expression_is_true": {"expression": "overall_score >= 1.0 and overall_score <= 5.0"}})

    model_def = {
        "name": source_name,
        "description": f"dbt schema for {contract_id}",
        "columns": columns,
    }
    if model_tests:
        model_def["tests"] = model_tests

    dbt = {"version": 2, "models": [model_def]}
    dbt_path = os.path.join(output_dir, f"{source_name}_dbt.yml")
    with open(dbt_path, "w") as f:
        yaml.dump(dbt, f, default_flow_style=False, sort_keys=False)
    print(f"  Wrote dbt schema: {dbt_path}")


def save_schema_snapshot(contract_id, profiles):
    """Save timestamped schema snapshot for evolution tracking."""
    snap_dir = os.path.join(BASE_DIR, "schema_snapshots", contract_id)
    os.makedirs(snap_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    snapshot = {
        "contract_id": contract_id,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "columns": {},
    }
    for col_name, prof in profiles.items():
        snapshot["columns"][col_name] = {
            "type": prof["type"],
            "null_fraction": prof["null_fraction"],
            "cardinality": prof["cardinality"],
        }
        if "stats" in prof:
            snapshot["columns"][col_name]["stats"] = prof["stats"]
        if "format" in prof:
            snapshot["columns"][col_name]["format"] = prof["format"]
        if "sample_values" in prof:
            snapshot["columns"][col_name]["sample_values"] = prof["sample_values"]

    snap_path = os.path.join(snap_dir, f"{timestamp}.yaml")
    with open(snap_path, "w") as f:
        yaml.dump(snapshot, f, default_flow_style=False, sort_keys=False)
    print(f"  Wrote schema snapshot: {snap_path}")


def main():
    parser = argparse.ArgumentParser(description="ContractGenerator: Auto-generate data contracts from JSONL")
    parser.add_argument("--source", help="Path to a specific JSONL source file")
    parser.add_argument("--output", default="generated_contracts/", help="Output directory for contracts")
    parser.add_argument("--all", action="store_true", help="Generate contracts for all known sources")
    args = parser.parse_args()

    output_dir = os.path.join(BASE_DIR, args.output) if not os.path.isabs(args.output) else args.output

    if args.all:
        print("Generating contracts for all sources...")
        for name, config in SOURCES.items():
            print(f"\n--- {name} ---")
            generate_contract(name, config["path"], output_dir)
    elif args.source:
        # Find matching source config
        source_name = None
        for name, config in SOURCES.items():
            if args.source.endswith(config["path"]) or config["path"] in args.source:
                source_name = name
                break
        if not source_name:
            # Infer name from filename
            basename = os.path.splitext(os.path.basename(args.source))[0]
            source_name = basename
            SOURCES[source_name] = {
                "path": args.source,
                "id": f"custom-{basename}",
                "title": f"Custom Contract — {basename}",
                "owner": "platform-team",
                "description": f"Auto-generated contract for {basename}",
            }
        source_path = SOURCES[source_name]["path"]
        if not os.path.isabs(args.source) and os.path.exists(os.path.join(BASE_DIR, args.source)):
            source_path = args.source
        print(f"Generating contract for {source_name}...")
        generate_contract(source_name, source_path, output_dir)
    else:
        parser.print_help()
        sys.exit(1)

    print("\nContract generation complete.")


if __name__ == "__main__":
    main()
