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
    },
    "week3-document-refinery-extractions": {
        "confidence_fields": ["extracted_facts[*].confidence"],
        "timestamp_fields": ["extracted_at"],
        "uuid_fields": ["doc_id", "extracted_facts[*].fact_id"],
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
        "uuid_fields": ["event_id", "aggregate_id"],
        "temporal_order": {"recorded_at": "occurred_at"},  # recorded >= occurred
        "pascal_case_fields": ["event_type", "aggregate_type"],
    },
    "langsmith-trace-records": {
        "timestamp_fields": ["start_time", "end_time"],
        "uuid_fields": ["id"],
        "temporal_order": {"end_time": "start_time"},
        "enum_fields": {"run_type": ["llm", "chain", "tool", "retriever", "embedding"]},
        "token_sum": {"total_tokens": ["prompt_tokens", "completion_tokens"]},
        "non_negative_fields": ["total_cost"],
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
    """Flatten nested dict/list for profiling."""
    flat = {}
    if isinstance(record, dict):
        for k, v in record.items():
            key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                flat.update(flatten_record(v, key))
            elif isinstance(v, list):
                flat[key] = v
                for i, item in enumerate(v):
                    if isinstance(item, dict):
                        flat.update(flatten_record(item, f"{key}[*]"))
                        break  # profile first item as representative
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
        # Detect UUID pattern
        uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
        if all(uuid_pattern.match(str(v)) for v in non_null[:20]):
            profile["format"] = "uuid"
        # Detect ISO 8601
        iso_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}')
        if all(iso_pattern.match(str(v)) for v in non_null[:20]):
            profile["format"] = "iso8601"
        # Detect SHA-256
        sha_pattern = re.compile(r'^[a-f0-9]{64}$')
        if all(sha_pattern.match(str(v)) for v in non_null[:20]):
            profile["format"] = "sha256"
    return profile


def profile_records(records):
    """Profile all columns from JSONL records."""
    all_flat = []
    for r in records:
        all_flat.append(flatten_record(r))
    # Collect all keys
    all_keys = set()
    for f in all_flat:
        all_keys.update(f.keys())
    profiles = {}
    for key in sorted(all_keys):
        values = [f.get(key) for f in all_flat]
        profiles[key] = profile_column(values)
    return profiles


def get_lineage_downstream(contract_id):
    """Get downstream consumers from lineage graph."""
    return DOWNSTREAM_MAP.get(contract_id, [])


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
        # Check if this is a confidence field
        conf_fields = rules.get("confidence_fields", [])
        for cf in conf_fields:
            if col_name == cf or col_name.endswith(cf.split("[*].")[-1] if "[*]" in cf else cf):
                if "confidence" in col_name:
                    entry["minimum"] = 0.0
                    entry["maximum"] = 1.0
                    entry["description"] = "Confidence score. MUST be float 0.0-1.0. BREAKING CHANGE if changed to 0-100."
        # UUID fields
        uuid_fields = rules.get("uuid_fields", [])
        for uf in uuid_fields:
            if col_name == uf or col_name.endswith(uf.split("[*].")[-1] if "[*]" in uf else uf):
                entry["format"] = "uuid"
                entry["unique"] = True
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
        schema[col_name] = entry
    return schema


def build_quality_section(profiles, contract_id):
    """Build quality checks section."""
    rules = ENFORCEMENT_RULES.get(contract_id, {})
    checks = []
    # Required fields (null_fraction == 0)
    for col, prof in profiles.items():
        if prof["null_fraction"] == 0 and "[*]" not in col:
            checks.append(f"missing_count({col}) = 0")
    # Unique fields
    uuid_fields = rules.get("uuid_fields", [])
    for uf in uuid_fields:
        if "[*]" not in uf:
            checks.append(f"duplicate_count({uf}) = 0")
    # Confidence range
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
    downstream = get_lineage_downstream(contract_id)

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
                    "description": f"{d['id']} consumes fields from this contract",
                    "fields_consumed": d["fields_consumed"],
                    "breaking_if_changed": d["breaking_if_changed"],
                }
                for d in downstream
            ],
        },
    }

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
    """Generate dbt-compatible schema.yml."""
    rules = ENFORCEMENT_RULES.get(contract_id, {})
    columns = []
    for col_name, col_schema in schema.items():
        if "[*]" in col_name:
            continue
        col_def = {"name": col_name, "description": col_schema.get("description", ""), "tests": []}
        if col_schema.get("required"):
            col_def["tests"].append("not_null")
        if col_schema.get("unique"):
            col_def["tests"].append("unique")
        if "enum" in col_schema:
            col_def["tests"].append({"accepted_values": {"values": col_schema["enum"]}})
        columns.append(col_def)

    dbt = {
        "version": 2,
        "models": [
            {
                "name": source_name,
                "description": f"dbt schema for {contract_id}",
                "columns": columns,
            }
        ],
    }
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
