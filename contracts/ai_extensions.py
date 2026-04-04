#!/usr/bin/env python3
"""AI Contract Extensions: Embedding drift, prompt input validation, LLM output schema enforcement.

Usage:
    python contracts/ai_extensions.py --all
    python contracts/ai_extensions.py --embedding-drift outputs/week3/extractions.jsonl
    python contracts/ai_extensions.py --prompt-validation outputs/week3/extractions.jsonl
    python contracts/ai_extensions.py --output-schema outputs/week2/verdicts.jsonl
"""
import argparse, json, os, re, uuid, hashlib
from datetime import datetime, timezone
import numpy as np

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASELINE_DIR = os.path.join(BASE_DIR, "schema_snapshots")
QUARANTINE_DIR = os.path.join(BASE_DIR, "outputs", "quarantine")
METRICS_PATH = os.path.join(BASE_DIR, "validation_reports", "ai_metrics.json")


def load_jsonl(path):
    records = []
    full = os.path.join(BASE_DIR, path) if not os.path.isabs(path) else path
    with open(full) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


# --- Extension 1: Embedding Drift Detection ---

def _simple_text_vector(text, dim=64):
    """Deterministic text-to-vector using character frequency hashing (no API needed)."""
    vec = np.zeros(dim)
    for i, ch in enumerate(text.encode("utf-8")):
        vec[ch % dim] += 1.0
    norm = np.linalg.norm(vec)
    return vec / norm if norm > 0 else vec


def _cosine_similarity(a, b):
    dot = np.dot(a, b)
    na, nb = np.linalg.norm(a), np.linalg.norm(b)
    return dot / (na * nb) if na > 0 and nb > 0 else 0.0


def check_embedding_drift(data_path, baseline_path=None, threshold=0.15, sample_size=200):
    """Extension 1: Detect semantic drift in text columns via embedding centroid distance."""
    records = load_jsonl(data_path)
    texts = []
    for r in records:
        for fact in r.get("extracted_facts", []):
            t = fact.get("text", "")
            if t:
                texts.append(t)
    if not texts:
        return {"status": "SKIP", "message": "No text values found"}

    sample = texts[:sample_size]
    vectors = np.array([_simple_text_vector(t) for t in sample])
    current_centroid = np.mean(vectors, axis=0)

    bp = baseline_path or os.path.join(BASELINE_DIR, "embedding_baselines.npz")
    if os.path.exists(bp):
        data = np.load(bp)
        baseline_centroid = data["centroid"]
        drift = 1.0 - _cosine_similarity(current_centroid, baseline_centroid)
    else:
        os.makedirs(os.path.dirname(bp), exist_ok=True)
        np.savez(bp, centroid=current_centroid)
        drift = 0.0

    status = "FAIL" if drift > threshold else ("WARN" if drift > threshold * 0.6 else "PASS")
    return {
        "check_type": "embedding_drift",
        "drift_score": round(float(drift), 4),
        "status": status,
        "threshold": threshold,
        "sample_size": len(sample),
        "message": f"Embedding drift={drift:.4f} ({'exceeds' if drift > threshold else 'within'} threshold {threshold})"
    }


# --- Extension 2: Prompt Input Schema Validation ---

PROMPT_INPUT_SCHEMA = {
    "required": ["doc_id", "source_path"],
    "properties": {
        "doc_id": {"type": "string", "pattern": r"^[0-9a-f]{8}-"},
        "source_path": {"type": "string", "min_length": 1},
    }
}


def check_prompt_input_schema(data_path):
    """Extension 2: Validate records against prompt input JSON Schema. Quarantine failures."""
    records = load_jsonl(data_path)
    valid, invalid = [], []
    for r in records:
        errors = []
        for field in PROMPT_INPUT_SCHEMA["required"]:
            if field not in r or r[field] is None:
                errors.append(f"missing required field: {field}")
        for field, rules in PROMPT_INPUT_SCHEMA["properties"].items():
            val = r.get(field)
            if val is None:
                continue
            if rules.get("type") == "string" and not isinstance(val, str):
                errors.append(f"{field}: expected string, got {type(val).__name__}")
            if "pattern" in rules and isinstance(val, str) and not re.match(rules["pattern"], val):
                errors.append(f"{field}: does not match pattern {rules['pattern']}")
            if "min_length" in rules and isinstance(val, str) and len(val) < rules["min_length"]:
                errors.append(f"{field}: too short")
        if errors:
            invalid.append({"record": r.get("doc_id", "unknown"), "errors": errors})
        else:
            valid.append(r)

    # Quarantine invalid records
    if invalid:
        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        qpath = os.path.join(QUARANTINE_DIR, f"{ts}.jsonl")
        with open(qpath, "w") as f:
            for inv in invalid:
                f.write(json.dumps(inv) + "\n")

    total = len(records)
    violation_rate = len(invalid) / max(total, 1)
    return {
        "check_type": "prompt_input_schema",
        "status": "FAIL" if violation_rate > 0.05 else "PASS",
        "total_records": total,
        "valid": len(valid),
        "invalid": len(invalid),
        "violation_rate": round(violation_rate, 4),
        "quarantine_path": os.path.join(QUARANTINE_DIR, f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.jsonl") if invalid else None,
        "message": f"{len(invalid)}/{total} records failed prompt input validation"
    }


# --- Extension 3: Structured LLM Output Enforcement ---

VERDICT_SCHEMA = {
    "required": ["verdict_id", "overall_verdict", "overall_score", "confidence"],
    "properties": {
        "verdict_id": {"type": "string"},
        "overall_verdict": {"type": "string", "enum": ["PASS", "FAIL", "WARN"]},
        "overall_score": {"type": "number"},
        "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
    }
}


def check_llm_output_schema(data_path, schema=None):
    """Extension 3: Validate LLM structured outputs against expected schema."""
    schema = schema or VERDICT_SCHEMA
    records = load_jsonl(data_path)
    violations = []
    for r in records:
        errors = []
        for field in schema["required"]:
            if field not in r or r[field] is None:
                errors.append(f"missing: {field}")
        for field, rules in schema["properties"].items():
            val = r.get(field)
            if val is None:
                continue
            if rules.get("type") == "string" and not isinstance(val, str):
                errors.append(f"{field}: expected string")
            if rules.get("type") == "number" and not isinstance(val, (int, float)):
                errors.append(f"{field}: expected number")
            if "enum" in rules and val not in rules["enum"]:
                errors.append(f"{field}: {val} not in {rules['enum']}")
            if "minimum" in rules and isinstance(val, (int, float)) and val < rules["minimum"]:
                errors.append(f"{field}: {val} < {rules['minimum']}")
            if "maximum" in rules and isinstance(val, (int, float)) and val > rules["maximum"]:
                errors.append(f"{field}: {val} > {rules['maximum']}")
        if errors:
            violations.append({"record_id": r.get("verdict_id", "unknown"), "errors": errors})

    total = len(records)
    violation_rate = len(violations) / max(total, 1)

    # Load baseline for trend detection
    baseline_rate = 0.0
    if os.path.exists(METRICS_PATH):
        with open(METRICS_PATH) as f:
            prev = json.load(f)
            baseline_rate = prev.get("baseline_violation_rate", 0.0)

    trend = "rising" if violation_rate > baseline_rate * 1.2 and baseline_rate > 0 else "stable"
    status = "FAIL" if violation_rate > 0.15 else ("WARN" if trend == "rising" or violation_rate > 0.05 else "PASS")

    return {
        "check_type": "llm_output_schema",
        "status": status,
        "total_outputs": total,
        "schema_violations": len(violations),
        "violation_rate": round(violation_rate, 4),
        "baseline_violation_rate": round(baseline_rate, 4),
        "trend": trend,
        "message": f"{len(violations)}/{total} LLM outputs failed schema validation (trend: {trend})"
    }


def run_all():
    """Run all AI extensions on available data."""
    results = {}

    # Extension 1: Embedding drift on Week 3 extractions
    w3_path = os.path.join(BASE_DIR, "outputs/week3/extractions.jsonl")
    if os.path.exists(w3_path):
        print("Extension 1: Embedding drift on Week 3 extractions...")
        results["embedding_drift"] = check_embedding_drift(w3_path)
        print(f"  {results['embedding_drift']['status']}: drift={results['embedding_drift']['drift_score']}")

    # Extension 2: Prompt input validation on Week 3
    if os.path.exists(w3_path):
        print("Extension 2: Prompt input schema validation on Week 3...")
        results["prompt_input_schema"] = check_prompt_input_schema(w3_path)
        print(f"  {results['prompt_input_schema']['status']}: {results['prompt_input_schema']['message']}")

    # Extension 3: LLM output schema on Week 2 verdicts
    w2_path = os.path.join(BASE_DIR, "outputs/week2/verdicts.jsonl")
    if os.path.exists(w2_path):
        print("Extension 3: LLM output schema enforcement on Week 2 verdicts...")
        results["llm_output_schema"] = check_llm_output_schema(w2_path)
        print(f"  {results['llm_output_schema']['status']}: {results['llm_output_schema']['message']}")

    # Write metrics
    metrics = {
        "run_date": datetime.now(timezone.utc).isoformat(),
        "prompt_hash": hashlib.sha256(json.dumps(PROMPT_INPUT_SCHEMA).encode()).hexdigest()[:16],
        **{k: v for k, v in results.items()},
    }
    if "llm_output_schema" in results:
        metrics["total_outputs"] = results["llm_output_schema"]["total_outputs"]
        metrics["schema_violations"] = results["llm_output_schema"]["schema_violations"]
        metrics["violation_rate"] = results["llm_output_schema"]["violation_rate"]
        metrics["trend"] = results["llm_output_schema"]["trend"]
        metrics["baseline_violation_rate"] = results["llm_output_schema"]["violation_rate"]

    os.makedirs(os.path.dirname(METRICS_PATH), exist_ok=True)
    with open(METRICS_PATH, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"\nAI metrics written to: {METRICS_PATH}")

    return results


def main():
    parser = argparse.ArgumentParser(description="AI Contract Extensions")
    parser.add_argument("--all", action="store_true", help="Run all extensions")
    parser.add_argument("--embedding-drift", help="Path to JSONL for embedding drift check")
    parser.add_argument("--prompt-validation", help="Path to JSONL for prompt input validation")
    parser.add_argument("--output-schema", help="Path to JSONL for LLM output schema check")
    args = parser.parse_args()

    if args.all:
        run_all()
    elif args.embedding_drift:
        r = check_embedding_drift(args.embedding_drift)
        print(json.dumps(r, indent=2))
    elif args.prompt_validation:
        r = check_prompt_input_schema(args.prompt_validation)
        print(json.dumps(r, indent=2))
    elif args.output_schema:
        r = check_llm_output_schema(args.output_schema)
        print(json.dumps(r, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
