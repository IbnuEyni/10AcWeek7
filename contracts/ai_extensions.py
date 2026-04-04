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


VIOLATION_LOG_PATH = os.path.join(BASE_DIR, "violation_log", "violations.jsonl")


def load_jsonl(path):
    records = []
    full = os.path.join(BASE_DIR, path) if not os.path.isabs(path) else path
    with open(full) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def write_ai_violation(check_result):
    """Write WARN/FAIL AI extension results to violation_log/violations.jsonl."""
    if check_result.get("status") not in ("WARN", "FAIL"):
        return
    entry = {
        "violation_id": str(uuid.uuid4()),
        "check_id": f"ai-extensions.{check_result.get('check_type', 'unknown')}",
        "detected_at": datetime.now(timezone.utc).isoformat(),
        "check_type": check_result.get("check_type"),
        "status": check_result.get("status"),
        "message": check_result.get("message", ""),
        "blame_chain": [],
        "blast_radius": {
            "registry_subscribers": [],
            "affected_nodes": [],
            "estimated_records": check_result.get("invalid", check_result.get("schema_violations", 0)),
        },
        "source": "ai_extensions",
    }
    os.makedirs(os.path.dirname(VIOLATION_LOG_PATH), exist_ok=True)
    with open(VIOLATION_LOG_PATH, "a") as f:
        f.write(json.dumps(entry) + "\n")




def _embed_sample(texts, n=200):
    """Embed texts using OpenAI text-embedding-3-small. Falls back to character-hash if no API key."""
    sample = texts[:n]
    api_key = os.environ.get("OPENAI_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")
    if api_key and os.environ.get("OPENAI_API_KEY"):
        try:
            import openai
            client = openai.OpenAI(api_key=os.environ["OPENAI_API_KEY"])
            resp = client.embeddings.create(input=sample, model="text-embedding-3-small")
            return np.array([e.embedding for e in resp.data]), "text-embedding-3-small"
        except Exception as e:
            print(f"  OpenAI embedding failed ({e}), falling back to character-hash")
    # Fallback: deterministic character-frequency hash (no API needed)
    vectors = np.array([_simple_text_vector(t) for t in sample])
    return vectors, "character-hash-64d"


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

    vectors, embed_method = _embed_sample(texts, n=sample_size)
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
        "sample_size": len(vectors),
        "embedding_method": embed_method,
        "message": f"Embedding drift={drift:.4f} ({'exceeds' if drift > threshold else 'within'} threshold {threshold}) [{embed_method}]"
    }


# --- Extension 2: Prompt Input Schema Validation ---

PROMPT_INPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["doc_id", "source_path", "content_preview"],
    "properties": {
        "doc_id":          {"type": "string", "minLength": 36, "maxLength": 36},
        "source_path":     {"type": "string", "minLength": 1},
        "content_preview": {"type": "string", "maxLength": 8000},
    },
    "additionalProperties": False,
}


def check_prompt_input_schema(data_path):
    """Extension 2: Validate records against prompt input JSON Schema. Quarantine failures."""
    records = load_jsonl(data_path)
    valid, invalid = [], []
    for r in records:
        errors = []
        # Required fields
        for field in PROMPT_INPUT_SCHEMA["required"]:
            if field not in r or r[field] is None:
                errors.append(f"missing required field: {field}")
        # Property constraints
        for field, rules in PROMPT_INPUT_SCHEMA["properties"].items():
            val = r.get(field)
            if val is None:
                continue
            if rules.get("type") == "string" and not isinstance(val, str):
                errors.append(f"{field}: expected string, got {type(val).__name__}")
            if "minLength" in rules and isinstance(val, str) and len(val) < rules["minLength"]:
                errors.append(f"{field}: length {len(val)} < minLength {rules['minLength']}")
            if "maxLength" in rules and isinstance(val, str) and len(val) > rules["maxLength"]:
                errors.append(f"{field}: length {len(val)} > maxLength {rules['maxLength']}")
        # additionalProperties: False only applies to the prompt input subset
        # Week 3 records are extraction outputs; we validate only the fields
        # that would be interpolated into the prompt (doc_id, source_path, content_preview)
        # We do NOT flag extra fields — the full record has many more fields by design.
        if errors:
            invalid.append({"record": r.get("doc_id", "unknown"), "errors": errors})
        else:
            valid.append(r)

    # Quarantine invalid records — never silently drop
    quarantine_path = None
    if invalid:
        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        quarantine_path = os.path.join(QUARANTINE_DIR, f"prompt_input_{ts}.jsonl")
        with open(quarantine_path, "w") as f:
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
        "quarantine_path": quarantine_path,
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


def check_output_schema_violation_rate(verdict_records, baseline_rate=None, warn_threshold=0.02):
    """Extension 3: Track LLM output schema violation rate. Spec-compliant signature."""
    total = len(verdict_records)
    violations = sum(1 for v in verdict_records
                     if v.get("overall_verdict") not in ("PASS", "FAIL", "WARN"))
    rate = violations / max(total, 1)
    trend = "unknown"
    if baseline_rate is not None:
        trend = "rising" if rate > baseline_rate * 1.5 else "stable"
    return {
        "total_outputs": total,
        "schema_violations": violations,
        "violation_rate": round(rate, 4),
        "trend": trend,
        "status": "WARN" if rate > warn_threshold else "PASS",
        "baseline_rate": baseline_rate,
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

    # Load persisted baseline rate (written on first run, never overwritten)
    baseline_rate = None
    if os.path.exists(METRICS_PATH):
        with open(METRICS_PATH) as f:
            prev = json.load(f)
        baseline_rate = prev.get("baseline_violation_rate")

    result = check_output_schema_violation_rate(records, baseline_rate=baseline_rate)
    result["check_type"] = "llm_output_schema"
    result["message"] = f"{result['schema_violations']}/{result['total_outputs']} LLM outputs failed schema validation (trend: {result['trend']})"
    return result


def run_all():
    """Run all AI extensions on available data."""
    results = {}

    # Extension 1: Embedding drift on Week 3 extractions
    w3_path = os.path.join(BASE_DIR, "outputs/week3/extractions.jsonl")
    if os.path.exists(w3_path):
        print("Extension 1: Embedding drift on Week 3 extractions...")
        results["embedding_drift"] = check_embedding_drift(w3_path)
        print(f"  {results['embedding_drift']['status']}: drift={results['embedding_drift']['drift_score']}")
        write_ai_violation(results["embedding_drift"])

    # Extension 2: Prompt input validation on Week 3
    if os.path.exists(w3_path):
        print("Extension 2: Prompt input schema validation on Week 3...")
        results["prompt_input_schema"] = check_prompt_input_schema(w3_path)
        print(f"  {results['prompt_input_schema']['status']}: {results['prompt_input_schema']['message']}")
        write_ai_violation(results["prompt_input_schema"])

    # Extension 3: LLM output schema on Week 2 verdicts
    w2_path = os.path.join(BASE_DIR, "outputs/week2/verdicts.jsonl")
    if os.path.exists(w2_path):
        print("Extension 3: LLM output schema enforcement on Week 2 verdicts...")
        results["llm_output_schema"] = check_llm_output_schema(w2_path)
        print(f"  {results['llm_output_schema']['status']}: {results['llm_output_schema']['message']}")
        write_ai_violation(results["llm_output_schema"])

    # Write metrics — baseline_violation_rate is written once and preserved
    existing_baseline = None
    if os.path.exists(METRICS_PATH):
        with open(METRICS_PATH) as f:
            existing_baseline = json.load(f).get("baseline_violation_rate")

    metrics = {
        "run_date": datetime.now(timezone.utc).isoformat(),
        "prompt_hash": hashlib.sha256(json.dumps(PROMPT_INPUT_SCHEMA, sort_keys=True).encode()).hexdigest()[:16],
        **{k: v for k, v in results.items()},
    }
    if "llm_output_schema" in results:
        r = results["llm_output_schema"]
        metrics["total_outputs"] = r["total_outputs"]
        metrics["schema_violations"] = r["schema_violations"]
        metrics["violation_rate"] = r["violation_rate"]
        metrics["trend"] = r["trend"]
        # Preserve baseline: only set on first run
        metrics["baseline_violation_rate"] = existing_baseline if existing_baseline is not None else r["violation_rate"]

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
