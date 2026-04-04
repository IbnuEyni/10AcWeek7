#!/usr/bin/env python3
"""Generate sample JSONL data for all 5 weeks + LangSmith traces.
Also generates a 'violated' week3 dataset with confidence in 0-100 range.
"""
import json, uuid, random, hashlib, os
from datetime import datetime, timedelta, timezone

random.seed(42)
BASE = os.path.dirname(os.path.abspath(__file__))

def uid():
    return str(uuid.uuid4())

def ts(days_ago=0, hours_ago=0):
    dt = datetime.now(timezone.utc) - timedelta(days=days_ago, hours=hours_ago)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def sha256(text):
    return hashlib.sha256(text.encode()).hexdigest()

def write_jsonl(path, records):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    print(f"  Wrote {len(records)} records to {path}")

# --- Week 1: Intent-Code Correlator ---
def gen_week1(n=55):
    files = ["src/main.py", "src/utils.py", "src/auth.py", "src/billing.py",
             "src/models.py", "src/api.py", "src/config.py", "src/db.py"]
    tags_pool = ["auth", "pii", "billing", "logging", "validation", "api", "config"]
    symbols = ["process_request", "validate_input", "authenticate_user",
               "calculate_total", "load_config", "query_db", "transform_data"]
    records = []
    for i in range(n):
        line_start = random.randint(1, 200)
        records.append({
            "intent_id": uid(),
            "description": f"Intent {i+1}: {random.choice(['Handle user authentication', 'Process billing data', 'Validate API input', 'Transform document', 'Load configuration', 'Query database records', 'Generate report'])}",
            "code_refs": [
                {
                    "file": random.choice(files),
                    "line_start": line_start,
                    "line_end": line_start + random.randint(5, 40),
                    "symbol": random.choice(symbols),
                    "confidence": round(random.uniform(0.6, 0.99), 2)
                }
                for _ in range(random.randint(1, 3))
            ],
            "governance_tags": random.sample(tags_pool, random.randint(1, 3)),
            "created_at": ts(days_ago=random.randint(0, 14))
        })
    write_jsonl(os.path.join(BASE, "outputs/week1/intent_records.jsonl"), records)

# --- Week 2: Digital Courtroom ---
def gen_week2(n=55):
    criteria = ["clarity", "completeness", "accuracy", "consistency", "relevance"]
    verdicts = ["PASS", "FAIL", "WARN"]
    records = []
    for i in range(n):
        scores = {}
        for c in random.sample(criteria, random.randint(3, 5)):
            scores[c] = {
                "score": random.randint(1, 5),
                "evidence": [f"Evidence excerpt {j+1} for {c}" for j in range(random.randint(1, 3))],
                "notes": f"Assessment notes for {c}"
            }
        score_vals = [s["score"] for s in scores.values()]
        overall = round(sum(score_vals) / len(score_vals), 1)
        records.append({
            "verdict_id": uid(),
            "target_ref": f"src/{random.choice(['main', 'utils', 'auth', 'billing'])}.py",
            "rubric_id": sha256(f"rubric_v{random.randint(1,3)}"),
            "rubric_version": f"{random.randint(1,2)}.{random.randint(0,5)}.0",
            "scores": scores,
            "overall_verdict": random.choice(verdicts),
            "overall_score": overall,
            "confidence": round(random.uniform(0.7, 0.99), 2),
            "evaluated_at": ts(days_ago=random.randint(0, 14))
        })
    write_jsonl(os.path.join(BASE, "outputs/week2/verdicts.jsonl"), records)

# --- Week 3: Document Refinery ---
def gen_week3(n=55, violated=False):
    entity_types = ["PERSON", "ORG", "LOCATION", "DATE", "AMOUNT", "OTHER"]
    models = ["claude-3-5-sonnet-20241022", "claude-3-haiku-20240307", "gpt-4o-2024-05-13"]
    records = []
    for i in range(n):
        entities = [
            {
                "entity_id": uid(),
                "name": random.choice(["Acme Corp", "Jane Doe", "New York", "2025-01-15", "$5000", "Widget"]),
                "type": random.choice(entity_types),
                "canonical_value": random.choice(["acme_corp", "jane_doe", "new_york", "2025-01-15", "5000", "widget"])
            }
            for _ in range(random.randint(1, 4))
        ]
        entity_ids = [e["entity_id"] for e in entities]
        facts = [
            {
                "fact_id": uid(),
                "text": f"Extracted fact {j+1}: {random.choice(['Revenue increased by 15%', 'Contract signed on Jan 15', 'Located in New York', 'Total amount is $5000', 'Approved by Jane Doe'])}",
                "entity_refs": random.sample(entity_ids, min(random.randint(1, 2), len(entity_ids))),
                "confidence": round(random.uniform(50, 99), 1) if violated else round(random.uniform(0.6, 0.99), 2),
                "page_ref": random.choice([None, random.randint(1, 20)]),
                "source_excerpt": f"Verbatim excerpt {j+1} from source document"
            }
            for j in range(random.randint(1, 5))
        ]
        records.append({
            "doc_id": uid(),
            "source_path": f"/data/documents/doc_{i+1}.pdf",
            "source_hash": sha256(f"doc_{i+1}_content"),
            "extracted_facts": facts,
            "entities": entities,
            "extraction_model": random.choice(models),
            "processing_time_ms": random.randint(200, 5000),
            "token_count": {"input": random.randint(1000, 8000), "output": random.randint(200, 2000)},
            "extracted_at": ts(days_ago=random.randint(0, 14))
        })
    path = "outputs/week3/extractions_violated.jsonl" if violated else "outputs/week3/extractions.jsonl"
    write_jsonl(os.path.join(BASE, path), records)

# --- Week 4: Brownfield Cartographer ---
def gen_week4(n=10):
    node_types = ["FILE", "TABLE", "SERVICE", "MODEL", "PIPELINE", "EXTERNAL"]
    rel_types = ["IMPORTS", "CALLS", "READS", "WRITES", "PRODUCES", "CONSUMES"]
    languages = ["python", "javascript", "sql", "yaml"]
    files = ["src/main.py", "src/utils.py", "src/auth.py", "src/billing.py",
             "src/models.py", "src/api.py", "src/config.py", "src/db.py",
             "src/week3/extractor.py", "src/week4/cartographer.py",
             "src/week5/event_store.py", "src/week2/courtroom.py"]
    records = []
    for i in range(n):
        nodes = [
            {
                "node_id": f"file::{f}",
                "type": "FILE",
                "label": os.path.basename(f),
                "metadata": {
                    "path": f,
                    "language": random.choice(languages),
                    "purpose": f"Handles {random.choice(['authentication', 'data processing', 'API routing', 'configuration', 'extraction', 'event sourcing'])}",
                    "last_modified": ts(days_ago=random.randint(0, 30))
                }
            }
            for f in files
        ]
        # Add some non-file nodes
        nodes.append({"node_id": "table::extractions", "type": "TABLE", "label": "extractions",
                       "metadata": {"path": "outputs/week3/extractions.jsonl", "language": "jsonl",
                                    "purpose": "Week 3 extraction output", "last_modified": ts(days_ago=1)}})
        nodes.append({"node_id": "service::week3-refinery", "type": "SERVICE", "label": "week3-refinery",
                       "metadata": {"path": "src/week3/", "language": "python",
                                    "purpose": "Document extraction service", "last_modified": ts(days_ago=1)}})
        nodes.append({"node_id": "service::week4-cartographer", "type": "SERVICE", "label": "week4-cartographer",
                       "metadata": {"path": "src/week4/", "language": "python",
                                    "purpose": "Lineage mapping service", "last_modified": ts(days_ago=1)}})

        node_ids = [n["node_id"] for n in nodes]
        edges = []
        for _ in range(random.randint(8, 20)):
            src = random.choice(node_ids)
            tgt = random.choice([nid for nid in node_ids if nid != src])
            edges.append({
                "source": src,
                "target": tgt,
                "relationship": random.choice(rel_types),
                "confidence": round(random.uniform(0.7, 1.0), 2)
            })
        # Add key cross-week edges
        edges.append({"source": "service::week3-refinery", "target": "table::extractions",
                       "relationship": "PRODUCES", "confidence": 0.99})
        edges.append({"source": "table::extractions", "target": "service::week4-cartographer",
                       "relationship": "CONSUMES", "confidence": 0.98})
        edges.append({"source": "file::src/week3/extractor.py", "target": "table::extractions",
                       "relationship": "WRITES", "confidence": 0.97})

        records.append({
            "snapshot_id": uid(),
            "codebase_root": os.path.abspath(BASE),
            "git_commit": sha256(f"commit_{i}")[:40],
            "nodes": nodes,
            "edges": edges,
            "captured_at": ts(days_ago=i)
        })
    write_jsonl(os.path.join(BASE, "outputs/week4/lineage_snapshots.jsonl"), records)

# --- Week 5: Event Sourcing Platform ---
def gen_week5(n=60):
    event_types = ["DocumentProcessed", "ExtractionCompleted", "VerdictRendered",
                   "LineageUpdated", "ContractValidated", "UserAuthenticated"]
    agg_types = ["Document", "Extraction", "Verdict", "Lineage", "Contract", "User"]
    sources = ["week1-correlator", "week2-courtroom", "week3-document-refinery",
               "week4-cartographer", "week5-event-store"]
    # Group events by aggregate for monotonic sequence numbers
    aggregates = {}
    records = []
    for i in range(n):
        etype_idx = random.randint(0, len(event_types) - 1)
        agg_id = uid() if random.random() > 0.5 else random.choice(list(aggregates.keys())) if aggregates else uid()
        if agg_id not in aggregates:
            aggregates[agg_id] = 0
        aggregates[agg_id] += 1
        occurred = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 14), hours=random.randint(0, 23))
        recorded = occurred + timedelta(seconds=random.randint(0, 5))
        records.append({
            "event_id": uid(),
            "event_type": event_types[etype_idx],
            "aggregate_id": agg_id,
            "aggregate_type": agg_types[etype_idx],
            "sequence_number": aggregates[agg_id],
            "payload": {"detail": f"Event payload for {event_types[etype_idx]}", "value": random.randint(1, 100)},
            "metadata": {
                "causation_id": random.choice([uid(), None]),
                "correlation_id": uid(),
                "user_id": f"user_{random.randint(1, 10)}",
                "source_service": random.choice(sources)
            },
            "schema_version": "1.0",
            "occurred_at": occurred.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "recorded_at": recorded.strftime("%Y-%m-%dT%H:%M:%SZ")
        })
    write_jsonl(os.path.join(BASE, "outputs/week5/events.jsonl"), records)

# --- LangSmith Traces ---
def gen_traces(n=55):
    run_types = ["llm", "chain", "tool", "retriever", "embedding"]
    names = ["extraction_chain", "verdict_chain", "embedding_lookup",
             "claude-3-5-sonnet", "gpt-4o", "retriever_v2", "tool_executor"]
    tags_pool = ["week1", "week2", "week3", "week4", "week5", "extraction", "verdict", "lineage"]
    records = []
    for i in range(n):
        prompt_tokens = random.randint(500, 8000)
        completion_tokens = random.randint(100, 3000)
        start = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 14), hours=random.randint(0, 23))
        end = start + timedelta(seconds=random.randint(1, 30))
        records.append({
            "id": uid(),
            "name": random.choice(names),
            "run_type": random.choice(run_types),
            "inputs": {"prompt": f"Sample input {i+1}"},
            "outputs": {"response": f"Sample output {i+1}"},
            "error": random.choice([None, None, None, "TimeoutError: request timed out"]),
            "start_time": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_time": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "total_tokens": prompt_tokens + completion_tokens,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_cost": round((prompt_tokens * 0.000003 + completion_tokens * 0.000015), 4),
            "tags": random.sample(tags_pool, random.randint(1, 3)),
            "parent_run_id": random.choice([uid(), None]),
            "session_id": uid()
        })
    write_jsonl(os.path.join(BASE, "outputs/traces/runs.jsonl"), records)


if __name__ == "__main__":
    print("Generating sample data...")
    gen_week1()
    gen_week2()
    gen_week3(violated=False)
    gen_week3(violated=True)  # violated version
    gen_week4()
    gen_week5()
    gen_traces()
    print("\nDone! All sample data generated.")
