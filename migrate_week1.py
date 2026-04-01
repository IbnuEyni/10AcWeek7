#!/usr/bin/env python3
"""Migrate Week 1 (Intent-Code Correlator) real data to canonical JSONL format.

Source: 10AcdWeek1/Roo-Code/.orchestration/agent_trace.jsonl + active_intents.yaml
Target: outputs/week1/intent_records.jsonl
"""
import json, os, uuid, yaml, hashlib
from datetime import datetime

BASE = os.path.dirname(os.path.abspath(__file__))
WEEK1_DIR = "/home/shuaib/Desktop/python/10Acd/10AcdWeek1/Roo-Code/.orchestration"
OUT_PATH = os.path.join(BASE, "outputs/week1/intent_records.jsonl")

TOOL_TO_TAG = {
    "write_to_file": ["mutation", "code-gen"],
    "apply_diff": ["refactor", "mutation"],
    "read_file": ["analysis"],
    "search_files": ["analysis", "navigation"],
    "execute_command": ["tooling", "build"],
    "list_files": ["navigation"],
    "select_active_intent": ["orchestration"],
    "attempt_completion": ["orchestration"],
    "update_todo_list": ["planning"],
    "ask_followup_question": ["interaction"],
}


def load_intents():
    path = os.path.join(WEEK1_DIR, "active_intents.yaml")
    with open(path) as f:
        return yaml.safe_load(f)


def load_traces():
    path = os.path.join(WEEK1_DIR, "agent_trace.jsonl")
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def migrate():
    intents = load_intents()
    traces = load_traces()

    intent_map = {}
    for intent in intents:
        intent_map[intent["id"]] = intent

    output_records = []
    for trace in traces:
        files = trace.get("files", [])
        if not files:
            continue

        # Build code_refs from files
        code_refs = []
        related_intent = None
        for fentry in files:
            rel_path = fentry.get("relative_path", "unknown")
            for conv in fentry.get("conversations", []):
                for rel in conv.get("related", []):
                    if rel.get("type") == "specification":
                        related_intent = rel.get("value")
                for rng in conv.get("ranges", []):
                    start = rng.get("start_line", 1)
                    end = rng.get("end_line", 1)
                    if end > 100000:
                        end = start + 50
                    conf_raw = trace.get("result", "")
                    confidence = 0.85 if conf_raw == "success" else 0.4
                    code_refs.append({
                        "file": rel_path if rel_path != "unknown" else f"src/{trace.get('toolName', 'unknown')}.ts",
                        "line_start": max(start, 1),
                        "line_end": max(end, start),
                        "symbol": trace.get("toolName", "unknown"),
                        "confidence": round(confidence, 2),
                    })

        if not code_refs:
            continue

        # Build description from intent + tool
        tool = trace.get("toolName", "unknown")
        intent_name = related_intent or "general"
        intent_info = intent_map.get(intent_name, {})
        desc = intent_info.get("description", f"Agent action: {tool}")
        description = f"{desc} via {tool}"

        tags = TOOL_TO_TAG.get(tool, ["general"])
        if related_intent:
            tags.append(related_intent)

        ts = trace.get("timestamp", "")
        if ts.endswith("Z"):
            created_at = ts
        else:
            created_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        output_records.append({
            "intent_id": trace.get("id", str(uuid.uuid4())),
            "description": description,
            "code_refs": code_refs,
            "governance_tags": list(set(tags)),
            "created_at": created_at,
        })

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        for r in output_records:
            f.write(json.dumps(r) + "\n")

    print(f"Week 1: Migrated {len(output_records)} intent records to {OUT_PATH}")


if __name__ == "__main__":
    migrate()
