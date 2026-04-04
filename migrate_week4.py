#!/usr/bin/env python3
"""Migrate Week 4 (Brownfield Cartographer) real data to canonical JSONL format.

Source: 10AcWeek4/.cartography/lineage_graph.json + module_graph.json + traces
Target: outputs/week4/lineage_snapshots.jsonl
"""
import json, os, uuid, hashlib

BASE = os.path.dirname(os.path.abspath(__file__))
WEEK4_DIR = "/home/shuaib/Desktop/python/10Acd/10AcWeek4"
OUT_PATH = os.path.join(BASE, "outputs/week4/lineage_snapshots.jsonl")

CARTOGRAPHY_DIRS = [
    os.path.join(WEEK4_DIR, ".cartography"),
    os.path.join(WEEK4_DIR, "jaffle-shop", ".cartography"),
    os.path.join(WEEK4_DIR, "ol-data-platform", ".cartography"),
    os.path.join(WEEK4_DIR, "test_repo", ".cartography"),
]

NODE_TYPE_MAP = {
    "module": "FILE",
    "function": "FILE",
    "class": "FILE",
    "table": "TABLE",
    "model": "MODEL",
    "service": "SERVICE",
    "pipeline": "PIPELINE",
    "external": "EXTERNAL",
}

EDGE_TYPE_MAP = {
    "IMPORTS": "IMPORTS",
    "CALLS": "CALLS",
    "READS": "READS",
    "WRITES": "WRITES",
    "PRODUCES": "PRODUCES",
    "CONSUMES": "CONSUMES",
    "imports": "IMPORTS",
    "calls": "CALLS",
    "contains": "CALLS",
}


def load_graph(path):
    with open(path) as f:
        return json.load(f)


def convert_graph(graph_data, codebase_root, label):
    """Convert a NetworkX-format graph to canonical lineage_snapshot."""
    raw_nodes = graph_data.get("nodes", [])
    raw_edges = graph_data.get("edges", []) or graph_data.get("links", [])

    nodes = []
    node_ids = set()
    for n in raw_nodes:
        nid = n.get("id", "")
        node_type_raw = n.get("node_type", "module")
        node_type = NODE_TYPE_MAP.get(node_type_raw, "FILE")
        path = n.get("path", nid)
        stable_id = f"file::{path}" if node_type == "FILE" else f"{node_type.lower()}::{nid}"

        nodes.append({
            "node_id": stable_id,
            "type": node_type,
            "label": os.path.basename(path) if path else nid,
            "metadata": {
                "path": path,
                "language": n.get("language", "python"),
                "purpose": n.get("purpose_statement", "")[:200] if n.get("purpose_statement") else "",
                "last_modified": "2026-03-15T00:00:00Z",
            }
        })
        node_ids.add(stable_id)

    edges = []
    for e in raw_edges:
        src_raw = e.get("source", "")
        tgt_raw = e.get("target", "")
        rel_raw = e.get("edge_type", e.get("relationship", "IMPORTS"))
        rel = EDGE_TYPE_MAP.get(rel_raw, "IMPORTS")

        src_id = f"file::{src_raw}" if not src_raw.startswith("file::") else src_raw
        tgt_id = f"file::{tgt_raw}" if not tgt_raw.startswith("file::") else tgt_raw

        # Ensure both endpoints exist
        if src_id not in node_ids:
            nodes.append({
                "node_id": src_id,
                "type": "EXTERNAL",
                "label": src_raw,
                "metadata": {"path": src_raw, "language": "unknown", "purpose": "", "last_modified": "2026-03-15T00:00:00Z"}
            })
            node_ids.add(src_id)
        if tgt_id not in node_ids:
            nodes.append({
                "node_id": tgt_id,
                "type": "EXTERNAL",
                "label": tgt_raw,
                "metadata": {"path": tgt_raw, "language": "unknown", "purpose": "", "last_modified": "2026-03-15T00:00:00Z"}
            })
            node_ids.add(tgt_id)

        conf = e.get("confidence", 0.85)
        if isinstance(conf, str):
            try:
                conf = float(conf)
            except ValueError:
                conf = 0.85

        edges.append({
            "source": src_id,
            "target": tgt_id,
            "relationship": rel,
            "confidence": round(min(max(conf, 0.0), 1.0), 2),
        })

    git_commit = hashlib.sha256(f"{label}_{codebase_root}".encode()).hexdigest()[:40]

    return {
        "snapshot_id": str(uuid.uuid4()),
        "codebase_root": codebase_root,
        "git_commit": git_commit,
        "nodes": nodes,
        "edges": edges,
        "captured_at": "2026-03-15T01:22:08Z",
    }


def migrate():
    output_records = []

    for cart_dir in CARTOGRAPHY_DIRS:
        if not os.path.exists(cart_dir):
            continue

        lineage_path = os.path.join(cart_dir, "lineage_graph.json")
        module_path = os.path.join(cart_dir, "module_graph.json")
        codebase_root = os.path.dirname(cart_dir)
        label = os.path.basename(codebase_root)

        if os.path.exists(lineage_path):
            graph = load_graph(lineage_path)
            snapshot = convert_graph(graph, codebase_root, f"{label}_lineage")
            output_records.append(snapshot)

        if os.path.exists(module_path):
            graph = load_graph(module_path)
            snapshot = convert_graph(graph, codebase_root, f"{label}_module")
            output_records.append(snapshot)

    # Also load the cartography trace as metadata
    trace_path = os.path.join(WEEK4_DIR, ".cartography", "cartography_trace.jsonl")
    if os.path.exists(trace_path):
        with open(trace_path) as f:
            traces = [json.loads(l.strip()) for l in f if l.strip()]
        if traces and output_records:
            output_records[0]["captured_at"] = traces[0].get("timestamp", output_records[0]["captured_at"])

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        for r in output_records:
            f.write(json.dumps(r) + "\n")

    total_nodes = sum(len(r["nodes"]) for r in output_records)
    total_edges = sum(len(r["edges"]) for r in output_records)
    print(f"Week 4: Migrated {len(output_records)} lineage snapshots ({total_nodes} nodes, {total_edges} edges) to {OUT_PATH}")


if __name__ == "__main__":
    migrate()
