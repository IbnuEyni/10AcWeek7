#!/usr/bin/env python3
"""ViolationAttributor: Traces contract violations to upstream commits via lineage + git blame.

Usage:
    python contracts/attributor.py --violation-report validation_reports/week3_violated.json
    python contracts/attributor.py --check-id "week3-document-refinery-extractions.extracted_facts[*].confidence.range"
"""
import argparse, json, os, re, subprocess, uuid
from datetime import datetime, timezone, timedelta
from collections import deque
import yaml

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REGISTRY_PATH = os.path.join(BASE_DIR, "contract_registry", "subscriptions.yaml")


def load_jsonl(path):
    records = []
    full = os.path.join(BASE_DIR, path) if not os.path.isabs(path) else path
    with open(full) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def load_registry():
    """Load contract_registry/subscriptions.yaml."""
    if not os.path.exists(REGISTRY_PATH):
        return []
    with open(REGISTRY_PATH) as f:
        data = yaml.safe_load(f)
    return data.get("subscriptions", [])


def registry_blast_radius(contract_id, failing_field, registry):
    """Step 1: Query registry for subscribers affected by a breaking field change."""
    affected = []
    for sub in registry:
        if sub.get("contract_id") != contract_id:
            continue
        breaking = [bf["field"] if isinstance(bf, dict) else bf
                    for bf in sub.get("breaking_fields", [])]
        # Match if failing_field starts with any breaking field prefix
        is_breaking = any(
            failing_field == bf or failing_field.startswith(bf.split("[")[0])
            for bf in breaking
        )
        affected.append({
            "subscriber_id": sub["subscriber_id"],
            "subscriber_team": sub.get("subscriber_team", ""),
            "fields_consumed": sub.get("fields_consumed", []),
            "is_breaking": is_breaking,
            "validation_mode": sub.get("validation_mode", "AUDIT"),
            "contact": sub.get("contact", ""),
        })
    return affected


def load_lineage_graph():
    """Load the latest Week 4 lineage snapshot."""
    path = os.path.join(BASE_DIR, "outputs/week4/lineage_snapshots.jsonl")
    if not os.path.exists(path):
        return {"nodes": [], "edges": []}
    records = load_jsonl(path)
    if not records:
        return {"nodes": [], "edges": []}
    # Use the most recent snapshot (last record)
    return records[-1]


def find_upstream_nodes(graph, failing_node_id):
    """BFS upstream from failing node through the lineage graph."""
    nodes_by_id = {n["node_id"]: n for n in graph.get("nodes", [])}
    # Build reverse adjacency (target -> sources)
    reverse_adj = {}
    for edge in graph.get("edges", []):
        tgt = edge["target"]
        src = edge["source"]
        if tgt not in reverse_adj:
            reverse_adj[tgt] = []
        reverse_adj[tgt].append({"node_id": src, "relationship": edge["relationship"],
                                  "confidence": edge.get("confidence", 0.5)})

    visited = set()
    queue = deque([(failing_node_id, 0)])
    upstream = []

    while queue:
        node_id, hops = queue.popleft()
        if node_id in visited:
            continue
        visited.add(node_id)
        if node_id != failing_node_id and node_id in nodes_by_id:
            node = nodes_by_id[node_id]
            upstream.append({"node_id": node_id, "node": node, "hops": hops})
        for neighbor in reverse_adj.get(node_id, []):
            if neighbor["node_id"] not in visited:
                queue.append((neighbor["node_id"], hops + 1))

    return upstream


def find_downstream_nodes(graph, failing_node_id):
    """BFS downstream from failing node for blast radius."""
    nodes_by_id = {n["node_id"]: n for n in graph.get("nodes", [])}
    adj = {}
    for edge in graph.get("edges", []):
        src = edge["source"]
        tgt = edge["target"]
        if src not in adj:
            adj[src] = []
        adj[src].append(tgt)

    visited = set()
    queue = deque([failing_node_id])
    downstream = []

    while queue:
        node_id = queue.popleft()
        if node_id in visited:
            continue
        visited.add(node_id)
        if node_id != failing_node_id:
            downstream.append(node_id)
        for neighbor in adj.get(node_id, []):
            if neighbor not in visited:
                queue.append(neighbor)

    return downstream


def git_log_file(file_path, since_days=14):
    """Get recent git commits for a file."""
    try:
        result = subprocess.run(
            ["git", "log", "--follow", f"--since={since_days} days ago",
             "--format=%H|%an|%ae|%ai|%s", "--", file_path],
            capture_output=True, text=True, cwd=BASE_DIR, timeout=10
        )
        commits = []
        for line in result.stdout.strip().split("\n"):
            if not line or "|" not in line:
                continue
            parts = line.split("|", 4)
            if len(parts) >= 5:
                commits.append({
                    "commit_hash": parts[0],
                    "author_name": parts[1],
                    "author_email": parts[2],
                    "timestamp": parts[3].strip(),
                    "message": parts[4],
                })
        return commits
    except Exception:
        return []


def git_blame_lines(file_path, line_start=1, line_end=10):
    """Get git blame for specific lines."""
    try:
        result = subprocess.run(
            ["git", "blame", "-L", f"{line_start},{line_end}", "--porcelain", file_path],
            capture_output=True, text=True, cwd=BASE_DIR, timeout=10
        )
        return result.stdout
    except Exception:
        return ""


def compute_confidence(commit, hops):
    """Confidence = 1.0 - (days_since_commit * 0.1) - (hops * 0.2), clamped to [0.05, 1.0]."""
    try:
        # Parse commit timestamp
        ts_str = commit.get("timestamp", "")
        # Handle git date format like "2025-01-14 09:00:00 +0000"
        ts_clean = re.sub(r'\s*[+-]\d{4}$', '', ts_str)
        commit_dt = datetime.strptime(ts_clean, "%Y-%m-%d %H:%M:%S")
        days = (datetime.now() - commit_dt).days
    except Exception:
        days = 7
    score = 1.0 - (days * 0.1) - (hops * 0.2)
    return round(max(0.05, min(1.0, score)), 2)


def attribute_violation(check_result, graph, registry=None):
    """Attribute a single violation to upstream commits."""
    check_id = check_result.get("check_id", "")
    column = check_result.get("column_name", "")

    # Determine which node to start from based on contract/check
    # Map contract prefixes to likely lineage nodes
    node_mapping = {
        "week3": ["table::extractions", "service::week3-refinery", "file::src/week3/extractor.py"],
        "week4": ["service::week4-cartographer", "file::src/week4/cartographer.py"],
        "week5": ["file::src/week5/event_store.py"],
        "week2": ["file::src/week2/courtroom.py"],
        "week1": ["file::src/main.py"],
    }

    starting_nodes = []
    for prefix, nodes in node_mapping.items():
        if prefix in check_id:
            starting_nodes = nodes
            break

    # Find upstream nodes via lineage
    all_upstream = []
    for start_node in starting_nodes:
        upstream = find_upstream_nodes(graph, start_node)
        all_upstream.extend(upstream)

    # Step 1: Registry blast radius (primary source)
    registry = registry or []
    contract_id = check_id.split(".")[0] if "." in check_id else check_id
    # Reconstruct contract_id from check_id prefix (e.g. week3-document-refinery-extractions)
    # check_id format: {contract_id}.{column}.{check_type}
    parts = check_id.split(".")
    # contract_id is everything before the column name (heuristic: first part with dashes)
    cid_parts = []
    for p in parts:
        if "-" in p or p.startswith("week") or p.startswith("langsmith"):
            cid_parts.append(p)
        else:
            break
    contract_id = ".".join(cid_parts) if cid_parts else parts[0]
    # failing field is the column_name from the check result
    failing_field = check_result.get("column_name", "")
    registry_subscribers = registry_blast_radius(contract_id, failing_field, registry)
    registry_affected_ids = [s["subscriber_id"] for s in registry_subscribers]

    # Step 2: Lineage graph downstream traversal (enrichment)
    downstream = []
    for start_node in starting_nodes:
        downstream.extend(find_downstream_nodes(graph, start_node))
    downstream = list(set(downstream))

    # Get git blame for upstream files
    blame_chain = []
    seen_commits = set()

    # Also check the direct source files
    source_files = []
    for node in all_upstream:
        meta = node.get("node", {}).get("metadata", {})
        path = meta.get("path", "")
        if path and path.endswith(".py"):
            source_files.append((path, node.get("hops", 1)))

    # Add direct files from starting nodes
    for sn in starting_nodes:
        if "::" in sn:
            path = sn.split("::", 1)[1]
            if path.endswith(".py"):
                source_files.append((path, 0))

    for file_path, hops in source_files:
        commits = git_log_file(file_path)
        for commit in commits:
            if commit["commit_hash"] in seen_commits:
                continue
            seen_commits.add(commit["commit_hash"])
            blame_chain.append({
                "rank": len(blame_chain) + 1,
                "file_path": file_path,
                "commit_hash": commit["commit_hash"],
                "author": commit["author_email"],
                "commit_timestamp": commit["timestamp"],
                "commit_message": commit["message"],
                "confidence_score": compute_confidence(commit, hops),
            })

    # Sort by confidence descending, limit to 5
    blame_chain.sort(key=lambda x: x["confidence_score"], reverse=True)
    blame_chain = blame_chain[:5]
    for i, entry in enumerate(blame_chain):
        entry["rank"] = i + 1

    # Ensure at least 1 candidate
    if not blame_chain:
        blame_chain.append({
            "rank": 1,
            "file_path": "unknown",
            "commit_hash": "0" * 40,
            "author": "unknown",
            "commit_timestamp": datetime.now(timezone.utc).isoformat(),
            "commit_message": "No git history found for upstream files",
            "confidence_score": 0.05,
        })

    # Infer affected pipelines from downstream nodes
    affected_pipelines = []
    for d in downstream:
        if "service::" in d:
            affected_pipelines.append(d.replace("service::", "") + "-pipeline")
        elif "file::" in d:
            affected_pipelines.append(d.replace("file::", ""))

    return {
        "violation_id": str(uuid.uuid4()),
        "check_id": check_id,
        "detected_at": datetime.now(timezone.utc).isoformat(),
        "blame_chain": blame_chain,
        "blast_radius": {
            "registry_subscribers": registry_subscribers,
            "affected_subscriber_ids": registry_affected_ids,
            "affected_nodes": downstream[:10],
            "affected_pipelines": affected_pipelines[:5],
            "estimated_records": check_result.get("records_failing", 0),
            "contamination_depth": len(downstream),
        },
    }


def main():
    parser = argparse.ArgumentParser(description="ViolationAttributor: Trace violations to commits")
    parser.add_argument("--violation-report", help="Path to validation report JSON")
    parser.add_argument("--check-id", help="Specific check_id to attribute")
    parser.add_argument("--output", default="violation_log/violations.jsonl", help="Output JSONL path")
    args = parser.parse_args()

    graph = load_lineage_graph()
    registry = load_registry()
    violations = []

    if args.violation_report:
        report_path = os.path.join(BASE_DIR, args.violation_report) if not os.path.isabs(args.violation_report) else args.violation_report
        with open(report_path) as f:
            report = json.load(f)
        failed_checks = [r for r in report.get("results", []) if r["status"] in ("FAIL", "ERROR")]
        if args.check_id:
            failed_checks = [r for r in failed_checks if r["check_id"] == args.check_id]
        for check in failed_checks:
            print(f"Attributing: {check['check_id']}...")
            violation = attribute_violation(check, graph, registry)
            violations.append(violation)
            print(f"  Blame chain: {len(violation['blame_chain'])} candidates")
            print(f"  Blast radius: {len(violation['blast_radius']['affected_nodes'])} affected nodes")
    else:
        parser.print_help()
        return

    # Write violations
    out_path = os.path.join(BASE_DIR, args.output) if not os.path.isabs(args.output) else args.output
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    # Append to existing file
    with open(out_path, "a") as f:
        for v in violations:
            f.write(json.dumps(v) + "\n")

    print(f"\nWrote {len(violations)} violations to {out_path}")

    # Print summary
    for v in violations:
        print(f"\n--- Violation: {v['check_id']} ---")
        for b in v["blame_chain"][:3]:
            print(f"  #{b['rank']} {b['file_path']} | {b['commit_hash'][:12]} | {b['author']} | conf={b['confidence_score']}")
            print(f"       \"{b['commit_message']}\"")
        br = v["blast_radius"]
        print(f"  Blast radius: {len(br['affected_nodes'])} nodes, {br['estimated_records']} records")


if __name__ == "__main__":
    main()
