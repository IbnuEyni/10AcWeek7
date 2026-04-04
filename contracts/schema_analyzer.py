#!/usr/bin/env python3
"""SchemaEvolutionAnalyzer: Diffs schema snapshots and classifies changes.

Usage:
    python contracts/schema_analyzer.py --contract-id week3-document-refinery-extractions --since "7 days ago"
    python contracts/schema_analyzer.py --snapshot-a schema_snapshots/week3/.../a.yaml --snapshot-b schema_snapshots/week3/.../b.yaml
"""
import argparse, json, os, glob, yaml, uuid
from datetime import datetime, timezone, timedelta

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Change classification taxonomy
CHANGE_TAXONOMY = {
    "add_nullable_column": {"backward_compatible": True, "action": "None. Downstream consumers can ignore the new column."},
    "add_required_column": {"backward_compatible": False, "action": "Coordinate with all producers. Provide a default or migration script."},
    "remove_column": {"backward_compatible": False, "action": "Deprecation period mandatory (minimum 2 sprints). Blast radius report required."},
    "rename_column": {"backward_compatible": False, "action": "Deprecation period with alias column. Notify all downstream consumers."},
    "type_widening": {"backward_compatible": True, "action": "Validate no precision loss. Re-run statistical checks."},
    "type_narrowing": {"backward_compatible": False, "action": "CRITICAL. Requires explicit migration plan with rollback."},
    "enum_addition": {"backward_compatible": True, "action": "Additive: notify all consumers."},
    "enum_removal": {"backward_compatible": False, "action": "Treat as breaking change. Deprecation period required."},
    "stats_shift": {"backward_compatible": True, "action": "Investigate root cause. May indicate upstream data quality issue."},
}

TYPE_HIERARCHY = {"null": 0, "boolean": 1, "integer": 2, "number": 3, "string": 4, "array": 5, "object": 6}


def load_snapshot(path):
    with open(path) as f:
        return yaml.safe_load(f)


def get_snapshots(contract_id, since_days=7):
    """Get all snapshots for a contract, sorted by timestamp."""
    snap_dir = os.path.join(BASE_DIR, "schema_snapshots", contract_id)
    if not os.path.exists(snap_dir):
        return []
    files = sorted(glob.glob(os.path.join(snap_dir, "*.yaml")))
    return files


def classify_type_change(old_type, new_type):
    old_rank = TYPE_HIERARCHY.get(old_type, -1)
    new_rank = TYPE_HIERARCHY.get(new_type, -1)
    if new_rank > old_rank:
        return "type_widening"
    elif new_rank < old_rank:
        return "type_narrowing"
    return None


def diff_snapshots(snap_a, snap_b):
    """Diff two schema snapshots and classify changes."""
    cols_a = snap_a.get("columns", {})
    cols_b = snap_b.get("columns", {})
    keys_a = set(cols_a.keys())
    keys_b = set(cols_b.keys())

    changes = []

    # Added columns
    for col in keys_b - keys_a:
        col_info = cols_b[col]
        nullable = col_info.get("null_fraction", 0) > 0
        change_type = "add_nullable_column" if nullable else "add_required_column"
        changes.append({
            "column": col,
            "change_type": change_type,
            "old_value": None,
            "new_value": col_info.get("type", "unknown"),
            "backward_compatible": CHANGE_TAXONOMY[change_type]["backward_compatible"],
            "required_action": CHANGE_TAXONOMY[change_type]["action"],
        })

    # Removed columns
    for col in keys_a - keys_b:
        changes.append({
            "column": col,
            "change_type": "remove_column",
            "old_value": cols_a[col].get("type", "unknown"),
            "new_value": None,
            "backward_compatible": False,
            "required_action": CHANGE_TAXONOMY["remove_column"]["action"],
        })

    # Modified columns
    for col in keys_a & keys_b:
        old = cols_a[col]
        new = cols_b[col]

        # Type change
        if old.get("type") != new.get("type"):
            tc = classify_type_change(old.get("type", ""), new.get("type", ""))
            if tc:
                changes.append({
                    "column": col,
                    "change_type": tc,
                    "old_value": old.get("type"),
                    "new_value": new.get("type"),
                    "backward_compatible": CHANGE_TAXONOMY[tc]["backward_compatible"],
                    "required_action": CHANGE_TAXONOMY[tc]["action"],
                })

        # Statistical shift
        old_stats = old.get("stats", {})
        new_stats = new.get("stats", {})
        if old_stats and new_stats:
            old_mean = old_stats.get("mean", 0)
            new_mean = new_stats.get("mean", 0)
            old_std = old_stats.get("stddev", 1)
            if old_std > 0 and abs(new_mean - old_mean) / old_std > 2:
                changes.append({
                    "column": col,
                    "change_type": "stats_shift",
                    "old_value": f"mean={old_mean:.4f}, std={old_std:.4f}",
                    "new_value": f"mean={new_mean:.4f}, std={new_stats.get('stddev', 0):.4f}",
                    "backward_compatible": True,
                    "required_action": CHANGE_TAXONOMY["stats_shift"]["action"],
                    "deviation_sigma": round(abs(new_mean - old_mean) / old_std, 2),
                })

        # Cardinality change (potential enum change)
        old_card = old.get("cardinality", 0)
        new_card = new.get("cardinality", 0)
        if old_card > 0 and new_card > old_card and old_card < 20:
            changes.append({
                "column": col,
                "change_type": "enum_addition",
                "old_value": f"cardinality={old_card}",
                "new_value": f"cardinality={new_card}",
                "backward_compatible": True,
                "required_action": CHANGE_TAXONOMY["enum_addition"]["action"],
            })
        elif old_card > 0 and new_card < old_card and old_card < 20:
            changes.append({
                "column": col,
                "change_type": "enum_removal",
                "old_value": f"cardinality={old_card}",
                "new_value": f"cardinality={new_card}",
                "backward_compatible": False,
                "required_action": CHANGE_TAXONOMY["enum_removal"]["action"],
            })

    return changes


def generate_migration_report(contract_id, snap_a_path, snap_b_path, changes):
    """Generate migration impact report for breaking changes."""
    breaking = [c for c in changes if not c["backward_compatible"]]
    has_breaking = len(breaking) > 0

    report = {
        "report_id": str(uuid.uuid4()),
        "contract_id": contract_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_before": snap_a_path,
        "snapshot_after": snap_b_path,
        "compatibility_verdict": "BREAKING" if has_breaking else "COMPATIBLE",
        "total_changes": len(changes),
        "breaking_changes": len(breaking),
        "compatible_changes": len(changes) - len(breaking),
        "changes": changes,
        "migration_checklist": [],
        "rollback_plan": {},
    }

    if has_breaking:
        checklist = []
        for i, bc in enumerate(breaking):
            checklist.append({
                "step": i + 1,
                "action": f"Address {bc['change_type']} on column '{bc['column']}'",
                "detail": bc["required_action"],
                "column": bc["column"],
                "change_type": bc["change_type"],
            })
        checklist.append({
            "step": len(checklist) + 1,
            "action": "Run ValidationRunner on all downstream consumers",
            "detail": "Verify no contract violations after migration",
        })
        checklist.append({
            "step": len(checklist) + 1,
            "action": "Update schema snapshots and baselines",
            "detail": "Re-run ContractGenerator to establish new baselines",
        })
        report["migration_checklist"] = checklist

        report["rollback_plan"] = {
            "description": "Revert to previous schema version if migration fails",
            "steps": [
                "Revert the upstream code change via git revert",
                "Re-run data pipeline with previous schema",
                "Validate all downstream consumers pass contract checks",
                "Update violation log with rollback event",
            ],
            "estimated_downtime": "< 30 minutes if automated, < 2 hours if manual",
        }

    return report


def main():
    parser = argparse.ArgumentParser(description="SchemaEvolutionAnalyzer: Diff and classify schema changes")
    parser.add_argument("--contract-id", help="Contract ID to analyze")
    parser.add_argument("--since", default="7 days ago", help="Time window for analysis")
    parser.add_argument("--snapshot-a", help="Path to first snapshot (older)")
    parser.add_argument("--snapshot-b", help="Path to second snapshot (newer)")
    parser.add_argument("--output", help="Output path for evolution report")
    args = parser.parse_args()

    if args.snapshot_a and args.snapshot_b:
        snap_a = load_snapshot(args.snapshot_a)
        snap_b = load_snapshot(args.snapshot_b)
        contract_id = snap_a.get("contract_id", args.contract_id or "unknown")
        changes = diff_snapshots(snap_a, snap_b)
        report = generate_migration_report(contract_id, args.snapshot_a, args.snapshot_b, changes)
    elif args.contract_id:
        snapshots = get_snapshots(args.contract_id)
        if len(snapshots) < 2:
            print(f"Need at least 2 snapshots for {args.contract_id}. Found {len(snapshots)}.")
            print("Generating a second snapshot with injected changes for demo...")
            # Create a modified snapshot to demonstrate evolution
            if snapshots:
                snap_a = load_snapshot(snapshots[-1])
                snap_b = json.loads(json.dumps(snap_a))  # deep copy
                snap_b["captured_at"] = datetime.now(timezone.utc).isoformat()
                # Inject a breaking change: confidence type narrowing
                if "extracted_facts[*].confidence" in snap_b.get("columns", {}):
                    col = snap_b["columns"]["extracted_facts[*].confidence"]
                    col["type"] = "integer"
                    if "stats" in col:
                        col["stats"]["mean"] = 73.5
                        col["stats"]["min"] = 50.0
                        col["stats"]["max"] = 99.0
                # Inject an added column
                snap_b["columns"]["extraction_version"] = {
                    "type": "string", "null_fraction": 0.0, "cardinality": 3
                }
                # Save the modified snapshot
                snap_dir = os.path.join(BASE_DIR, "schema_snapshots", args.contract_id)
                ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                new_snap_path = os.path.join(snap_dir, f"{ts}_evolved.yaml")
                with open(new_snap_path, "w") as f:
                    yaml.dump(snap_b, f, default_flow_style=False, sort_keys=False)
                snapshots.append(new_snap_path)
                print(f"  Created evolved snapshot: {new_snap_path}")

        if len(snapshots) >= 2:
            snap_a = load_snapshot(snapshots[-2])
            snap_b = load_snapshot(snapshots[-1])
            changes = diff_snapshots(snap_a, snap_b)
            report = generate_migration_report(args.contract_id, snapshots[-2], snapshots[-1], changes)
        else:
            print("Still not enough snapshots.")
            return
    else:
        parser.print_help()
        return

    # Output
    if args.output:
        out_path = os.path.join(BASE_DIR, args.output) if not os.path.isabs(args.output) else args.output
    else:
        os.makedirs(os.path.join(BASE_DIR, "validation_reports"), exist_ok=True)
        cid = report.get("contract_id", "unknown")
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
        out_path = os.path.join(BASE_DIR, "validation_reports", f"schema_evolution_{cid}_{ts}.json")

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"\nSchema evolution report: {out_path}")
    print(f"  Compatibility: {report['compatibility_verdict']}")
    print(f"  Total changes: {report['total_changes']}")
    print(f"  Breaking: {report['breaking_changes']}")
    print(f"  Compatible: {report['compatible_changes']}")
    for c in report["changes"]:
        compat = "✓" if c["backward_compatible"] else "✗ BREAKING"
        print(f"  [{compat}] {c['change_type']}: {c['column']} ({c['old_value']} → {c['new_value']})")
    if report.get("migration_checklist"):
        print("\n  Migration checklist:")
        for step in report["migration_checklist"]:
            print(f"    {step['step']}. {step['action']}")


if __name__ == "__main__":
    main()
