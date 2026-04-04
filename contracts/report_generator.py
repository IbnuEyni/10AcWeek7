#!/usr/bin/env python3
"""ReportGenerator: Auto-generates the Enforcer Report from live validation data.

Usage:
    python contracts/report_generator.py
"""
import json, os, glob, yaml
from datetime import datetime, timezone, timedelta

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REGISTRY_PATH = os.path.join(BASE_DIR, "contract_registry", "subscriptions.yaml")


def load_registry():
    if not os.path.exists(REGISTRY_PATH):
        return []
    with open(REGISTRY_PATH) as f:
        data = yaml.safe_load(f)
    return data.get("subscriptions", [])


def subscribers_for_contract(contract_id, registry):
    return [s["subscriber_id"] for s in registry if s.get("contract_id") == contract_id]


def load_validation_reports():
    """Load only the most recent report per contract to avoid double-counting."""
    by_contract = {}
    for path in sorted(glob.glob(os.path.join(BASE_DIR, "validation_reports", "*.json"))):
        fname = os.path.basename(path)
        if any(x in fname for x in ("schema_evolution", "migration_impact", "ai_metrics")):
            continue
        try:
            with open(path) as f:
                r = json.load(f)
            cid = r.get("contract_id", "unknown")
            # Keep the most recent (highest timestamp in filename)
            if cid not in by_contract or path > by_contract[cid]["_path"]:
                r["_path"] = path
                by_contract[cid] = r
        except Exception:
            pass
    return list(by_contract.values())


def load_violations():
    path = os.path.join(BASE_DIR, "violation_log", "violations.jsonl")
    if not os.path.exists(path):
        return []
    violations = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("{\"_comment"):
                try:
                    violations.append(json.loads(line))
                except Exception:
                    pass
    return violations


def load_ai_metrics():
    path = os.path.join(BASE_DIR, "validation_reports", "ai_metrics.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def load_schema_changes():
    """Load migration impact reports from the past 7 days."""
    changes = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    for path in glob.glob(os.path.join(BASE_DIR, "validation_reports", "migration_impact_*.json")):
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(path), tz=timezone.utc)
            if mtime < cutoff:
                continue
            with open(path) as f:
                r = json.load(f)
            for c in r.get("changes", []):
                changes.append({
                    "contract_id": r.get("contract_id", "unknown"),
                    "column": c.get("column", ""),
                    "change_type": c.get("change_type", ""),
                    "compatibility": "BREAKING" if not c.get("backward_compatible") else "COMPATIBLE",
                    "required_action": c.get("required_action", ""),
                    "old_value": c.get("old_value"),
                    "new_value": c.get("new_value"),
                })
        except Exception:
            pass
    return changes


def compute_health_score(reports):
    """Formula: (checks_passed / total_checks) * 100, minus 20 per CRITICAL violation."""
    total_checks = sum(r.get("total_checks", 0) for r in reports)
    passed = sum(r.get("passed", 0) for r in reports)
    if total_checks == 0:
        return 100
    base = (passed / total_checks) * 100
    critical_count = sum(
        1 for r in reports
        for res in r.get("results", [])
        if res.get("severity") == "CRITICAL" and res.get("status") == "FAIL"
    )
    return max(0, min(100, round(base - (critical_count * 20))))


def get_top_violations(reports, n=3):
    failures = []
    for r in reports:
        for res in r.get("results", []):
            if res.get("status") in ("FAIL", "ERROR"):
                failures.append({
                    "contract": r.get("contract_id", "unknown"),
                    "check_id": res.get("check_id", ""),
                    "column": res.get("column_name", ""),
                    "severity": res.get("severity", "LOW"),
                    "message": res.get("message", ""),
                    "records_failing": res.get("records_failing", 0),
                })
    severity_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    failures.sort(key=lambda x: severity_order.get(x["severity"], 9))
    return failures[:n]


def generate_plain_description(violation, registry):
    """Plain-language description naming the system, field, and downstream consumers."""
    col = violation["column"]
    contract = violation["contract"]
    msg = violation["message"]
    count = violation["records_failing"]
    # Map contract_id to human-readable system name
    system_names = {
        "week1-intent-code-correlator": "Week 1 Intent-Code Correlator",
        "week2-digital-courtroom-verdicts": "Week 2 Digital Courtroom",
        "week3-document-refinery-extractions": "Week 3 Document Refinery",
        "week4-brownfield-cartographer-lineage": "Week 4 Brownfield Cartographer",
        "week5-event-sourcing-platform-events": "Week 5 Event Sourcing Platform",
        "langsmith-trace-records": "LangSmith Trace Records",
    }
    system = system_names.get(contract, contract)
    subs = subscribers_for_contract(contract, registry)
    consumer_str = ", ".join(subs) if subs else "no registered downstream consumers"
    return (
        f"The {system}'s '{col}' field failed its {violation['severity']} check: {msg} "
        f"This affects {count} records. "
        f"Downstream consumers at risk: {consumer_str}."
    )


def generate_specific_actions(top_violations, schema_changes, ai_metrics):
    """Generate specific, actionable recommendations referencing real file paths."""
    actions = []

    for v in top_violations:
        contract = v["contract"]
        col = v["column"]
        check_id = v["check_id"]
        sev = v["severity"]

        # Map contract to source file
        source_files = {
            "week3-document-refinery-extractions": "src/week3/extractor.py",
            "week5-event-sourcing-platform-events": "src/week5/event_store.py",
            "week2-digital-courtroom-verdicts": "src/week2/courtroom.py",
            "week4-brownfield-cartographer-lineage": "src/week4/cartographer.py",
            "week1-intent-code-correlator": "src/week1/correlator.py",
        }
        src = source_files.get(contract, f"src/{contract.split('-')[0]}/")

        if "confidence" in col and "range" in check_id:
            actions.append({
                "priority": len(actions) + 1,
                "action": f"Update {src} to output confidence as float 0.0–1.0 "
                          f"per contract {contract} clause {col}.range",
                "detail": v["message"],
                "severity": sev,
            })
        elif "drift" in check_id:
            actions.append({
                "priority": len(actions) + 1,
                "action": f"Investigate statistical drift in {col} in {src}. "
                          f"Re-establish baseline in schema_snapshots/baselines.json "
                          f"after confirming the scale change is intentional.",
                "detail": v["message"],
                "severity": sev,
            })
        else:
            actions.append({
                "priority": len(actions) + 1,
                "action": f"Fix {sev} violation in {src}: {check_id}",
                "detail": v["message"],
                "severity": sev,
            })

    # Add schema change action if breaking changes detected
    breaking = [c for c in schema_changes if c["compatibility"] == "BREAKING"]
    if breaking and len(actions) < 3:
        b = breaking[0]
        actions.append({
            "priority": len(actions) + 1,
            "action": f"Address breaking schema change '{b['change_type']}' on "
                      f"'{b['column']}' in contract {b['contract_id']}. "
                      f"{b['required_action']}",
            "detail": f"Changed from {b['old_value']} to {b['new_value']}",
            "severity": "CRITICAL",
        })

    # Add AI risk action if elevated
    drift = ai_metrics.get("embedding_drift", {})
    if drift.get("status") in ("FAIL", "WARN") and len(actions) < 3:
        actions.append({
            "priority": len(actions) + 1,
            "action": "Investigate embedding drift in extracted_facts[*].text. "
                      "Re-run contracts/ai_extensions.py --embedding-drift after "
                      "confirming input distribution has not changed.",
            "detail": drift.get("message", ""),
            "severity": "HIGH",
        })

    if not actions:
        actions.append({
            "priority": 1,
            "action": "All checks passing. Schedule monthly baseline refresh: "
                      "delete schema_snapshots/baselines.json and re-run "
                      "contracts/runner.py on clean data to recalibrate thresholds.",
            "detail": "No immediate action required.",
            "severity": "LOW",
        })

    return actions[:3]


def main():
    registry = load_registry()
    reports = load_validation_reports()
    violations = load_violations()
    ai_metrics = load_ai_metrics()
    schema_changes = load_schema_changes()

    health_score = compute_health_score(reports)
    top_violations = get_top_violations(reports)

    total_checks = sum(r.get("total_checks", 0) for r in reports)
    total_passed = sum(r.get("passed", 0) for r in reports)
    total_failed = sum(r.get("failed", 0) for r in reports)

    severity_counts = {}
    for r in reports:
        for res in r.get("results", []):
            if res.get("status") == "FAIL":
                sev = res.get("severity", "LOW")
                severity_counts[sev] = severity_counts.get(sev, 0) + 1

    recommended_actions = generate_specific_actions(top_violations, schema_changes, ai_metrics)

    report = {
        "report_id": datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data_health_score": health_score,
        "health_narrative": (
            f"Data health score is {health_score}/100. "
            f"{total_passed}/{total_checks} checks passed across all contracts. "
            + (f"{total_failed} checks failed." if total_failed > 0 else "No failures detected.")
        ),
        "violations_this_week": {
            "total": len(violations),
            "by_severity": severity_counts,
            "top_violations": [
                {
                    "check_id": v["check_id"],
                    "severity": v["severity"],
                    "plain_description": generate_plain_description(v, registry),
                }
                for v in top_violations
            ],
        },
        "schema_changes_detected": {
            "summary": (
                f"{len(schema_changes)} schema change(s) detected in the past 7 days. "
                f"{len([c for c in schema_changes if c['compatibility'] == 'BREAKING'])} breaking."
                if schema_changes else
                "No schema changes detected in the past 7 days."
            ),
            "changes": schema_changes,
        },
        "ai_system_risk_assessment": {
            "embedding_drift": ai_metrics.get("embedding_drift", {"status": "NOT_RUN"}),
            "prompt_input_schema": ai_metrics.get("prompt_input_schema", {"status": "NOT_RUN"}),
            "llm_output_schema": ai_metrics.get("llm_output_schema", {"status": "NOT_RUN"}),
            "overall_ai_risk": "LOW" if all(
                ai_metrics.get(k, {}).get("status") in ("PASS", "BASELINE_SET", "NOT_RUN", None)
                for k in ["embedding_drift", "llm_output_schema"]
            ) else "ELEVATED",
        },
        "recommended_actions": recommended_actions,
    }

    out_dir = os.path.join(BASE_DIR, "enforcer_report")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "report_data.json")
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"Enforcer Report generated: {out_path}")
    print(f"  Data Health Score: {health_score}/100")
    print(f"  Total checks: {total_checks}, Passed: {total_passed}, Failed: {total_failed}")
    print(f"  Violations logged: {len(violations)}")
    print(f"  Schema changes detected: {len(schema_changes)}")
    for i, v in enumerate(top_violations):
        print(f"  Top {i+1}: [{v['severity']}] {v['check_id']}")


if __name__ == "__main__":
    main()
