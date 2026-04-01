#!/usr/bin/env python3
"""ReportGenerator: Auto-generates the Enforcer Report from live validation data.

Usage:
    python contracts/report_generator.py
"""
import json, os, glob
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_validation_reports():
    reports = []
    for path in glob.glob(os.path.join(BASE_DIR, "validation_reports", "*.json")):
        if "schema_evolution" in path or "ai_metrics" in path:
            continue
        with open(path) as f:
            reports.append(json.load(f))
    return reports


def load_violations():
    path = os.path.join(BASE_DIR, "violation_log", "violations.jsonl")
    if not os.path.exists(path):
        return []
    violations = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("{\"_comment"):
                violations.append(json.loads(line))
    return violations


def load_ai_metrics():
    path = os.path.join(BASE_DIR, "validation_reports", "ai_metrics.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def compute_health_score(reports):
    total_checks = sum(r.get("total_checks", 0) for r in reports)
    passed = sum(r.get("passed", 0) for r in reports)
    if total_checks == 0:
        return 100
    base = (passed / total_checks) * 100
    critical_count = 0
    for r in reports:
        for res in r.get("results", []):
            if res.get("severity") == "CRITICAL" and res.get("status") == "FAIL":
                critical_count += 1
    return max(0, round(base - (critical_count * 20)))


def get_top_violations(reports, n=3):
    failures = []
    for r in reports:
        for res in r.get("results", []):
            if res.get("status") in ("FAIL", "ERROR"):
                failures.append({
                    "contract": r.get("contract_id", "unknown"),
                    "check_id": res.get("check_id", ""),
                    "column": res.get("column_name", ""),
                    "severity": res.get("severity", ""),
                    "message": res.get("message", ""),
                    "records_failing": res.get("records_failing", 0),
                })
    severity_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    failures.sort(key=lambda x: severity_order.get(x["severity"], 9))
    return failures[:n]


def generate_plain_description(violation):
    col = violation["column"]
    contract = violation["contract"]
    msg = violation["message"]
    count = violation["records_failing"]
    system = contract.split("-")[0] if "-" in contract else contract
    return (
        f"The {system} system's '{col}' field has a {violation['severity']} violation: "
        f"{msg} This affects {count} records and may impact downstream consumers "
        f"that depend on this field."
    )


def main():
    reports = load_validation_reports()
    violations = load_violations()
    ai_metrics = load_ai_metrics()

    health_score = compute_health_score(reports)
    top_violations = get_top_violations(reports)

    total_checks = sum(r.get("total_checks", 0) for r in reports)
    total_passed = sum(r.get("passed", 0) for r in reports)
    total_failed = sum(r.get("failed", 0) for r in reports)

    # Severity counts from violations
    severity_counts = {}
    for v in top_violations:
        sev = v["severity"]
        severity_counts[sev] = severity_counts.get(sev, 0) + 1

    # Build report
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
                    "plain_description": generate_plain_description(v),
                }
                for v in top_violations
            ],
        },
        "schema_changes_detected": {
            "summary": "Schema snapshots are tracked in schema_snapshots/. Run schema_analyzer.py to detect changes.",
            "changes": [],
        },
        "ai_system_risk_assessment": {
            "embedding_drift": ai_metrics.get("embedding_drift", {"status": "NOT_RUN"}),
            "prompt_input_schema": ai_metrics.get("prompt_input_schema", {"status": "NOT_RUN"}),
            "llm_output_schema": ai_metrics.get("llm_output_schema", {"status": "NOT_RUN"}),
            "overall_ai_risk": "LOW" if all(
                ai_metrics.get(k, {}).get("status") in ("PASS", "NOT_RUN", None)
                for k in ["embedding_drift", "prompt_input_schema", "llm_output_schema"]
            ) else "ELEVATED",
        },
        "recommended_actions": [],
    }

    # Generate recommended actions from top violations
    for i, v in enumerate(top_violations):
        report["recommended_actions"].append({
            "priority": i + 1,
            "action": f"Fix {v['severity']} violation in {v['contract']}: {v['check_id']}",
            "detail": v["message"],
        })
    if not report["recommended_actions"]:
        report["recommended_actions"].append({
            "priority": 1,
            "action": "All checks passing. Continue monitoring for statistical drift.",
            "detail": "No immediate action required.",
        })

    # Write report
    out_dir = os.path.join(BASE_DIR, "enforcer_report")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "report_data.json")
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"Enforcer Report generated: {out_path}")
    print(f"  Data Health Score: {health_score}/100")
    print(f"  Total checks: {total_checks}, Passed: {total_passed}, Failed: {total_failed}")
    print(f"  Violations logged: {len(violations)}")
    for i, v in enumerate(top_violations):
        print(f"  Top {i+1}: [{v['severity']}] {v['check_id']}")


if __name__ == "__main__":
    main()
