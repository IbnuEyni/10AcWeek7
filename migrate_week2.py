#!/usr/bin/env python3
"""Migrate Week 2 (Digital Courtroom) real data to canonical JSONL format.

Source: 10Acweek2/automatoin-auditor/audit/streamlit_runs/*.md + rubric
Target: outputs/week2/verdicts.jsonl
"""
import json, os, re, uuid, hashlib

BASE = os.path.dirname(os.path.abspath(__file__))
WEEK2_DIR = "/home/shuaib/Desktop/python/10Acd/10Acweek2/automatoin-auditor"
OUT_PATH = os.path.join(BASE, "outputs/week2/verdicts.jsonl")


def parse_audit_report(filepath):
    """Parse a markdown audit report into verdict records."""
    with open(filepath) as f:
        content = f.read()

    records = []

    # Extract repo URL
    repo_match = re.search(r'\*\*Repository\*\*:\s*(.+)', content)
    target_ref = repo_match.group(1).strip() if repo_match else filepath

    # Extract date
    date_match = re.search(r'\*\*Report Date\*\*:\s*(.+)', content)
    report_date = date_match.group(1).strip() if date_match else "2026-02-28T00:00:00Z"
    if "T" not in report_date:
        report_date = report_date.replace(" ", "T") + "Z"

    # Extract overall score
    overall_match = re.search(r'\*\*Overall Score\*\*:\s*([\d.]+)/5\.0', content)
    overall_score = float(overall_match.group(1)) if overall_match else 3.0

    # Parse criterion sections
    criterion_pattern = re.compile(
        r'### (.+?)\n\n\*\*Final Score\*\*:\s*(\d)/5',
        re.MULTILINE
    )

    scores = {}
    for match in criterion_pattern.finditer(content):
        criterion_name = match.group(1).strip().lower().replace(" ", "_")
        score_val = int(match.group(2))

        # Extract evidence from the section after this match
        section_start = match.end()
        next_section = content.find("\n### ", section_start)
        if next_section == -1:
            next_section = len(content)
        section_text = content[section_start:next_section]

        # Extract opinion arguments as evidence
        evidence = []
        arg_matches = re.findall(r'- \*\*Argument\*\*:\s*(.+?)(?=\n- \*\*)', section_text, re.DOTALL)
        for arg in arg_matches[:3]:
            evidence.append(arg.strip()[:200])

        scores[criterion_name] = {
            "score": score_val,
            "evidence": evidence if evidence else [f"Score {score_val}/5 for {criterion_name}"],
            "notes": f"Evaluated by multi-agent courtroom system"
        }

    if not scores:
        return []

    # Determine overall verdict
    if overall_score >= 4.0:
        verdict = "PASS"
    elif overall_score >= 2.5:
        verdict = "WARN"
    else:
        verdict = "FAIL"

    # Compute rubric hash
    rubric_path = os.path.join(WEEK2_DIR, "rubric", "auditor_rubric.json")
    if os.path.exists(rubric_path):
        with open(rubric_path, "rb") as f:
            rubric_id = hashlib.sha256(f.read()).hexdigest()
    else:
        rubric_id = hashlib.sha256(b"default_rubric").hexdigest()

    # Confidence based on judge agreement
    score_vals = [s["score"] for s in scores.values()]
    score_std = (sum((s - overall_score)**2 for s in score_vals) / max(len(score_vals), 1)) ** 0.5
    confidence = round(max(0.5, 1.0 - score_std * 0.1), 2)

    records.append({
        "verdict_id": str(uuid.uuid4()),
        "target_ref": target_ref,
        "rubric_id": rubric_id,
        "rubric_version": "3.0.0",
        "scores": scores,
        "overall_verdict": verdict,
        "overall_score": overall_score,
        "confidence": confidence,
        "evaluated_at": report_date,
    })

    return records


def migrate():
    all_records = []

    # Process streamlit runs
    runs_dir = os.path.join(WEEK2_DIR, "audit", "streamlit_runs")
    if os.path.exists(runs_dir):
        for fname in sorted(os.listdir(runs_dir)):
            if fname.endswith(".md"):
                path = os.path.join(runs_dir, fname)
                records = parse_audit_report(path)
                all_records.extend(records)

    # Process self-generated reports
    for subdir in ["report_onself_generated", "report_onpeer_generated"]:
        rdir = os.path.join(WEEK2_DIR, "audit", subdir)
        if os.path.exists(rdir):
            for fname in sorted(os.listdir(rdir)):
                if fname.endswith(".md"):
                    path = os.path.join(rdir, fname)
                    records = parse_audit_report(path)
                    all_records.extend(records)

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        for r in all_records:
            f.write(json.dumps(r) + "\n")

    print(f"Week 2: Migrated {len(all_records)} verdict records to {OUT_PATH}")


if __name__ == "__main__":
    migrate()
