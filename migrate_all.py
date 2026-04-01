#!/usr/bin/env python3
"""Master migration script: runs all week migrations and verifies output."""
import subprocess, sys, os, json

BASE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = [
    "migrate_week1.py",
    "migrate_week2.py",
    "migrate_week3.py",
    "migrate_week4.py",
    "migrate_week5.py",
    "migrate_traces.py",
]

EXPECTED_OUTPUTS = {
    "outputs/week1/intent_records.jsonl": 50,
    "outputs/week2/verdicts.jsonl": 5,
    "outputs/week3/extractions.jsonl": 50,
    "outputs/week4/lineage_snapshots.jsonl": 1,
    "outputs/week5/events.jsonl": 50,
    "outputs/traces/runs.jsonl": 10,
}


def main():
    print("=" * 60)
    print("  MASTER MIGRATION: Weeks 1-5 + Traces")
    print("=" * 60)

    for script in SCRIPTS:
        path = os.path.join(BASE, script)
        print(f"\nRunning {script}...")
        result = subprocess.run([sys.executable, path], capture_output=True, text=True)
        print(result.stdout.strip())
        if result.returncode != 0:
            print(f"  ERROR: {result.stderr.strip()}")

    print("\n" + "=" * 60)
    print("  VERIFICATION")
    print("=" * 60)

    all_ok = True
    for path, min_count in EXPECTED_OUTPUTS.items():
        full = os.path.join(BASE, path)
        if not os.path.exists(full):
            print(f"  MISSING: {path}")
            all_ok = False
            continue
        with open(full) as f:
            count = sum(1 for line in f if line.strip())
        status = "OK" if count >= min_count else "LOW"
        if status == "LOW":
            all_ok = False
        print(f"  {status}: {path} -> {count} records (min: {min_count})")

    print("\n" + ("ALL MIGRATIONS PASSED" if all_ok else "SOME MIGRATIONS NEED ATTENTION"))


if __name__ == "__main__":
    main()
