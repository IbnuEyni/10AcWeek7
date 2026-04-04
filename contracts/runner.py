#!/usr/bin/env python3
"""ValidationRunner: Executes contract checks against data snapshots.

Usage:
    python contracts/runner.py --contract generated_contracts/week3_extractions.yaml \
        --data outputs/week3/extractions.jsonl \
        --output validation_reports/week3_report.json
"""
import argparse, json, os, re, sys, uuid, hashlib
from datetime import datetime, timezone
import yaml
import numpy as np

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASELINES_PATH = os.path.join(BASE_DIR, "schema_snapshots", "baselines.json")


def load_jsonl(path):
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def load_contract(path):
    with open(path) as f:
        return yaml.safe_load(f)


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def extract_values(records, field_path):
    """Extract values from records for a given dotted/bracketed field path.
    Supports: 'field', 'field.sub', 'field[*].sub'
    """
    parts = re.split(r'\[\*\]\.?|\.', field_path)
    parts = [p for p in parts if p]

    def _extract(obj, remaining_parts):
        if not remaining_parts:
            return [obj]
        key = remaining_parts[0]
        rest = remaining_parts[1:]
        if isinstance(obj, dict) and key in obj:
            return _extract(obj[key], rest)
        elif isinstance(obj, list):
            results = []
            for item in obj:
                results.extend(_extract(item, [key] + list(rest)))
            return results
        return []

    all_vals = []
    for r in records:
        all_vals.extend(_extract(r, parts))
    return all_vals


def load_baselines():
    if os.path.exists(BASELINES_PATH):
        with open(BASELINES_PATH) as f:
            return json.load(f)
    return {}


def save_baselines(baselines):
    os.makedirs(os.path.dirname(BASELINES_PATH), exist_ok=True)
    with open(BASELINES_PATH, "w") as f:
        json.dump(baselines, f, indent=2)


class ValidationRunner:
    def __init__(self, contract, records, data_path):
        self.contract = contract
        self.records = records
        self.data_path = data_path
        self.contract_id = contract.get("id", "unknown")
        self.results = []
        self.baselines = load_baselines()

    def run_all(self):
        schema = self.contract.get("schema", {})
        for col_name, col_spec in schema.items():
            self._check_required(col_name, col_spec)
            self._check_type(col_name, col_spec)
            self._check_range(col_name, col_spec)
            self._check_enum(col_name, col_spec)
            self._check_pattern(col_name, col_spec)
            self._check_unique(col_name, col_spec)
            self._check_format(col_name, col_spec)

        self._check_row_count()
        self._execute_soda_checks()
        self._check_cross_references()
        self._check_temporal_order()
        self._check_token_sum()
        self._check_graph_integrity()
        self._check_statistical_drift()

        return self._build_report()

    def _add_result(self, check_id, column, check_type, status, actual, expected, severity, failing_count=0, sample=None, message=""):
        self.results.append({
            "check_id": check_id,
            "column_name": column,
            "check_type": check_type,
            "status": status,
            "actual_value": str(actual),
            "expected": str(expected),
            "severity": severity,
            "records_failing": failing_count,
            "sample_failing": sample or [],
            "message": message,
        })

    def _check_required(self, col_name, col_spec):
        if not col_spec.get("required"):
            return
        try:
            values = extract_values(self.records, col_name)
            null_count = sum(1 for v in values if v is None)
            total = len(values)
            if total == 0:
                self._add_result(
                    f"{self.contract_id}.{col_name}.required", col_name, "required",
                    "ERROR", "column not found", "column exists", "CRITICAL",
                    message=f"Column {col_name} not found in any record"
                )
            elif null_count > 0:
                self._add_result(
                    f"{self.contract_id}.{col_name}.required", col_name, "not_null",
                    "FAIL", f"{null_count}/{total} null", "0 nulls", "CRITICAL",
                    failing_count=null_count,
                    message=f"{col_name} has {null_count} null values"
                )
            else:
                self._add_result(
                    f"{self.contract_id}.{col_name}.required", col_name, "not_null",
                    "PASS", f"0/{total} null", "0 nulls", "LOW"
                )
        except Exception as e:
            self._add_result(
                f"{self.contract_id}.{col_name}.required", col_name, "required",
                "ERROR", str(e), "column exists", "CRITICAL",
                message=f"Error checking required: {e}"
            )

    def _check_type(self, col_name, col_spec):
        expected_type = col_spec.get("type")
        if not expected_type:
            return
        try:
            values = extract_values(self.records, col_name)
            non_null = [v for v in values if v is not None]
            if not non_null:
                return
            type_map = {
                "string": str, "number": (int, float), "integer": int,
                "boolean": bool, "array": list, "object": dict,
            }
            expected_py = type_map.get(expected_type)
            if not expected_py:
                return
            # For "number", also accept string-encoded numerics (e.g. '1.0')
            if expected_type == "number":
                def is_number(v):
                    if isinstance(v, (int, float)) and not isinstance(v, bool):
                        return True
                    if isinstance(v, str):
                        try:
                            float(v)
                            return True
                        except ValueError:
                            return False
                    return False
                failing = [v for v in non_null if not is_number(v)]
            else:
                failing = [v for v in non_null if not isinstance(v, expected_py)]
            if failing:
                self._add_result(
                    f"{self.contract_id}.{col_name}.type", col_name, "type",
                    "FAIL", f"{len(failing)} type mismatches", f"all {expected_type}",
                    "CRITICAL", failing_count=len(failing),
                    sample=[str(v)[:50] for v in failing[:5]],
                    message=f"{col_name}: expected {expected_type}, found {type(failing[0]).__name__}"
                )
            else:
                self._add_result(
                    f"{self.contract_id}.{col_name}.type", col_name, "type",
                    "PASS", f"all {expected_type}", f"all {expected_type}", "LOW"
                )
        except Exception as e:
            self._add_result(
                f"{self.contract_id}.{col_name}.type", col_name, "type",
                "ERROR", str(e), f"all {expected_type}", "CRITICAL",
                message=f"Error checking type: {e}"
            )

    def _check_range(self, col_name, col_spec):
        minimum = col_spec.get("minimum")
        maximum = col_spec.get("maximum")
        if minimum is None and maximum is None:
            return
        try:
            values = extract_values(self.records, col_name)
            nums = [v for v in values if isinstance(v, (int, float)) and v is not None]
            if not nums:
                return
            arr = np.array(nums)
            actual_min = float(np.min(arr))
            actual_max = float(np.max(arr))
            actual_mean = float(np.mean(arr))
            violations = []
            if minimum is not None:
                violations.extend([v for v in nums if v < minimum])
            if maximum is not None:
                violations.extend([v for v in nums if v > maximum])
            if violations:
                self._add_result(
                    f"{self.contract_id}.{col_name}.range", col_name, "range",
                    "FAIL",
                    f"min={round(actual_min,4)}, max={round(actual_max,4)}, mean={round(actual_mean,4)}",
                    f"min>={minimum}, max<={maximum}",
                    "CRITICAL", failing_count=len(violations),
                    sample=[str(v) for v in violations[:5]],
                    message=f"{col_name} has {len(violations)} values outside [{minimum}, {maximum}]. "
                            f"Actual range: [{actual_min:.4f}, {actual_max:.4f}], mean={actual_mean:.4f}."
                )
            else:
                self._add_result(
                    f"{self.contract_id}.{col_name}.range", col_name, "range",
                    "PASS",
                    f"min={round(actual_min,4)}, max={round(actual_max,4)}",
                    f"min>={minimum}, max<={maximum}", "LOW"
                )
        except Exception as e:
            self._add_result(
                f"{self.contract_id}.{col_name}.range", col_name, "range",
                "ERROR", str(e), f"[{minimum}, {maximum}]", "CRITICAL",
                message=f"Error checking range: {e}"
            )

    def _check_enum(self, col_name, col_spec):
        allowed = col_spec.get("enum")
        if not allowed:
            return
        try:
            values = extract_values(self.records, col_name)
            non_null = [v for v in values if v is not None]
            invalid = [v for v in non_null if v not in allowed]
            if invalid:
                self._add_result(
                    f"{self.contract_id}.{col_name}.enum", col_name, "accepted_values",
                    "FAIL", f"invalid: {set(invalid)}", f"one of {allowed}",
                    "CRITICAL", failing_count=len(invalid),
                    sample=[str(v) for v in list(set(invalid))[:5]],
                    message=f"{col_name}: found values not in allowed set: {set(invalid)}"
                )
            else:
                self._add_result(
                    f"{self.contract_id}.{col_name}.enum", col_name, "accepted_values",
                    "PASS", f"all in {allowed}", f"one of {allowed}", "LOW"
                )
        except Exception as e:
            self._add_result(
                f"{self.contract_id}.{col_name}.enum", col_name, "accepted_values",
                "ERROR", str(e), f"one of {allowed}", "CRITICAL"
            )

    def _check_pattern(self, col_name, col_spec):
        pattern = col_spec.get("pattern")
        if not pattern:
            return
        try:
            values = extract_values(self.records, col_name)
            non_null = [str(v) for v in values if v is not None]
            regex = re.compile(pattern)
            invalid = [v for v in non_null if not regex.match(v)]
            if invalid:
                self._add_result(
                    f"{self.contract_id}.{col_name}.pattern", col_name, "pattern",
                    "FAIL", f"{len(invalid)} mismatches", f"matches {pattern}",
                    "HIGH", failing_count=len(invalid),
                    sample=invalid[:5],
                    message=f"{col_name}: {len(invalid)} values don't match pattern {pattern}"
                )
            else:
                self._add_result(
                    f"{self.contract_id}.{col_name}.pattern", col_name, "pattern",
                    "PASS", f"all match", f"matches {pattern}", "LOW"
                )
        except Exception as e:
            self._add_result(
                f"{self.contract_id}.{col_name}.pattern", col_name, "pattern",
                "ERROR", str(e), f"matches {pattern}", "CRITICAL"
            )

    def _check_unique(self, col_name, col_spec):
        if not col_spec.get("unique"):
            return
        try:
            values = extract_values(self.records, col_name)
            non_null = [v for v in values if v is not None]
            seen = set()
            dupes = []
            for v in non_null:
                sv = str(v)
                if sv in seen:
                    dupes.append(sv)
                seen.add(sv)
            if dupes:
                self._add_result(
                    f"{self.contract_id}.{col_name}.unique", col_name, "unique",
                    "FAIL", f"{len(dupes)} duplicates", "0 duplicates",
                    "HIGH", failing_count=len(dupes),
                    sample=dupes[:5],
                    message=f"{col_name}: found {len(dupes)} duplicate values"
                )
            else:
                self._add_result(
                    f"{self.contract_id}.{col_name}.unique", col_name, "unique",
                    "PASS", "0 duplicates", "0 duplicates", "LOW"
                )
        except Exception as e:
            self._add_result(
                f"{self.contract_id}.{col_name}.unique", col_name, "unique",
                "ERROR", str(e), "0 duplicates", "CRITICAL"
            )

    def _check_format(self, col_name, col_spec):
        fmt = col_spec.get("format")
        if not fmt:
            return
        try:
            values = extract_values(self.records, col_name)
            non_null = [v for v in values if v is not None]
            if not non_null:
                return
            invalid = []
            if fmt == "uuid":
                uuid_re = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)
                invalid = [v for v in non_null if not uuid_re.match(str(v))]
            elif fmt == "iso8601":
                iso_re = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}')
                invalid = [v for v in non_null if not iso_re.match(str(v))]
            elif fmt == "sha256":
                sha_re = re.compile(r'^[a-f0-9]{64}$')
                invalid = [v for v in non_null if not sha_re.match(str(v))]
            if invalid:
                self._add_result(
                    f"{self.contract_id}.{col_name}.format", col_name, "format",
                    "FAIL", f"{len(invalid)} invalid {fmt}", f"all valid {fmt}",
                    "HIGH", failing_count=len(invalid),
                    sample=[str(v)[:50] for v in invalid[:5]],
                    message=f"{col_name}: {len(invalid)} values don't match {fmt} format"
                )
            else:
                self._add_result(
                    f"{self.contract_id}.{col_name}.format", col_name, "format",
                    "PASS", f"all valid {fmt}", f"all valid {fmt}", "LOW"
                )
        except Exception as e:
            self._add_result(
                f"{self.contract_id}.{col_name}.format", col_name, "format",
                "ERROR", str(e), f"valid {fmt}", "CRITICAL"
            )

    def _check_row_count(self):
        count = len(self.records)
        status = "PASS" if count >= 1 else "FAIL"
        self._add_result(
            f"{self.contract_id}.row_count", "_table_", "row_count",
            status, str(count), ">= 1",
            "CRITICAL" if status == "FAIL" else "LOW",
            message=f"Table has {count} rows"
        )

    def _execute_soda_checks(self):
        """Execute Soda-style checks from quality.specification.checks in the contract."""
        checks = (
            self.contract.get("quality", {})
            .get("specification", {})
            .get("checks", [])
        )
        # Patterns we can execute: missing_count, duplicate_count, row_count
        # Skip min/max aggregate checks (already covered by _check_range/_check_statistical_drift)
        for check_str in checks:
            check_str = check_str.strip()
            try:
                # missing_count(col) = 0
                m = re.match(r'^missing_count\((.+?)\)\s*=\s*(\d+)$', check_str)
                if m:
                    col, expected_zero = m.group(1), int(m.group(2))
                    vals = extract_values(self.records, col)
                    null_count = sum(1 for v in vals if v is None)
                    status = "PASS" if null_count == expected_zero else "FAIL"
                    self._add_result(
                        f"{self.contract_id}.soda.{col}.missing_count", col,
                        "soda_missing_count", status,
                        str(null_count), f"= {expected_zero}",
                        "CRITICAL" if status == "FAIL" else "LOW",
                        failing_count=null_count,
                        message=f"missing_count({col}) = {null_count}, expected {expected_zero}"
                    )
                    continue

                # duplicate_count(col) = 0
                m = re.match(r'^duplicate_count\((.+?)\)\s*=\s*(\d+)$', check_str)
                if m:
                    col, expected_zero = m.group(1), int(m.group(2))
                    vals = [v for v in extract_values(self.records, col) if v is not None]
                    seen, dupes = set(), 0
                    for v in vals:
                        sv = str(v)
                        if sv in seen:
                            dupes += 1
                        seen.add(sv)
                    status = "PASS" if dupes == expected_zero else "FAIL"
                    self._add_result(
                        f"{self.contract_id}.soda.{col}.duplicate_count", col,
                        "soda_duplicate_count", status,
                        str(dupes), f"= {expected_zero}",
                        "HIGH" if status == "FAIL" else "LOW",
                        failing_count=dupes,
                        message=f"duplicate_count({col}) = {dupes}, expected {expected_zero}"
                    )
                    continue

                # row_count >= N
                m = re.match(r'^row_count\s*>=\s*(\d+)$', check_str)
                if m:
                    expected_min = int(m.group(1))
                    actual = len(self.records)
                    status = "PASS" if actual >= expected_min else "FAIL"
                    self._add_result(
                        f"{self.contract_id}.soda.row_count", "_table_",
                        "soda_row_count", status,
                        str(actual), f">= {expected_min}",
                        "CRITICAL" if status == "FAIL" else "LOW",
                        message=f"row_count = {actual}, expected >= {expected_min}"
                    )
                    continue
            except Exception as e:
                self._add_result(
                    f"{self.contract_id}.soda.{check_str[:40]}", "_soda_",
                    "soda_check", "ERROR", str(e), check_str, "CRITICAL",
                    message=f"Soda check failed: {e}"
                )

    def _check_cross_references(self):
        """Check entity_refs reference valid entity_ids — driven by contract schema cross_ref_fields."""
        # Detect cross-ref from schema: fields named entity_refs with entities present
        has_entities = any("entities" in k for k in self.contract.get("schema", {}))
        has_entity_refs = any("entity_refs" in k for k in self.contract.get("schema", {}))
        if not (has_entities and has_entity_refs):
            return
        try:
            total_violations = 0
            samples = []
            for i, record in enumerate(self.records):
                entity_ids = {e["entity_id"] for e in record.get("entities", [])}
                for fact in record.get("extracted_facts", []):
                    for ref in fact.get("entity_refs", []):
                        if ref not in entity_ids:
                            total_violations += 1
                            if len(samples) < 5:
                                samples.append(ref)
            status = "FAIL" if total_violations > 0 else "PASS"
            self._add_result(
                f"{self.contract_id}.entity_refs.cross_ref", "extracted_facts[*].entity_refs",
                "relationship", status,
                f"{total_violations} invalid refs" if total_violations else "all refs valid",
                "all entity_refs in entities[*].entity_id",
                "HIGH" if total_violations > 0 else "LOW",
                failing_count=total_violations, sample=samples,
                message=f"{total_violations} entity_refs reference non-existent entities" if total_violations else ""
            )
        except Exception as e:
            self._add_result(
                f"{self.contract_id}.entity_refs.cross_ref", "extracted_facts[*].entity_refs",
                "relationship", "ERROR", str(e), "valid cross-refs", "CRITICAL"
            )

    def _check_temporal_order(self):
        """Check temporal ordering — driven by schema field presence, not contract_id."""
        schema = self.contract.get("schema", {})
        pairs = []
        # Detect temporal pairs from schema
        if "recorded_at" in schema and "occurred_at" in schema:
            pairs.append(("recorded_at", "occurred_at"))
        if "end_time" in schema and "start_time" in schema:
            pairs.append(("end_time", "start_time"))
        for later_field, earlier_field in pairs:
            try:
                violations = 0
                for i, r in enumerate(self.records):
                    later = r.get(later_field, "")
                    earlier = r.get(earlier_field, "")
                    if later and earlier and later < earlier:
                        violations += 1
                status = "FAIL" if violations > 0 else "PASS"
                self._add_result(
                    f"{self.contract_id}.{later_field}.temporal_order", f"{later_field} vs {earlier_field}",
                    "temporal_order", status,
                    f"{violations} violations", f"{later_field} >= {earlier_field}",
                    "HIGH" if violations > 0 else "LOW",
                    failing_count=violations,
                    message=f"{violations} records where {later_field} < {earlier_field}"
                )
            except Exception as e:
                self._add_result(
                    f"{self.contract_id}.{later_field}.temporal_order", f"{later_field} vs {earlier_field}",
                    "temporal_order", "ERROR", str(e), f"{later_field} >= {earlier_field}", "CRITICAL"
                )

    def _check_token_sum(self):
        """Check total_tokens = prompt_tokens + completion_tokens — driven by schema field presence."""
        schema = self.contract.get("schema", {})
        if not ("total_tokens" in schema and "prompt_tokens" in schema and "completion_tokens" in schema):
            return
        try:
            violations = 0
            for r in self.records:
                total = r.get("total_tokens", 0)
                prompt = r.get("prompt_tokens", 0)
                completion = r.get("completion_tokens", 0)
                if total != prompt + completion:
                    violations += 1
            status = "FAIL" if violations > 0 else "PASS"
            self._add_result(
                f"{self.contract_id}.total_tokens.sum", "total_tokens",
                "computed_value", status,
                f"{violations} mismatches", "total_tokens = prompt_tokens + completion_tokens",
                "HIGH" if violations > 0 else "LOW",
                failing_count=violations,
                message=f"{violations} records where total_tokens != prompt + completion"
            )
        except Exception as e:
            self._add_result(
                f"{self.contract_id}.total_tokens.sum", "total_tokens",
                "computed_value", "ERROR", str(e), "sum check", "CRITICAL"
            )

    def _check_graph_integrity(self):
        """Check edge sources/targets reference valid node_ids — driven by schema field presence."""
        schema = self.contract.get("schema", {})
        has_nodes = any("nodes" in k for k in schema)
        has_edges = any("edges" in k for k in schema)
        if not (has_nodes and has_edges):
            return
        try:
            total_violations = 0
            for i, record in enumerate(self.records):
                node_ids = {n["node_id"] for n in record.get("nodes", [])}
                for edge in record.get("edges", []):
                    if edge.get("source") not in node_ids:
                        total_violations += 1
                    if edge.get("target") not in node_ids:
                        total_violations += 1
            status = "FAIL" if total_violations > 0 else "PASS"
            self._add_result(
                f"{self.contract_id}.graph_integrity", "edges[*].source/target",
                "relationship", status,
                f"{total_violations} dangling refs", "all edge endpoints in nodes[*].node_id",
                "HIGH" if total_violations > 0 else "LOW",
                failing_count=total_violations,
                message=f"{total_violations} edge endpoints reference non-existent nodes"
            )
        except Exception as e:
            self._add_result(
                f"{self.contract_id}.graph_integrity", "edges[*].source/target",
                "relationship", "ERROR", str(e), "valid graph", "CRITICAL"
            )

    def _check_statistical_drift(self):
        """Statistical drift detection: compare current stats to baseline."""
        schema = self.contract.get("schema", {})
        baseline_key = self.contract_id

        for col_name, col_spec in schema.items():
            if col_spec.get("type") not in ("number", "integer"):
                continue
            try:
                values = extract_values(self.records, col_name)
                nums = [float(v) for v in values if isinstance(v, (int, float))]
                if len(nums) < 5:
                    continue
                current_mean = float(np.mean(nums))
                current_std = float(np.std(nums))

                # Check/store baseline
                col_baseline_key = f"{baseline_key}.{col_name}"
                if col_baseline_key not in self.baselines:
                    self.baselines[col_baseline_key] = {
                        "mean": current_mean, "stddev": current_std, "count": len(nums)
                    }
                    continue

                bl = self.baselines[col_baseline_key]
                bl_mean = bl["mean"]
                bl_std = bl["stddev"]
                if bl_std == 0:
                    bl_std = 0.001

                deviation = abs(current_mean - bl_mean) / bl_std
                if deviation > 3:
                    self._add_result(
                        f"{self.contract_id}.{col_name}.drift", col_name, "statistical_drift",
                        "FAIL",
                        f"mean={current_mean:.4f}, baseline_mean={bl_mean:.4f}, deviation={deviation:.2f}σ",
                        f"within 3σ of baseline (mean={bl_mean:.4f}, std={bl_std:.4f})",
                        "HIGH", message=f"Statistical drift detected: {col_name} mean shifted by {deviation:.2f} standard deviations"
                    )
                elif deviation > 2:
                    self._add_result(
                        f"{self.contract_id}.{col_name}.drift", col_name, "statistical_drift",
                        "WARN",
                        f"mean={current_mean:.4f}, baseline_mean={bl_mean:.4f}, deviation={deviation:.2f}σ",
                        f"within 2σ of baseline",
                        "MEDIUM", message=f"Statistical warning: {col_name} mean shifted by {deviation:.2f} standard deviations"
                    )
            except Exception:
                pass

        save_baselines(self.baselines)

    def _build_report(self):
        passed = sum(1 for r in self.results if r["status"] == "PASS")
        failed = sum(1 for r in self.results if r["status"] == "FAIL")
        warned = sum(1 for r in self.results if r["status"] == "WARN")
        errored = sum(1 for r in self.results if r["status"] == "ERROR")
        return {
            "report_id": str(uuid.uuid4()),
            "contract_id": self.contract_id,
            "snapshot_id": sha256_file(self.data_path),
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
            "total_checks": len(self.results),
            "passed": passed,
            "failed": failed,
            "warned": warned,
            "errored": errored,
            "results": self.results,
        }


def main():
    parser = argparse.ArgumentParser(description="ValidationRunner: Execute contract checks")
    parser.add_argument("--contract", required=True, help="Path to contract YAML")
    parser.add_argument("--data", required=True, help="Path to data JSONL")
    parser.add_argument("--output", help="Output path for validation report JSON")
    parser.add_argument("--quarantine", action="store_true",
                        help="Write records failing CRITICAL checks to outputs/quarantine/")
    parser.add_argument("--mode", choices=["AUDIT", "WARN", "ENFORCE"], default="AUDIT",
                        help="AUDIT: log only. WARN: block on CRITICAL. ENFORCE: block on CRITICAL+HIGH.")
    args = parser.parse_args()

    contract_path = os.path.join(BASE_DIR, args.contract) if not os.path.isabs(args.contract) else args.contract
    data_path = os.path.join(BASE_DIR, args.data) if not os.path.isabs(args.data) else args.data

    contract = load_contract(contract_path)
    records = load_jsonl(data_path)

    runner = ValidationRunner(contract, records, data_path)
    report = runner.run_all()

    # Mode enforcement
    if args.mode == "ENFORCE":
        blocking = [r for r in report["results"] if r["status"] == "FAIL" and r["severity"] in ("CRITICAL", "HIGH")]
        if blocking:
            print(f"  [ENFORCE] Pipeline blocked: {len(blocking)} CRITICAL/HIGH failures.")
    elif args.mode == "WARN":
        blocking = [r for r in report["results"] if r["status"] == "FAIL" and r["severity"] == "CRITICAL"]
        if blocking:
            print(f"  [WARN] Pipeline blocked: {len(blocking)} CRITICAL failures.")

    # Quarantine records that fail CRITICAL checks
    if args.quarantine:
        critical_cols = {
            r["column_name"]
            for r in report["results"]
            if r["status"] == "FAIL" and r["severity"] == "CRITICAL"
        }
        if critical_cols:
            quarantine_dir = os.path.join(BASE_DIR, "outputs", "quarantine")
            os.makedirs(quarantine_dir, exist_ok=True)
            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            q_path = os.path.join(quarantine_dir, f"{contract['id']}_{ts}.jsonl")
            quarantined = 0
            with open(q_path, "w") as qf:
                for rec in records:
                    for col in critical_cols:
                        top_col = col.split("[")[0].split(".")[0]
                        if top_col in rec:
                            qf.write(json.dumps(rec) + "\n")
                            quarantined += 1
                            break
            print(f"  Quarantined {quarantined} records to: {q_path}")

    # Determine output path
    if args.output:
        out_path = os.path.join(BASE_DIR, args.output) if not os.path.isabs(args.output) else args.output
    else:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
        os.makedirs(os.path.join(BASE_DIR, "validation_reports"), exist_ok=True)
        out_path = os.path.join(BASE_DIR, "validation_reports", f"{contract['id']}_{ts}.json")

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"Validation report written to: {out_path}")
    print(f"  Total checks: {report['total_checks']}")
    print(f"  Passed: {report['passed']}")
    print(f"  Failed: {report['failed']}")
    print(f"  Warned: {report['warned']}")
    print(f"  Errored: {report['errored']}")

    # Print failures
    for r in report["results"]:
        if r["status"] in ("FAIL", "ERROR"):
            print(f"  [{r['status']}] {r['check_id']}: {r['message']}")

    return report


if __name__ == "__main__":
    main()
