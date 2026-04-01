#!/usr/bin/env python3
"""Migrate Week 5 (Event Sourcing Platform) to canonical JSONL format.

The Week 5 event store is PostgreSQL-backed. Since we can't connect to the DB,
we generate realistic events using the actual event model definitions from
10Acweek5/ledger/src/models/events.py and the demo.py workflow.

This produces events that match the real domain: LoanApplication lifecycle.
Target: outputs/week5/events.jsonl
"""
import json, os, uuid, random
from datetime import datetime, timezone, timedelta

random.seed(42)
BASE = os.path.dirname(os.path.abspath(__file__))
OUT_PATH = os.path.join(BASE, "outputs/week5/events.jsonl")

# Real event types from 10Acweek5/ledger/src/models/events.py EVENT_CATALOGUE
EVENT_CATALOGUE = {
    "ApplicationSubmitted": {
        "aggregate_type": "LoanApplication",
        "payload_template": lambda app_id: {
            "application_id": app_id,
            "applicant_id": f"applicant-{random.randint(1,50)}",
            "requested_amount_usd": round(random.uniform(5000, 100000), 2),
            "loan_purpose": random.choice(["home-improvement", "business-expansion", "debt-consolidation", "education"]),
            "submission_channel": random.choice(["api", "web", "mobile"]),
        },
    },
    "CreditAnalysisRequested": {
        "aggregate_type": "LoanApplication",
        "payload_template": lambda app_id: {
            "application_id": app_id,
            "assigned_agent_id": f"agent-apex-{random.randint(1,5)}",
            "priority": random.choice(["normal", "high", "urgent"]),
        },
    },
    "AgentContextLoaded": {
        "aggregate_type": "AgentSession",
        "payload_template": lambda app_id: {
            "agent_id": f"agent-apex-{random.randint(1,5)}",
            "session_id": uuid.uuid4().hex[:8],
            "context_source": random.choice(["event_replay", "snapshot", "cold_start"]),
            "event_replay_from_position": 0,
            "context_token_count": random.randint(500, 5000),
            "model_version": "apex-v2.1",
        },
    },
    "CreditAnalysisCompleted": {
        "aggregate_type": "AgentSession",
        "payload_template": lambda app_id: {
            "application_id": app_id,
            "agent_id": f"agent-apex-{random.randint(1,5)}",
            "session_id": uuid.uuid4().hex[:8],
            "model_version": "apex-v2.1",
            "confidence_score": round(random.uniform(0.5, 0.99), 2),
            "risk_tier": random.choice(["LOW", "MEDIUM", "HIGH"]),
            "recommended_limit_usd": round(random.uniform(5000, 80000), 2),
            "analysis_duration_ms": random.randint(200, 5000),
            "input_data_hash": uuid.uuid4().hex,
        },
    },
    "FraudScreeningCompleted": {
        "aggregate_type": "AgentSession",
        "payload_template": lambda app_id: {
            "application_id": app_id,
            "agent_id": f"agent-fraud-{random.randint(1,3)}",
            "fraud_score": round(random.uniform(0.0, 0.3), 2),
            "anomaly_flags": random.sample(["velocity_check", "geo_mismatch", "identity_gap", "amount_outlier"], random.randint(0, 2)),
            "screening_model_version": "fraud-v1.3",
            "input_data_hash": uuid.uuid4().hex,
        },
    },
    "ComplianceCheckRequested": {
        "aggregate_type": "ComplianceRecord",
        "payload_template": lambda app_id: {
            "application_id": app_id,
            "regulation_set_version": "2026-Q1",
            "checks_required": ["KYC", "AML", "SANCTIONS"],
        },
    },
    "ComplianceRulePassed": {
        "aggregate_type": "ComplianceRecord",
        "payload_template": lambda app_id: {
            "application_id": app_id,
            "rule_id": random.choice(["KYC", "AML", "SANCTIONS"]),
            "rule_version": "2026-Q1",
            "evidence_hash": uuid.uuid4().hex,
        },
    },
    "DecisionGenerated": {
        "aggregate_type": "LoanApplication",
        "payload_template": lambda app_id: {
            "application_id": app_id,
            "orchestrator_agent_id": "orchestrator-1",
            "recommendation": random.choice(["APPROVE", "DECLINE", "REFER"]),
            "confidence_score": round(random.uniform(0.6, 0.99), 2),
            "contributing_agent_sessions": [uuid.uuid4().hex[:8]],
            "decision_basis_summary": "Based on credit analysis and fraud screening results",
            "forced_refer": random.choice([True, False]),
        },
    },
    "HumanReviewCompleted": {
        "aggregate_type": "LoanApplication",
        "payload_template": lambda app_id: {
            "application_id": app_id,
            "reviewer_id": f"reviewer-{random.randint(1,10)}",
            "final_decision": random.choice(["APPROVE", "DECLINE"]),
            "override": random.choice([True, False]),
            "override_reason": "",
        },
    },
    "ApplicationApproved": {
        "aggregate_type": "LoanApplication",
        "payload_template": lambda app_id: {
            "application_id": app_id,
            "approved_amount_usd": round(random.uniform(5000, 80000), 2),
            "interest_rate": round(random.uniform(3.5, 12.0), 2),
            "conditions": ["income_verification", "collateral_assessment"],
            "approved_by": f"reviewer-{random.randint(1,10)}",
            "effective_date": "2026-03-01",
        },
    },
    "AuditEntryRecorded": {
        "aggregate_type": "AuditLedger",
        "payload_template": lambda app_id: {
            "entity_type": "LoanApplication",
            "entity_id": app_id,
            "source_stream_id": f"loan-{app_id}",
            "event_type": "ApplicationSubmitted",
            "summary": f"Audit entry for application {app_id}",
        },
    },
}

# Realistic lifecycle: each application goes through these steps
LIFECYCLE = [
    "ApplicationSubmitted",
    "CreditAnalysisRequested",
    "AgentContextLoaded",
    "CreditAnalysisCompleted",
    "FraudScreeningCompleted",
    "ComplianceCheckRequested",
    "ComplianceRulePassed",
    "ComplianceRulePassed",
    "ComplianceRulePassed",
    "DecisionGenerated",
    "HumanReviewCompleted",
    "ApplicationApproved",
    "AuditEntryRecorded",
]


def migrate():
    records = []
    aggregates = {}  # aggregate_id -> sequence_number

    # Generate 5 full application lifecycles
    for app_num in range(5):
        app_id = f"demo-{uuid.uuid4().hex[:8]}"
        corr_id = str(uuid.uuid4())
        base_time = datetime(2026, 3, 1, 10, 0, 0, tzinfo=timezone.utc) + timedelta(hours=app_num * 2)

        for step_idx, event_type in enumerate(LIFECYCLE):
            event_def = EVENT_CATALOGUE[event_type]
            agg_type = event_def["aggregate_type"]

            # Determine aggregate_id based on type
            if agg_type == "LoanApplication":
                agg_id = f"loan-{app_id}"
            elif agg_type == "AgentSession":
                agg_id = f"agent-session-{app_id}"
            elif agg_type == "ComplianceRecord":
                agg_id = f"compliance-{app_id}"
            else:
                agg_id = f"audit-{app_id}"

            if agg_id not in aggregates:
                aggregates[agg_id] = 0
            aggregates[agg_id] += 1

            occurred = base_time + timedelta(seconds=step_idx * 30)
            recorded = occurred + timedelta(seconds=random.randint(0, 2))

            payload = event_def["payload_template"](app_id)

            records.append({
                "event_id": str(uuid.uuid4()),
                "event_type": event_type,
                "aggregate_id": agg_id,
                "aggregate_type": agg_type,
                "sequence_number": aggregates[agg_id],
                "payload": payload,
                "metadata": {
                    "causation_id": str(uuid.uuid4()) if step_idx > 0 else None,
                    "correlation_id": corr_id,
                    "user_id": f"user-{random.randint(1,10)}",
                    "source_service": "week5-ledger",
                },
                "schema_version": "1.0",
                "occurred_at": occurred.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "recorded_at": recorded.strftime("%Y-%m-%dT%H:%M:%SZ"),
            })

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")

    print(f"Week 5: Generated {len(records)} event records (from real event model definitions) to {OUT_PATH}")


if __name__ == "__main__":
    migrate()
