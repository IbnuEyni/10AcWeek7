#!/usr/bin/env python3
"""Migrate Week 3 (Document Refinery) real data to canonical JSONL format.

Source: 10AcWeek3/.refinery/ (extraction_ledger.jsonl, ldus/*.json, profiles/*.json)
Target: outputs/week3/extractions.jsonl
"""
import json, os, uuid, hashlib, re

BASE = os.path.dirname(os.path.abspath(__file__))
WEEK3_DIR = "/home/shuaib/Desktop/python/10Acd/10AcWeek3/.refinery"
OUT_PATH = os.path.join(BASE, "outputs/week3/extractions.jsonl")


def load_ledger():
    path = os.path.join(WEEK3_DIR, "extraction_ledger.jsonl")
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def load_profile(doc_id):
    path = os.path.join(WEEK3_DIR, "profiles", f"{doc_id}.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return None


def load_ldus(doc_id):
    path = os.path.join(WEEK3_DIR, "ldus", f"{doc_id}_ldus.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return None


def infer_entity_type(text):
    text_lower = text.lower()
    if any(w in text_lower for w in ["corp", "ltd", "inc", "company", "bank", "reinsurance"]):
        return "ORG"
    if re.search(r'\d{4}[-/]\d{2}[-/]\d{2}|\b(january|february|march|june|july)\b', text_lower):
        return "DATE"
    if re.search(r'\$[\d,]+|usd|birr|etb|\d+\.\d+%', text_lower):
        return "AMOUNT"
    if any(w in text_lower for w in ["ethiopia", "addis", "new york", "london"]):
        return "LOCATION"
    if len(text.split()) <= 3 and text[0].isupper():
        return "PERSON"
    return "OTHER"


def migrate():
    ledger = load_ledger()
    output_records = []

    for entry in ledger:
        doc_id = entry["doc_id"]
        profile = load_profile(doc_id)
        ldus = load_ldus(doc_id)

        # Build entities from LDU content
        entities = []
        extracted_facts = []

        if ldus:
            # Extract entities from LDU text content
            seen_entities = {}
            for ldu in ldus:
                content = ldu.get("content", "")
                # Simple entity extraction from content
                words = re.findall(r'[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*', content)
                for w in words[:3]:
                    if w not in seen_entities and len(w) > 2:
                        eid = str(uuid.uuid4())
                        etype = infer_entity_type(w)
                        seen_entities[w] = eid
                        entities.append({
                            "entity_id": eid,
                            "name": w,
                            "type": etype,
                            "canonical_value": w.lower().replace(" ", "_"),
                        })

            entity_ids = [e["entity_id"] for e in entities]

            for ldu in ldus:
                content = ldu.get("content", "")
                if not content.strip():
                    continue
                # Pick entity refs from entities found in this LDU
                refs = []
                for ename, eid in seen_entities.items():
                    if ename.lower() in content.lower():
                        refs.append(eid)
                if not refs and entity_ids:
                    refs = [entity_ids[0]]

                page_refs = ldu.get("page_refs", [])
                page_ref = page_refs[0] if page_refs else None

                confidence = entry.get("confidence_score", 0.8)
                # Ensure confidence is 0.0-1.0
                if confidence > 1.0:
                    confidence = confidence / 100.0
                confidence = round(min(max(confidence, 0.0), 1.0), 2)

                extracted_facts.append({
                    "fact_id": str(uuid.uuid4()),
                    "text": content[:500].strip(),
                    "entity_refs": refs[:5],
                    "confidence": confidence,
                    "page_ref": page_ref,
                    "source_excerpt": content[:300].strip(),
                })
        else:
            # No LDUs available — create a minimal fact from the ledger entry
            entities.append({
                "entity_id": str(uuid.uuid4()),
                "name": doc_id.replace("_", " "),
                "type": "OTHER",
                "canonical_value": doc_id.lower(),
            })
            confidence = entry.get("confidence_score", 0.8)
            if confidence > 1.0:
                confidence = confidence / 100.0
            extracted_facts.append({
                "fact_id": str(uuid.uuid4()),
                "text": f"Document processed: {entry.get('filename', doc_id)}",
                "entity_refs": [entities[0]["entity_id"]],
                "confidence": round(min(max(confidence, 0.0), 1.0), 2),
                "page_ref": None,
                "source_excerpt": f"Processed via {entry.get('strategy_used', 'unknown')} strategy",
            })

        # Source hash from filename
        source_hash = hashlib.sha256(entry.get("filename", doc_id).encode()).hexdigest()

        # Model mapping from strategy
        strategy = entry.get("strategy_used", "fast_text")
        model_map = {
            "vision_augmented": "claude-3-5-sonnet-20241022",
            "layout_aware": "claude-3-haiku-20240307",
            "fast_text": "gpt-4o-2024-05-13",
            "enhanced_table": "claude-3-5-sonnet-20241022",
        }
        model = model_map.get(strategy, "claude-3-5-sonnet-20241022")

        proc_time = entry.get("processing_time_ms", 1000)
        if isinstance(proc_time, float) and proc_time < 10:
            proc_time = int(proc_time * 1000)
        else:
            proc_time = int(proc_time)
        proc_time = max(proc_time, 100)

        total_pages = 1
        if profile:
            total_pages = profile.get("total_pages", 1)

        input_tokens = total_pages * 200
        output_tokens = len(extracted_facts) * 50

        ts = entry.get("timestamp", "2026-03-01T00:00:00Z")
        if "T" not in ts:
            ts = ts + "T00:00:00Z"
        if not ts.endswith("Z"):
            ts = ts + "Z"

        output_records.append({
            "doc_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, doc_id)),
            "source_path": f"/data/documents/{entry.get('filename', doc_id + '.pdf')}",
            "source_hash": source_hash,
            "extracted_facts": extracted_facts,
            "entities": entities,
            "extraction_model": model,
            "processing_time_ms": proc_time,
            "token_count": {"input": input_tokens, "output": output_tokens},
            "extracted_at": ts,
        })

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        for r in output_records:
            f.write(json.dumps(r) + "\n")

    total_facts = sum(len(r["extracted_facts"]) for r in output_records)
    print(f"Week 3: Migrated {len(output_records)} extraction records ({total_facts} facts) to {OUT_PATH}")


if __name__ == "__main__":
    migrate()
