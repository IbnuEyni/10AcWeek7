"""Week 3 Document Refinery - Extractor module."""

def extract_facts(document_content, model="claude-3-5-sonnet-20241022"):
    """Extract facts from document content using LLM."""
    # confidence is float 0.0-1.0
    confidence = calculate_confidence(document_content)
    return {"confidence": confidence, "facts": []}

def calculate_confidence(content):
    """Calculate extraction confidence score. Returns float 0.0-1.0."""
    base_score = len(content) / 10000
    return min(max(base_score, 0.0), 1.0)
