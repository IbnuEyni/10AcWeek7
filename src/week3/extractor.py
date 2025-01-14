"""Week 3 Document Refinery - Extractor module."""

def extract_facts(document_content, model="claude-3-5-sonnet-20241022"):
    """Extract facts from document content using LLM."""
    # confidence changed to percentage scale 0-100
    confidence = calculate_confidence(document_content)
    return {"confidence": confidence, "facts": []}

def calculate_confidence(content):
    """Calculate extraction confidence score. Returns int 0-100."""
    base_score = len(content) / 100
    return min(max(int(base_score), 0), 100)
