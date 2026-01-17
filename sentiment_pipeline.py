"""
Social Sentiment Pipeline for Crypto Bot Trading
Purpose: RISK CONTROL ONLY - Not for generating trading signals
"""

import re
import json
from typing import Optional
from dataclasses import dataclass


# LEXICON (LOAD ONLY – DO NOT MODIFY)
LEXICON = {
    "bullish": ["moon", "breakout", "accumulation", "buy wall", "whale buying"],
    "bearish": ["dump", "rug", "hack", "exploit", "sell wall"],
    "fear": ["panic", "exit", "collapse", "bankruptcy"],
    "greed": ["100x", "lambo", "all in", "yolo"]
}

# REGEX RULES (MATCH ONLY)
REGEX_PATTERNS = [
    re.compile(r"\b(dump|dumping|dumped)\b", re.IGNORECASE),
    re.compile(r"\b(rug|rugpull)\b", re.IGNORECASE),
    re.compile(r"\b\d{2,4}x\b", re.IGNORECASE),
    re.compile(r"[!]{2,}")
]

# Mapping regex patterns to categories
REGEX_CATEGORY_MAP = {
    0: "bearish",  # dump|dumping|dumped
    1: "bearish",  # rug|rugpull
    2: "greed",    # \d{2,4}x
    3: None        # exclamation marks (counted separately, no category)
}

# SCORING COEFFICIENTS (FIXED - DO NOT MODIFY)
SCORE_WEIGHTS = {
    "bullish": 1.0,
    "greed": 0.5,
    "bearish": -1.2,
    "fear": -1.5
}

# LABEL THRESHOLDS (FIXED - DO NOT MODIFY)
LABEL_THRESHOLD_POSITIVE = 0.2
LABEL_THRESHOLD_NEGATIVE = -0.2


@dataclass
class EngagementData:
    like: int
    reply: int
    share: int


@dataclass
class AuthorData:
    followers: int
    reputation_score: float


@dataclass
class SentimentRecord:
    source: str
    asset: str
    text: str
    timestamp: str
    engagement: EngagementData
    author: AuthorData


def validate_record(data: dict) -> Optional[SentimentRecord]:
    """
    Validate input record. Returns None if ANY field is missing or null.
    """
    try:
        # Check top-level required fields
        required_fields = ["source", "asset", "text", "timestamp", "engagement", "author"]
        for field in required_fields:
            if field not in data or data[field] is None:
                return None
        
        # Validate source value
        if data["source"] not in ["twitter", "reddit", "telegram"]:
            return None
        
        # Validate engagement fields
        engagement = data["engagement"]
        if not isinstance(engagement, dict):
            return None
        for field in ["like", "reply", "share"]:
            if field not in engagement or engagement[field] is None:
                return None
            if not isinstance(engagement[field], (int, float)):
                return None
        
        # Validate author fields
        author = data["author"]
        if not isinstance(author, dict):
            return None
        for field in ["followers", "reputation_score"]:
            if field not in author or author[field] is None:
                return None
        
        # Validate text is not empty
        if not isinstance(data["text"], str) or len(data["text"].strip()) == 0:
            return None
        
        # Validate timestamp is not empty
        if not isinstance(data["timestamp"], str) or len(data["timestamp"].strip()) == 0:
            return None
        
        # Validate asset is not empty
        if not isinstance(data["asset"], str) or len(data["asset"].strip()) == 0:
            return None
        
        return SentimentRecord(
            source=data["source"],
            asset=data["asset"],
            text=data["text"],
            timestamp=data["timestamp"],
            engagement=EngagementData(
                like=int(engagement["like"]),
                reply=int(engagement["reply"]),
                share=int(engagement["share"])
            ),
            author=AuthorData(
                followers=int(author["followers"]),
                reputation_score=float(author["reputation_score"])
            )
        )
    except (KeyError, TypeError, ValueError):
        return None


def preprocess_text(text: str) -> tuple[str, str]:
    """
    Pre-processing:
    - Convert text to lowercase
    - Remove URLs
    - Remove emojis
    - Preserve original raw text for audit
    
    Returns: (processed_text, original_text)
    """
    original_text = text
    
    # Convert to lowercase
    processed = text.lower()
    
    # Remove URLs
    url_pattern = re.compile(r'https?://\S+|www\.\S+')
    processed = url_pattern.sub('', processed)
    
    # Remove emojis (Unicode emoji ranges)
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags
        "\U00002702-\U000027B0"  # dingbats
        "\U000024C2-\U0001F251"  # enclosed characters
        "\U0001F900-\U0001F9FF"  # supplemental symbols
        "\U0001FA00-\U0001FA6F"  # chess symbols
        "\U0001FA70-\U0001FAFF"  # symbols and pictographs extended-A
        "\U00002600-\U000026FF"  # misc symbols
        "]+",
        flags=re.UNICODE
    )
    processed = emoji_pattern.sub('', processed)
    
    return processed, original_text


def count_lexicon_matches(text: str) -> dict[str, int]:
    """
    Count exact lexicon matches.
    No NLP. No embeddings. No semantic inference.
    """
    counts = {
        "bullish": 0,
        "bearish": 0,
        "fear": 0,
        "greed": 0
    }
    
    for category, terms in LEXICON.items():
        for term in terms:
            # Count occurrences of each term
            pattern = re.compile(r'\b' + re.escape(term) + r'\b', re.IGNORECASE)
            matches = pattern.findall(text)
            counts[category] += len(matches)
    
    return counts


def count_regex_matches(text: str) -> dict[str, int]:
    """
    Count regex matches and add to appropriate categories.
    """
    counts = {
        "bullish": 0,
        "bearish": 0,
        "fear": 0,
        "greed": 0
    }
    
    for idx, pattern in enumerate(REGEX_PATTERNS):
        matches = pattern.findall(text)
        category = REGEX_CATEGORY_MAP.get(idx)
        if category and category in counts:
            counts[category] += len(matches)
    
    return counts


def calculate_raw_score(counts: dict[str, int]) -> float:
    """
    Scoring Formula (FIXED):
    raw_score = (bullish_count * +1.0) + (greed_count * +0.5) 
                - (bearish_count * 1.2) - (fear_count * 1.5)
    """
    raw_score = (
        (counts["bullish"] * 1.0) +
        (counts["greed"] * 0.5) -
        (counts["bearish"] * 1.2) -
        (counts["fear"] * 1.5)
    )
    return raw_score


def normalize_score(raw_score: float, total_matched_terms: int) -> float:
    """
    Normalization:
    normalized_score = raw_score / total_matched_terms
    Clamp result to range [-1.0, +1.0]
    """
    if total_matched_terms == 0:
        return 0.0
    
    normalized = raw_score / total_matched_terms
    
    # Clamp to [-1.0, +1.0]
    return max(-1.0, min(1.0, normalized))


def assign_label(score: float) -> int:
    """
    Label Assignment (FIXED THRESHOLDS):
    IF score >= 0.2       → label = +1
    IF -0.2 < score < 0.2 → label = 0
    IF score <= -0.2      → label = -1
    
    NO fuzzy logic. NO overrides.
    """
    if score >= LABEL_THRESHOLD_POSITIVE:
        return 1
    elif score <= LABEL_THRESHOLD_NEGATIVE:
        return -1
    else:
        return 0


def get_llm_sentiment(text: str) -> Optional[dict]:
    """
    OPTIONAL LLM ASSIST (SECONDARY ONLY)
    Used ONLY IF:
    - total_matched_terms == 0
    - text is ambiguous
    
    Returns dict with 'label' and 'confidence' or None if unavailable.
    """
    # LLM PROMPT (DO NOT MODIFY):
    # SYSTEM:
    # You are a financial sentiment classifier.
    # Return ONLY JSON.
    # No opinions. No predictions.
    #
    # USER:
    # Classify sentiment.
    # Labels: bullish=1, neutral=0, bearish=-1
    # Text: "{TEXT}"
    
    try:
        from llm_classifier import classify_with_llm
        return classify_with_llm(text)
    except ImportError:
        return None


def process_record(data: dict) -> Optional[dict]:
    """
    Main processing function for a single record.
    Returns final output format or None if record should be dropped.
    """
    # Validate input
    record = validate_record(data)
    if record is None:
        return None
    
    # Pre-processing
    processed_text, original_text = preprocess_text(record.text)
    
    # Term Counting
    lexicon_counts = count_lexicon_matches(processed_text)
    regex_counts = count_regex_matches(processed_text)
    
    # Combine counts
    total_counts = {
        "bullish": lexicon_counts["bullish"] + regex_counts["bullish"],
        "bearish": lexicon_counts["bearish"] + regex_counts["bearish"],
        "fear": lexicon_counts["fear"] + regex_counts["fear"],
        "greed": lexicon_counts["greed"] + regex_counts["greed"]
    }
    
    total_matched_terms = sum(total_counts.values())
    
    # Calculate score
    raw_score = calculate_raw_score(total_counts)
    normalized_score = normalize_score(raw_score, total_matched_terms)
    
    # Assign label
    rule_based_label = assign_label(normalized_score) if total_matched_terms > 0 else None
    
    # LLM processing (only if rule-based fails)
    llm_used = False
    llm_label = None
    llm_confidence = None
    
    if total_matched_terms == 0:
        llm_result = get_llm_sentiment(processed_text)
        if llm_result is not None:
            llm_used = True
            llm_label = llm_result.get("label")
            llm_confidence = llm_result.get("confidence")
    
    # PRIORITY RULE:
    # IF rule-based label exists → IGNORE LLM output.
    # LLM output is NEVER allowed to override rule-based results.
    if rule_based_label is not None:
        final_label = rule_based_label
        final_confidence = abs(normalized_score)
    elif llm_label is not None:
        final_label = llm_label
        final_confidence = llm_confidence if llm_confidence is not None else 0.0
    else:
        # No matches and no LLM result - default to neutral
        final_label = 0
        final_confidence = 0.0
    
    # Build output (STRICT FORMAT - DO NOT CHANGE)
    output = {
        "asset": record.asset,
        "timestamp": record.timestamp,
        "source": record.source,
        "sentiment": {
            "rule_based": {
                "bullish": total_counts["bullish"],
                "bearish": total_counts["bearish"],
                "fear": total_counts["fear"],
                "greed": total_counts["greed"],
                "score": round(normalized_score, 6),
                "label": rule_based_label if rule_based_label is not None else 0
            },
            "llm": {
                "used": llm_used,
                "label": llm_label,
                "confidence": llm_confidence
            },
            "final": {
                "label": final_label,
                "confidence": round(final_confidence, 6)
            }
        }
    }
    
    return output


def process_batch(records: list[dict]) -> list[dict]:
    """
    Process a batch of records.
    Invalid records are dropped (not included in output).
    """
    results = []
    for record in records:
        result = process_record(record)
        if result is not None:
            results.append(result)
    return results


def process_json_file(input_path: str, output_path: str) -> int:
    """
    Process records from a JSON file and write results to output file.
    Returns count of successfully processed records.
    """
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if isinstance(data, list):
        results = process_batch(data)
    else:
        result = process_record(data)
        results = [result] if result is not None else []
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)
    
    return len(results)


if __name__ == "__main__":
    # Example usage
    sample_record = {
        "source": "twitter",
        "asset": "BTC",
        "text": "BTC is going to the moon!! 🚀 Whale buying detected, this is a breakout!",
        "timestamp": "2026-01-17T10:30:00Z",
        "engagement": {
            "like": 150,
            "reply": 25,
            "share": 40
        },
        "author": {
            "followers": 5000,
            "reputation_score": 0.85
        }
    }
    
    result = process_record(sample_record)
    if result:
        print(json.dumps(result, indent=2))
    else:
        print("Record dropped due to validation failure")
