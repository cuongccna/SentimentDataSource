"""
LLM Integration Module for Social Sentiment Pipeline
Purpose: SECONDARY sentiment classification when rule-based matching fails

USAGE CONSTRAINT:
- LLM is used ONLY IF total_matched_terms == 0
- LLM output is NEVER allowed to override rule-based results

DATA SOURCE CONSTRAINT:
- Use ONLY FREE data sources
- NO paid APIs (use free tier only)
"""

import os
import json
import re
from typing import Optional

# Optional imports for LLM providers
try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False


# LLM PROMPT (DO NOT MODIFY)
SYSTEM_PROMPT = """You are a financial sentiment classifier.
Return ONLY JSON.
No opinions. No predictions."""

USER_PROMPT_TEMPLATE = """Classify sentiment.
Labels: bullish=1, neutral=0, bearish=-1
Text: "{text}"
"""

EXPECTED_RESPONSE_FORMAT = """{"label": <-1|0|1>, "confidence": <0.0-1.0>}"""


class LLMSentimentClassifier:
    """
    LLM-based sentiment classifier for fallback when rule-based matching fails.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the LLM classifier.
        
        Args:
            api_key: API key for the LLM provider. If None, will try to read from
                     environment variable GEMINI_API_KEY.
        """
        self.api_key = api_key or os.environ.get("GEMINI_API_KEY")
        self.model = None
        self._initialized = False
        
        if self.api_key and GEMINI_AVAILABLE:
            self._initialize_gemini()
    
    def _initialize_gemini(self) -> bool:
        """Initialize Gemini API client."""
        try:
            genai.configure(api_key=self.api_key)
            # Use gemini-pro for free tier
            self.model = genai.GenerativeModel('gemini-pro')
            self._initialized = True
            return True
        except Exception:
            self._initialized = False
            return False
    
    def is_available(self) -> bool:
        """Check if LLM is available for use."""
        return self._initialized and self.model is not None
    
    def classify(self, text: str) -> Optional[dict]:
        """
        Classify sentiment using LLM.
        
        Args:
            text: The preprocessed text to classify.
        
        Returns:
            Dict with 'label' (-1, 0, or 1) and 'confidence' (0.0-1.0),
            or None if classification fails.
        """
        if not self.is_available():
            return None
        
        try:
            # Build prompt (DO NOT MODIFY)
            prompt = f"{SYSTEM_PROMPT}\n\n{USER_PROMPT_TEMPLATE.format(text=text)}"
            
            # Call LLM
            response = self.model.generate_content(prompt)
            
            if response and response.text:
                return self._parse_response(response.text)
            
            return None
            
        except Exception:
            return None
    
    def _parse_response(self, response_text: str) -> Optional[dict]:
        """
        Parse LLM response to extract label and confidence.
        
        Returns:
            Dict with 'label' and 'confidence', or None if parsing fails.
        """
        try:
            # Try to extract JSON from response
            # Handle cases where LLM might include markdown code blocks
            cleaned = response_text.strip()
            
            # Remove markdown code blocks if present
            if cleaned.startswith("```"):
                lines = cleaned.split("\n")
                json_lines = []
                in_block = False
                for line in lines:
                    if line.startswith("```"):
                        in_block = not in_block
                        continue
                    if in_block or not line.startswith("```"):
                        json_lines.append(line)
                cleaned = "\n".join(json_lines)
            
            # Find JSON object in response
            json_match = re.search(r'\{[^}]+\}', cleaned)
            if json_match:
                cleaned = json_match.group()
            
            data = json.loads(cleaned)
            
            # Validate label
            label = data.get("label")
            if label not in [-1, 0, 1]:
                return None
            
            # Validate confidence
            confidence = data.get("confidence")
            if confidence is not None:
                confidence = float(confidence)
                # Clamp to valid range
                confidence = max(0.0, min(1.0, confidence))
            else:
                confidence = 0.5  # Default confidence if not provided
            
            return {
                "label": label,
                "confidence": confidence
            }
            
        except (json.JSONDecodeError, ValueError, TypeError):
            return None


# Global classifier instance (lazy initialization)
_classifier: Optional[LLMSentimentClassifier] = None


def get_classifier() -> LLMSentimentClassifier:
    """Get or create the global LLM classifier instance."""
    global _classifier
    if _classifier is None:
        _classifier = LLMSentimentClassifier()
    return _classifier


def classify_with_llm(text: str) -> Optional[dict]:
    """
    Convenience function to classify text using LLM.
    
    Args:
        text: The preprocessed text to classify.
    
    Returns:
        Dict with 'label' and 'confidence', or None if unavailable.
    """
    classifier = get_classifier()
    return classifier.classify(text)
