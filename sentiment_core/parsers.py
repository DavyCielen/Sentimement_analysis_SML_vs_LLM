"""
Parsing utilities for sentiment_core.
"""
import re

def parse_sentiment(response_content: str) -> str:
    """
    Parse the sentiment from the response content.
    Returns 'positive', 'negative', 'neutral', or 'unknown'.
    """
    content = response_content.lower()
    if re.search(r'\bpositive\b', content):
        return 'positive'
    if re.search(r'\bnegative\b', content):
        return 'negative'
    if re.search(r'\bneutral\b', content):
        return 'neutral'
    return 'unknown'