import pytest

from sentiment_core.parsers import parse_sentiment


@pytest.mark.parametrize(
    "text,expected",
    [
        ("This is very positive!", "positive"),
        ("I feel a bit negative about this.", "negative"),
        ("The result is neutral.", "neutral"),
        ("Completely unclear.", "unknown"),
        ("Mixed positive and negative signals.", "positive"),
        ("Absolutely NEUTRAL response.", "neutral"),
    ],
)
def test_parse_sentiment(text, expected):
    result = parse_sentiment(text)
    assert result == expected