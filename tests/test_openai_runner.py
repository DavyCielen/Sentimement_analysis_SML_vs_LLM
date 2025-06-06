import sys
import os
import pytest
import types

# Stub openai module before importing runner
fake_openai = types.SimpleNamespace()
class FakeClient:
    def __init__(self, api_key=None):
        pass
    class chat:
        class completions:
            @staticmethod
            def create(model, messages, max_tokens):
                # Return a response with 'negative' sentiment
                choice = types.SimpleNamespace(message=types.SimpleNamespace(content='negative'))
                return types.SimpleNamespace(choices=[choice])
fake_openai.OpenAI = FakeClient
sys.modules['openai'] = fake_openai

# Ensure project root is on path for sentiment_core import
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, root_dir)
# Add temp directory to path to import runner module
sys.path.insert(1, os.path.join(root_dir, 'temp'))
import open_ai

def test_openai_runner_once(monkeypatch):
    # Set fake API key
    monkeypatch.setenv('OPENAI_API_KEY', 'fake-key')
    # Stub DB helpers
    monkeypatch.setattr(
        open_ai,
        'get_least_used_model_prompt_dataset',
        lambda library, exclude: (4, 5, 6, 'openai_model', 'prompt:{content}', 'dataset', 0)
    )
    monkeypatch.setattr(open_ai, 'fetch_batch', lambda mid, pid, did: [(20, 'world')])
    # Capture update_prediction calls
    calls = []
    def fake_update(row_id, mid, pid, did, prediction, pred_time, formatted):
        calls.append((row_id, prediction, formatted))
    monkeypatch.setattr(open_ai, 'update_prediction', fake_update)
    monkeypatch.setattr(open_ai, 'decrement_count', lambda *args, **kwargs: None)
    monkeypatch.setattr(open_ai, 'revert_batch_status', lambda rows, *args, **kwargs: None)

    # Run runner once with model flag
    sys.argv = ['open_ai.py', '--once', '--model', 'test-model']
    open_ai.main()
    # Verify that update_prediction was called with expected values
    assert calls == [(20, 'negative', 'prompt:world')]