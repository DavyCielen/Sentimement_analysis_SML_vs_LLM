import sys
import os
import pytest
import types

# Stub ollama module before importing runner
fake_ollama = types.SimpleNamespace()
fake_ollama.chat = lambda model, messages: {'message': {'content': 'neutral'}}
sys.modules['ollama'] = fake_ollama

# Ensure project root is on path for sentiment_core import
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, root_dir)
# Add temp directory to path to import runner module
sys.path.insert(1, os.path.join(root_dir, 'temp'))
import run_ollama

def test_run_ollama_runner_once(monkeypatch):
    # Stub subprocess.Popen and requests.post
    monkeypatch.setattr(run_ollama.subprocess, 'Popen', lambda *args, **kwargs: None)
    monkeypatch.setattr(
        run_ollama,
        'requests',
        types.SimpleNamespace(post=lambda url, json, timeout: types.SimpleNamespace(status_code=200))
    )
    # Stub DB helpers in runner module
    monkeypatch.setattr(
        run_ollama,
        'get_least_used_model_prompt_dataset',
        lambda library, exclude: (7, 8, 9, 'ollama_model', 'prompt {content}', 'dataset', 0)
    )
    monkeypatch.setattr(run_ollama, 'fetch_batch', lambda mid, pid, did: [(30, 'abc')])
    # Capture update_prediction calls
    calls = []
    def fake_update(row_id, mid, pid, did, prediction, pred_time, formatted):
        calls.append((row_id, prediction, formatted))
    monkeypatch.setattr(run_ollama, 'update_prediction', fake_update)
    monkeypatch.setattr(run_ollama, 'decrement_count', lambda *args, **kwargs: None)
    monkeypatch.setattr(run_ollama, 'revert_batch_status', lambda rows, *args, **kwargs: None)

    # Run runner once with model flag
    sys.argv = ['run_ollama.py', '--once', '--model', 'test-model']
    run_ollama.main()
    # Verify that update_prediction was called with expected values
    assert calls == [(30, 'neutral', 'prompt abc')]