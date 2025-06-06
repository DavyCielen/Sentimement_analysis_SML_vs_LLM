import sys
import os
import types
import pytest

# Stub transformers module before importing runner to avoid heavy imports
class FakePipeline:
    def __call__(self, prompt, labels):
        return {'labels': ['positive']}
fake_transformers = types.SimpleNamespace(pipeline=lambda *args, **kwargs: FakePipeline())
sys.modules['transformers'] = fake_transformers
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# Ensure project root is on path for sentiment_core import
sys.path.insert(0, root_dir)
# Add temp directory to path to import runner module
sys.path.insert(1, os.path.abspath(os.path.join(root_dir, 'temp')))
import bert_classifier

def test_bert_runner_once(monkeypatch):
    # Stub BERT pipeline to always return 'positive'
    class FakePipeline:
        def __call__(self, prompt, labels):
            return {'labels': ['positive']}
    monkeypatch.setattr(bert_classifier, 'bert_pipeline', FakePipeline())

    # Stub DB helpers in runner module
    monkeypatch.setattr(
        bert_classifier,
        'get_least_used_model_prompt_dataset',
        lambda library, exclude: (1, 2, 3, 'bert_model', 'prompt {content}', 'dataset', 0)
    )
    monkeypatch.setattr(bert_classifier, 'fetch_batch', lambda mid, pid, did: [(10, 'hello')])
    # Capture update_prediction calls
    calls = []
    def fake_update(row_id, mid, pid, did, prediction, pred_time, formatted):
        calls.append((row_id, mid, pid, did, prediction, formatted))
    monkeypatch.setattr(bert_classifier, 'update_prediction', fake_update)
    monkeypatch.setattr(bert_classifier, 'decrement_count', lambda *args, **kwargs: None)
    monkeypatch.setattr(bert_classifier, 'revert_batch_status', lambda rows, *args, **kwargs: None)

    # Run runner once
    sys.argv = ['bert_classifier.py', '--once']
    bert_classifier.main()
    # Verify that update_prediction was called with expected values
    assert calls == [(10, 1, 2, 3, 'positive', 'hello')]