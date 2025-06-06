# Refactor & Testing TODO List

1. Extract shared logic into `sentiment_core/`
   - Create `sentiment_core/` package with `__init__.py`
   - Move in:
     - `parse_sentiment`
     - DB-helper functions: `get_least_used_model_prompt_dataset`, `fetch_batch`, `update_prediction`, `revert_batch_status`, `decrement_count`
     - Common logging setup & config constants (connection params, `batch_size`)

2. Refactor runner scripts
   - In each of `bert_classifier.py`, `open_ai.py`, `run_ollama.py`:
     - Remove duplicated imports/helpers, import from `sentiment_core`
     - Retain only model-specific `.generate(...)` integration
     - Add `--once` CLI flag to process one batch then exit

3. Pure-function unit tests
   - Write `pytest` tests for `parse_sentiment` (cover “positive”, “negative”, “neutral”, “unknown”)
   - Test any other pure helpers extracted

4. Adapter-unit tests with mocks
   - For each adapter (Bert, OpenAI, Ollama):
     - Mock the external call (transformers pipeline, OpenAI API, Ollama RPC)
     - Assert `.generate()` returns correct label via `parse_sentiment`

5. Database-helper tests
   - Use `testcontainers-python` (or Docker Compose) to spin up throwaway Postgres
   - Apply schema & seed minimal rows before each test
   - Tests for: `get_least_used_model_prompt_dataset`, `fetch_batch`, `update_prediction`, `revert_batch_status`, `decrement_count`
   - Tear down container between tests

6. Lightweight integration tests
   - Fixture: seed DB with one model/prompt/dataset + pending rows
   - Call runner script via `subprocess.run([...,'--once'])`
   - Assert pending→done and counter updates

7. Dockerfile smoke-tests
   - CI: `docker build -f dockerfile.bert .`, etc.
   - (Optional) Run container against test DB to verify startup

8. CI pipeline configuration
   - Hook up `pytest` (with coverage) for unit, adapter, DB-helper, integration tests
   - Add Docker build steps for each Dockerfile on every PR