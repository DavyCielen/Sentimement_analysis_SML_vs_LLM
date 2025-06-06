 Here’s what I found in the temp directory:

    Files present (6)
    • bert_classifier.py
    • open_ai.py
    • run_ollama.py
    • dockerfile.bert
    • dockerfile.openai
    • dockerfile.gpt4all

        1. Heavy code-duplication among the three Python scripts:
           – Identical imports (psycopg2, time, logging, re), logging setup, DB connection
    parameters and batch_size.
           – The parse_sentiment() function is the same in all three.
           – The get_least_used_model_prompt_dataset, fetch_batch, update_prediction,
    revert_batch_status, decrement_count functions all follow the same structure (only
    small tweaks to SQL or “library” value).
           – A near-identical main processing loop in each, differing only in how the Model
     is invoked (Transformers vs OpenAI vs Ollama).
        2. Boilerplate repetition across the three Dockerfiles:
           – All use python:3.9-slim, set WORKDIR /app, COPY . .
           – Each installs its own pip dependencies but otherwise share the same layout.
           – The CMD/ENTRYPOINT lines differ only in which script they launch.

    No pair of files are byte-for-byte identical, but ~70–80% of the code in the three
    scripts—and most of the Dockerfile scaffolding—is duplicated.
user
in a next step we want to migrate but how can we write tests that everything will work
after the refactor?

    thinking for 7s

    codex
    Here’s a high‐level testing strategy to give you confidence that after you pull all
    that shared logic into a common library and slim down each “runner” script, nothing
    will break.

        1. Extract your shared logic into a small package
           • Move `parse_sentiment`, the DB-helper functions
    (`get_least_used_model_prompt_dataset`, `fetch_batch`, etc.) and any other common bits
    into something like `sentiment_core/`.
           • Keep each “runner” script (`bert_classifier.py`, `open_ai.py`,
    `run_ollama.py`) as a thin adapter that imports from `sentiment_core` and plugs in its
    own `.generate(...)` call.
        2. Unit-tests for pure functions
           • parse_sentiment: cover all branches (“positive”, “negative”, “neutral”, and
    “unknown”).
           • Any other pure helper you pull out.
        3. Unit-tests for Model adapters with mocks
           • Use `pytest` and `monkeypatch` (or `unittest.mock`) to stub out the actual API
     calls or transformer pipeline.
           • Feed in a fake response and assert that your adapter (e.g.
    `BertModel.generate`, `OpenAIModel.generate`, `OllamaModel.generate`) invokes
    `parse_sentiment` correctly and returns the expected label.
        4. Database‐helper tests
           • Spin up a throwaway PostgreSQL for tests (via Docker Compose or
    [`testcontainers-python`](https://github.com/testcontainers/testcontainers-python)).
           • Create your schema and seed it with a few rows.
           • Write tests for `get_least_used_model_prompt_dataset`, `fetch_batch`,
    `update_prediction`, etc., asserting that the right rows are returned or updated.
           • Tear down the container between tests so each test sees a clean DB.
        5. Lightweight integration tests
           • Write a small fixture that seeds the DB with exactly one model/prompt/dataset
    row and a few “pending” rows.
           • Launch one of your runner scripts via `subprocess.run([..., "--once"])` or
    similar (you’ll need to add a flag to exit after one batch).
           • Assert afterwards that all “pending” rows have become “done” and that the
    count fields moved up/down correctly.
        6. Smoke-test your Dockerfiles
           • Add CI jobs that do `docker build -f dockerfile.bert .`, etc., to ensure they
    still build.
           • Optionally spin up the container (with a test DB via Docker Compose) and
    ensure the entrypoint runs without immediate errors.
        7. Automation & CI
           • Use `pytest` with coverage to make sure you’re hitting both core logic and the
     adapters.
           • On every PR, run the full suite (unit, DB‐container tests, Docker builds) so
    you catch integration breaks early.

    —

    By layering tests from pure functions all the way up to end‐to‐end runs in a throwaway
    DB, you’ll be able to refactor freely into shared modules and know—with high
    confidence—that nothing regresses.
user