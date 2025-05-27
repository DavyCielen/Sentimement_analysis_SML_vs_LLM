import pytest
from sqlalchemy import create_engine, text, inspect
import os
import sys
# Ensure project root is on PYTHONPATH for db_setup import
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db_setup import create_schema

# Clean up data between tests to ensure isolation
@pytest.fixture(autouse=True)
def cleanup_db(pg_engine):
    """
    Truncate all operational tables before each test to isolate state.
    """
    with pg_engine.begin() as conn:
        conn.execute(text(
            "TRUNCATE TABLE predictionstatus, predictions, modelpromptstatus, rows, status_update_log RESTART IDENTITY CASCADE"
        ))
    yield

try:
    from testcontainers.postgres import PostgresContainer
except ImportError:
    PostgresContainer = None


@pytest.fixture(scope="session")
def pg_engine():
    if PostgresContainer is None:
        pytest.skip("testcontainers not available")
    with PostgresContainer("postgres:15-alpine") as pg:
        engine = create_engine(pg.get_connection_url())
        with engine.begin() as conn:
            create_schema(conn)
        yield engine
        engine.dispose()

def test_schema_presence(pg_engine):
    insp = inspect(pg_engine)
    expected_tables = {
        "modelpromptstatus",
        "predictionstatus",
        "predictions",
        "rows",
        "status_update_log",
    }
    assert expected_tables.issubset(set(insp.get_table_names()))

    columns = {
        "modelpromptstatus": {"model_id", "prompt_id", "dataset_id", "status"},
        "predictionstatus": {
            "row_id",
            "model_id",
            "prompt_id",
            "dataset_id",
            "status",
            "in_progress_time",
        },
    }
    for table, cols in columns.items():
        names = {c["name"] for c in insp.get_columns(table)}
        assert cols.issubset(names)

    with pg_engine.connect() as conn:
        funcs = {
            "add_prediction_status_for_model_prompt_dataset",
            "remove_prediction_status_for_model_prompt_dataset",
            "update_in_progress_time",
            "update_modelpromptstatus",
        }
        for f in funcs:
            res = conn.execute(text("""SELECT COUNT(*) FROM pg_proc WHERE proname=:f"""), {"f": f}).scalar_one()
            assert res == 1
        triggers = {
            "after_insert_model_prompt_status",
            "after_delete_model_prompt_status",
            "before_update_prediction_status",
            "predictions_after_insert",
        }
        for t in triggers:
            res = conn.execute(text("""SELECT COUNT(*) FROM pg_trigger WHERE tgname=:t"""), {"t": t}).scalar_one()
            assert res == 1
        # Ensure no existing predictionstatus rows before workflow tests
        initial = conn.execute(text("SELECT COUNT(*) FROM predictionstatus")).scalar_one()
        assert initial == 0

def test_predictionstatus_content_after_modelpromptstatus(pg_engine):
    """
    After inserting rows and a modelpromptstatus entry, predictionstatus
    should contain one pending entry per row with correct identifiers.
    """
    with pg_engine.begin() as conn:
        # Seed rows table
        conn.execute(text(
            "INSERT INTO rows (row_id, dataset_id, content, expected_prediction)"
            " VALUES (10, 5, 'foo', 'FOO'), (20, 5, 'bar', 'BAR')"
        ))
        # Insert a model-prompt-dataset job
        conn.execute(text(
            "INSERT INTO modelpromptstatus (model_id, prompt_id, dataset_id, status)"
            " VALUES (1, 2, 5, 'pending')"
        ))
        # Verify predictionstatus rows
        result = conn.execute(text(
            "SELECT row_id, model_id, prompt_id, dataset_id, status, in_progress_time"
            " FROM predictionstatus ORDER BY row_id"
        ))
        rows = result.fetchall()
        # Expect exactly two entries
        assert len(rows) == 2
        # Check each entry's fields
        expected_ids = [10, 20]
        for row in rows:
            # row is a tuple: (row_id, model_id, prompt_id, dataset_id, status, in_progress_time)
            row_id, model_id, prompt_id, dataset_id, status, in_progress_time = row
            assert row_id in expected_ids
            assert model_id == 1
            assert prompt_id == 2
            assert dataset_id == 5
            assert status == 'pending'
            assert in_progress_time is None

def test_trigger_workflow(pg_engine):
    with pg_engine.begin() as conn:
        conn.execute(text("INSERT INTO rows (row_id, dataset_id, content, expected_prediction) VALUES (1, 42, 'a', 'A'), (2, 42, 'b', 'B')"))
        conn.execute(text("INSERT INTO modelpromptstatus (model_id, prompt_id, dataset_id, status) VALUES (99, 7, 42, 'pending')"))
        count = conn.execute(text("SELECT COUNT(*) FROM predictionstatus")).scalar_one()
        assert count == 2

        conn.execute(text("DELETE FROM modelpromptstatus WHERE model_id=99 AND prompt_id=7 AND dataset_id=42"))
        count = conn.execute(text("SELECT COUNT(*) FROM predictionstatus")).scalar_one()
        assert count == 0

        conn.execute(text("INSERT INTO modelpromptstatus (model_id, prompt_id, dataset_id, status) VALUES (99, 7, 42, 'pending')"))
        conn.execute(text("UPDATE predictionstatus SET status='in_progress' WHERE row_id=1 AND model_id=99 AND prompt_id=7 AND dataset_id=42"))
        ts = conn.execute(text("SELECT in_progress_time FROM predictionstatus WHERE row_id=1 AND model_id=99 AND prompt_id=7 AND dataset_id=42")).scalar_one()
        assert ts is not None

        for i in range(50):
            rid = 1 if i % 2 == 0 else 2
            conn.execute(text("INSERT INTO predictions (row_id, model_id, prompt_id, dataset_id, prediction, prediction_time, status) VALUES (:rid, 99, 7, 42, 'ok', 0.1, 'success')"), {"rid": rid})

        status = conn.execute(text("SELECT status FROM modelpromptstatus WHERE model_id=99 AND prompt_id=7 AND dataset_id=42")).scalar_one()
        assert status == 'stop'
        log_count = conn.execute(text("SELECT COUNT(*) FROM status_update_log WHERE model_id=99 AND prompt_id=7 AND dataset_id=42")).scalar_one()
        assert log_count == 1
