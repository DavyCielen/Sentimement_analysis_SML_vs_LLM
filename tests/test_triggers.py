import pytest
from sqlalchemy import create_engine, text, inspect

try:
    from testcontainers.postgres import PostgresContainer
except ImportError:
    PostgresContainer = None

DDL = [
    """
    CREATE TABLE modelpromptstatus (
        model_id   INT  NOT NULL,
        prompt_id  INT  NOT NULL,
        dataset_id INT  NOT NULL,
        status     VARCHAR NOT NULL DEFAULT 'pending',
        PRIMARY KEY (model_id, prompt_id, dataset_id)
    );
    """,
    """
    CREATE TABLE predictionstatus (
        row_id INT NOT NULL,
        model_id INT NOT NULL,
        prompt_id INT NOT NULL,
        dataset_id INT NOT NULL,
        status VARCHAR NOT NULL,
        in_progress_time TIMESTAMP,
        PRIMARY KEY (row_id, model_id, prompt_id, dataset_id)
    );
    """,
    """
    CREATE TABLE predictions (
        prediction_id SERIAL PRIMARY KEY,
        row_id INT,
        model_id INT,
        prompt_id INT,
        dataset_id INT,
        prediction VARCHAR NOT NULL,
        prediction_time FLOAT8 NOT NULL,
        status VARCHAR NOT NULL,
        formatted_prompt TEXT
    );
    """,
    """
    CREATE TABLE rows (
        row_id INT PRIMARY KEY,
        dataset_id INT NOT NULL,
        content TEXT,
        expected_prediction VARCHAR
    );
    """,
    """
    CREATE TABLE status_update_log (
        id SERIAL PRIMARY KEY,
        model_id INT,
        prompt_id INT,
        dataset_id INT,
        status VARCHAR,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE OR REPLACE FUNCTION public.add_prediction_status_for_model_prompt_dataset()
    RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        INSERT INTO public.predictionstatus (row_id, model_id, prompt_id, dataset_id, status)
        SELECT r.row_id, NEW.model_id, NEW.prompt_id, NEW.dataset_id, 'pending'
        FROM   public.rows r
        WHERE  r.dataset_id = NEW.dataset_id
        ON CONFLICT DO NOTHING;
        RETURN NEW;
    END;$$;
    """,
    """
    CREATE OR REPLACE FUNCTION public.remove_prediction_status_for_model_prompt_dataset()
    RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        DELETE FROM public.predictionstatus
        WHERE model_id  = OLD.model_id
          AND prompt_id = OLD.prompt_id
          AND dataset_id = OLD.dataset_id;
        RETURN OLD;
    END;$$;
    """,
    """
    CREATE OR REPLACE FUNCTION public.update_in_progress_time()
    RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        IF NEW.status = 'in_progress' THEN
            NEW.in_progress_time := CURRENT_TIMESTAMP;
        END IF;
        RETURN NEW;
    END;$$;
    """,
    """
    CREATE OR REPLACE FUNCTION public.update_modelpromptstatus()
    RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE
        total_rows INTEGER;
        count_rows INTEGER;
    BEGIN
        SELECT COUNT(*) INTO count_rows
        FROM   public.predictions
        WHERE  model_id  = NEW.model_id
          AND  prompt_id = NEW.prompt_id
          AND  dataset_id = NEW.dataset_id;

        IF count_rows % 50 = 0 THEN
            SELECT COUNT(*) INTO total_rows
            FROM   public.predictions
            WHERE  dataset_id = NEW.dataset_id;

            IF count_rows >= total_rows OR count_rows > 5000 THEN
                UPDATE public.modelpromptstatus
                   SET status = 'stop'
                 WHERE model_id  = NEW.model_id
                   AND prompt_id = NEW.prompt_id
                   AND dataset_id = NEW.dataset_id;

                INSERT INTO public.status_update_log (model_id, prompt_id, dataset_id, status)
                VALUES (NEW.model_id, NEW.prompt_id, NEW.dataset_id, 'in_use');
            END IF;
        END IF;
        RETURN NEW;
    END;$$;
    """,
    """
    CREATE TRIGGER after_insert_model_prompt_status
    AFTER INSERT ON modelpromptstatus
    FOR EACH ROW EXECUTE FUNCTION add_prediction_status_for_model_prompt_dataset();
    """,
    """
    CREATE TRIGGER after_delete_model_prompt_status
    AFTER DELETE ON modelpromptstatus
    FOR EACH ROW EXECUTE FUNCTION remove_prediction_status_for_model_prompt_dataset();
    """,
    """
    CREATE TRIGGER before_update_prediction_status
    BEFORE UPDATE ON predictionstatus
    FOR EACH ROW EXECUTE FUNCTION update_in_progress_time();
    """,
    """
    CREATE TRIGGER predictions_after_insert
    AFTER INSERT ON predictions
    FOR EACH ROW EXECUTE FUNCTION update_modelpromptstatus();
    """,
]

@pytest.fixture(scope="session")
def pg_engine():
    if PostgresContainer is None:
        pytest.skip("testcontainers not available")
    with PostgresContainer("postgres:15-alpine") as pg:
        engine = create_engine(pg.get_connection_url())
        with engine.begin() as conn:
            for stmt in DDL:
                conn.execute(text(stmt))
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
