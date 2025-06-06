import pytest

import sentiment_core.db_helpers as dbh

class FakeCursor:
    def __init__(self, fetchone_result=None, fetchall_result=None):
        self.fetchone_result = fetchone_result
        self.fetchall_result = fetchall_result
        self.executed = []
    def execute(self, sql, params=None):
        # Record the SQL and parameters
        self.executed.append((sql.strip(), params))
    def fetchone(self):
        return self.fetchone_result
    def fetchall(self):
        return self.fetchall_result
    def close(self):
        pass

class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False
    def cursor(self):
        return self._cursor
    def commit(self):
        self.committed = True
    def close(self):
        pass

def test_get_least_used_none(monkeypatch):
    # Simulate no available record
    fake_cursor = FakeCursor(fetchone_result=None)
    conn = FakeConnection(fake_cursor)
    monkeypatch.setattr(dbh.psycopg2, 'connect', lambda **kwargs: conn)
    result = dbh.get_least_used_model_prompt_dataset('bert', exclude_prompt_ids=[1, 2])
    assert result is None
    # Ensure we attempted to select one row
    assert any('LIMIT 1' in sql for sql, _ in fake_cursor.executed)

def test_get_least_used_success(monkeypatch):
    # Simulate a returned record tuple including library
    record = (10, 20, 30, 'model', 'text {content}', 'dataset', 5, 'bert')
    fake_cursor = FakeCursor(fetchone_result=record)
    conn = FakeConnection(fake_cursor)
    monkeypatch.setattr(dbh.psycopg2, 'connect', lambda **kwargs: conn)
    out = dbh.get_least_used_model_prompt_dataset('bert', exclude_prompt_ids=[2, 3])
    # Should return all except the library field
    assert out == record[:-1]
    # Check lock, update, and unlock were executed
    sqls = [sql for sql, _ in fake_cursor.executed]
    assert any('pg_advisory_lock' in sql for sql in sqls)
    assert any('UPDATE ModelPromptStatus' in sql for sql in sqls)
    assert any('pg_advisory_unlock' in sql for sql in sqls)
    assert conn.committed

@pytest.mark.parametrize('rows', [[], [(1, 'a'), (2, 'b')]])
def test_fetch_batch(monkeypatch, rows):
    fake_cursor = FakeCursor(fetchall_result=rows)
    conn = FakeConnection(fake_cursor)
    monkeypatch.setattr(dbh.psycopg2, 'connect', lambda **kwargs: conn)
    out = dbh.fetch_batch(1, 2, 3)
    assert out == rows
    if rows:
        # Should update status to in_progress for fetched rows
        assert any('UPDATE PredictionStatus' in sql for sql, _ in fake_cursor.executed)

def test_update_prediction(monkeypatch):
    fake_cursor = FakeCursor()
    conn = FakeConnection(fake_cursor)
    monkeypatch.setattr(dbh.psycopg2, 'connect', lambda **kwargs: conn)
    # Call with sample data
    dbh.update_prediction(5, 6, 7, 8, 'PRED', 0.123, 'fmt')
    # Should INSERT into Predictions and UPDATE PredictionStatus
    sqls = [sql for sql, _ in fake_cursor.executed]
    assert any('INSERT INTO Predictions' in sql for sql in sqls)
    assert any('UPDATE PredictionStatus' in sql for sql in sqls)
    assert conn.committed

def test_revert_batch_status(monkeypatch):
    rows = [(9, 'x'), (10, 'y')]
    fake_cursor = FakeCursor()
    conn = FakeConnection(fake_cursor)
    monkeypatch.setattr(dbh.psycopg2, 'connect', lambda **kwargs: conn)
    dbh.revert_batch_status(rows, 1, 2, 3)
    # Should set statuses back to pending
    assert any('UPDATE PredictionStatus' in sql for sql, _ in fake_cursor.executed)
    assert conn.committed

def test_decrement_count(monkeypatch):
    fake_cursor = FakeCursor()
    conn = FakeConnection(fake_cursor)
    monkeypatch.setattr(dbh.psycopg2, 'connect', lambda **kwargs: conn)
    dbh.decrement_count(11, 12, 13)
    # Lock, update, and unlock
    sqls = [sql for sql, _ in fake_cursor.executed]
    assert any('pg_advisory_lock' in sql for sql in sqls)
    assert any('UPDATE ModelPromptStatus' in sql for sql in sqls)
    assert any('pg_advisory_unlock' in sql for sql in sqls)
    assert conn.committed