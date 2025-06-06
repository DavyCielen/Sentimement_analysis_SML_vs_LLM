"""
Database helper functions for sentiment_core.
"""
import psycopg2
from .config import db_params, batch_size

def get_least_used_model_prompt_dataset(library: str, exclude_prompt_ids=None):
    """
    Acquire the least used model-prompt-dataset combination for the given library.
    """
    exclude_prompt_ids = exclude_prompt_ids or []
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    try:
        exclude_clause = ''
        if exclude_prompt_ids:
            placeholders = ','.join(str(x) for x in exclude_prompt_ids)
            exclude_clause = f"AND mps.prompt_id NOT IN ({placeholders})"
        # Choose ordering based on library
        if library == 'openai':
            order_clause = 'ORDER BY mps.model_id * RANDOM()'
        else:
            order_clause = 'ORDER BY mps.count ASC'
        sql = f"""
            SELECT mps.model_id, mps.prompt_id, mps.dataset_id,
                   m.name, p.text, d.name AS dataset_name,
                   mps.count, m.library
            FROM ModelPromptStatus mps
            JOIN Models m ON mps.model_id = m.model_id
            JOIN Prompts p ON mps.prompt_id = p.prompt_id
            JOIN Datasets d ON mps.dataset_id = d.dataset_id
            WHERE (mps.status = 'available' OR mps.status = 'in_use')
              AND m.library = %s {exclude_clause}
            {order_clause}
            LIMIT 1
        """
        cursor.execute(sql, (library,))
        result = cursor.fetchone()
        if not result:
            return None
        model_id, prompt_id, dataset_id, model_name, prompt_text, dataset_name, count, _ = result
        # Lock, update count, and mark in use
        cursor.execute("SELECT pg_advisory_lock(%s)", (model_id,))
        cursor.execute(
            """
            UPDATE ModelPromptStatus
            SET count = count + 1, status = 'in_use'
            WHERE model_id = %s AND prompt_id = %s AND dataset_id = %s
            """,
            (model_id, prompt_id, dataset_id),
        )
        cursor.execute("SELECT pg_advisory_unlock(%s)", (model_id,))
        conn.commit()
        return model_id, prompt_id, dataset_id, model_name, prompt_text, dataset_name, count
    finally:
        cursor.close()
        conn.close()

def fetch_batch(model_id, prompt_id, dataset_id):
    """
    Reserve and return a batch of pending rows.
    """
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT row_id, content
            FROM Rows
            WHERE dataset_id = %s AND row_id IN (
                SELECT row_id
                FROM PredictionStatus
                WHERE model_id = %s AND prompt_id = %s AND dataset_id = %s AND status = 'pending'
                LIMIT %s
            )
            FOR UPDATE SKIP LOCKED
            """,
            (dataset_id, model_id, prompt_id, dataset_id, batch_size),
        )
        rows = cursor.fetchall()
        if rows:
            ids = [r[0] for r in rows]
            cursor.execute(
                """
                UPDATE PredictionStatus
                SET status = 'in_progress'
                WHERE row_id = ANY(%s) AND model_id = %s AND prompt_id = %s AND dataset_id = %s
                """,
                (ids, model_id, prompt_id, dataset_id),
            )
            conn.commit()
        return rows
    finally:
        cursor.close()
        conn.close()

def update_prediction(row_id, model_id, prompt_id, dataset_id, prediction, prediction_time, formatted_prompt):
    """
    Insert a prediction record and mark status done.
    """
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO Predictions (
                row_id, model_id, prompt_id, dataset_id,
                prediction, prediction_time, status, formatted_prompt
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row_id, model_id, prompt_id, dataset_id,
                prediction.strip().lower(), prediction_time,
                'done', formatted_prompt.strip().lower(),
            ),
        )
        cursor.execute(
            """
            UPDATE PredictionStatus
            SET status = 'done'
            WHERE row_id = %s AND model_id = %s AND prompt_id = %s AND dataset_id = %s
            """,
            (row_id, model_id, prompt_id, dataset_id),
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def revert_batch_status(rows, model_id, prompt_id, dataset_id):
    """
    Reset batch status back to pending on error.
    """
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    try:
        ids = [r[0] for r in rows]
        cursor.execute(
            """
            UPDATE PredictionStatus
            SET status = 'pending'
            WHERE row_id = ANY(%s) AND model_id = %s AND prompt_id = %s AND dataset_id = %s
            """,
            (ids, model_id, prompt_id, dataset_id),
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def decrement_count(model_id, prompt_id, dataset_id):
    """
    Decrement the count on ModelPromptStatus and release lock.
    """
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT pg_advisory_lock(%s)", (model_id,))
        cursor.execute(
            """
            UPDATE ModelPromptStatus
            SET count = count - 1
            WHERE model_id = %s AND prompt_id = %s AND dataset_id = %s
            """,
            (model_id, prompt_id, dataset_id),
        )
        cursor.execute("SELECT pg_advisory_unlock(%s)", (model_id,))
        conn.commit()
    finally:
        cursor.close()
        conn.close()