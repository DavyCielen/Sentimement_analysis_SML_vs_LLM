import os
import pandas as pd
from sqlalchemy import create_engine, exc as sqlalchemy_exc
from dotenv import load_dotenv # For loading .env file for local development
from collections import Counter
from typing import List, Dict, Any, Optional

def majority_vote(predictions):
    """
    Determines the majority vote from a list of predictions.

    Args:
        predictions: A list of prediction values.

    Returns:
        The most common prediction value.
    """
    if not predictions:
        return None  # Or raise an error, depending on desired behavior for empty input
    return Counter(predictions).most_common(1)[0][0]

# --- Database Functions ---

def get_db_params_from_env() -> Dict[str, str]:
    """
    Retrieves PostgreSQL database connection parameters from environment variables.

    Environment variables expected:
        POSTGRES_USER: Username for the PostgreSQL database.
        POSTGRES_PASSWORD: Password for the PostgreSQL database.
        POSTGRES_HOST: Host of the PostgreSQL database (e.g., localhost).
        POSTGRES_PORT: Port for the PostgreSQL database (e.g., 5432).
        POSTGRES_DBNAME: Name of the PostgreSQL database.

    Returns:
        A dictionary containing the database parameters.
    
    Raises:
        ValueError: If any of the required environment variables are not set.
    """
    load_dotenv()  # Load .env file if present

    required_vars_map = {
        'user': 'DB_USER',
        'password': 'DB_PASSWORD',
        'host': 'DB_HOST',
        'port': 'DB_PORT',
        'dbname': 'DB_NAME'
    }
    
    db_params: Dict[str, str] = {}
    missing_vars: List[str] = []
    
    for key, env_var_name in required_vars_map.items():
        value = os.getenv(env_var_name)
        if value is None:
            missing_vars.append(env_var_name)
        else:
            db_params[key] = value
            
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
    return db_params

def fetch_data_from_db(query: str) -> Optional[pd.DataFrame]:
    """
    Connects to the PostgreSQL database using environment variables,
    executes the given query, and returns the results as a pandas DataFrame.

    Args:
        query: The SQL query string to execute.

    Returns:
        A pandas DataFrame containing the query results.
        Returns None if configuration, connection or query fails.
    """
    try:
        db_params = get_db_params_from_env()
        connection_string = (
            f"postgresql://{db_params['user']}:{db_params['password']}"
            f"@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
        )
        engine = create_engine(connection_string)
        
        # Using context manager for the connection
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection)
        return df
        
    except ValueError as e:  # From get_db_params_from_env if vars are missing
        print(f"Configuration error: {e}")
        return None
    except sqlalchemy_exc.SQLAlchemyError as e: # More specific SQLAlchemy errors
        print(f"Database connection or query error: {e}")
        return None
    except Exception as e: # Catch any other unexpected errors
        print(f"An unexpected error occurred during database operation: {e}")
        return None


# --- Analysis Helper Functions ---

def calculate_ensemble_prediction(
    df: pd.DataFrame, 
    group_by_cols: List[str], 
    prediction_col: str = 'prediction', 
    ensemble_col_name: str = 'ensemble_prediction'
) -> Optional[pd.DataFrame]:
    """
    Calculates ensemble predictions by applying majority vote after grouping.

    Args:
        df: Pandas DataFrame containing the prediction data.
        group_by_cols: A list of column names to group by.
        prediction_col: The name of the column containing individual predictions.
        ensemble_col_name: The name for the new column containing ensemble predictions.

    Returns:
        A new pandas DataFrame with the group_by columns and the ensemble predictions column,
        or None if input is invalid (e.g., missing columns).
    """
    if df is None or df.empty:
        print("Input DataFrame is None or empty. Cannot calculate ensemble predictions.")
        return None

    missing_group_cols = [col for col in group_by_cols if col not in df.columns]
    if missing_group_cols:
        print(f"Error: The following group_by_cols are not in DataFrame columns: {missing_group_cols}. Available columns: {df.columns.tolist()}")
        return None
    if prediction_col not in df.columns:
        print(f"Error: prediction_col ('{prediction_col}') not in DataFrame columns: {df.columns.tolist()}")
        return None

    print(f"\nCalculating ensemble predictions, grouping by {group_by_cols} on '{prediction_col}' column...")
    
    temp_list_col = '_prediction_list_for_voting' # Temporary column for lists of predictions
    
    try:
        ensembled_df = df.groupby(group_by_cols, as_index=False).agg(
            **{temp_list_col: pd.NamedAgg(column=prediction_col, aggfunc=list)}
        )
    except Exception as e:
        print(f"Error during groupby operation: {e}")
        return None

    if ensembled_df.empty:
        print("Warning: Grouping resulted in an empty DataFrame. No ensemble predictions to calculate.")
        # Return an empty DataFrame with expected columns if possible, or just the empty ensembled_df
        expected_cols = group_by_cols + [ensemble_col_name]
        if temp_list_col in ensembled_df.columns: # if agg produced the temp list col
             expected_cols.append(temp_list_col)
        return pd.DataFrame(columns=expected_cols)

    ensembled_df[ensemble_col_name] = ensembled_df[temp_list_col].apply(majority_vote)
    
    print(f"Calculated ensemble predictions in column '{ensemble_col_name}'.")
    # By default, the temp_list_col is kept for inspection. It can be dropped if desired:
    # ensembled_df = ensembled_df.drop(columns=[temp_list_col])
    return ensembled_df

# --- Example Usage ---
if __name__ == "__main__":
    print("Attempting to fetch data and demonstrate flexible ensemble calculations...")
    print("Please ensure your .env file is set up or environment variables are exported.")
    
    # Updated example query based on user's modification (no WHERE/LIMIT).
    # WARNING: This query might fetch a very large amount of data from your database!
    # For practical use, add appropriate WHERE clauses and potentially a LIMIT.
    example_query = """
    SELECT 
        pr.row_id AS row_id,
        pr.dataset_id AS dataset_id,
        pr.model_id AS model_id,      -- Ensured model_id is aliased for clarity
        pr.prompt_id AS prompt_id,    -- Ensured prompt_id is aliased for clarity
        pr.prediction AS prediction,
        r.expected_prediction AS expected_prediction
    FROM 
        predictions pr
    JOIN
        rows r ON pr.row_id = r.row_id
    JOIN
        models m ON pr.model_id = m.model_id
    """
    
    data_df = fetch_data_from_db(example_query)
    
    if data_df is not None and not data_df.empty:
        print(f"Successfully fetched {len(data_df)} rows. DataFrame columns: {data_df.columns.tolist()}")

        # --- Example 1: Group by row_id and dataset_id ---
        ensemble_df_row_dataset = calculate_ensemble_prediction(
            df=data_df, 
            group_by_cols=['row_id', 'dataset_id'], 
            prediction_col='prediction',
            ensemble_col_name='ensemble_pred_row_ds'
        )
        if ensemble_df_row_dataset is not None:
            print("\nEnsemble by row_id, dataset_id (first 5 rows):")
            print(ensemble_df_row_dataset.head())

        # --- Example 2: Group by prompt_id ---
        # Ensure 'prompt_id' is in data_df.columns from your query
        ensemble_df_prompt = calculate_ensemble_prediction(
            df=data_df, 
            group_by_cols=['prompt_id'], 
            prediction_col='prediction',
            ensemble_col_name='ensemble_pred_prompt'
        )
        if ensemble_df_prompt is not None:
            print("\nEnsemble by prompt_id (first 5 rows):")
            print(ensemble_df_prompt.head())

        # --- Example 3: Group by model_id ---
        # Ensure 'model_id' is in data_df.columns from your query
        ensemble_df_model = calculate_ensemble_prediction(
            df=data_df, 
            group_by_cols=['model_id'], 
            prediction_col='prediction',
            ensemble_col_name='ensemble_pred_model'
        )
        if ensemble_df_model is not None:
            print("\nEnsemble by model_id (first 5 rows):")
            print(ensemble_df_model.head())
            
        # --- Example 4: Group by dataset_id and model_id ---
        ensemble_df_dataset_model = calculate_ensemble_prediction(
            df=data_df, 
            group_by_cols=['dataset_id', 'model_id'], 
            prediction_col='prediction',
            ensemble_col_name='ensemble_pred_ds_model'
        )
        if ensemble_df_dataset_model is not None:
            print("\nEnsemble by dataset_id, model_id (first 5 rows):")
            print(ensemble_df_dataset_model.head())
            
    elif data_df is not None and data_df.empty:
        print("Query executed successfully, but no data was returned. Check your query or database content.")
    else:
        print("Failed to fetch data. Please check environment variables, database connection, and query.")

    # Example of using majority_vote directly with a list of predictions
    sample_predictions_list: List[Any] = ['A', 'B', 'A', 'C', 'A', 'B', 'A']
    mv = majority_vote(sample_predictions_list)
    print(f"\nDirect majority_vote example on {sample_predictions_list} results in: {mv}")

