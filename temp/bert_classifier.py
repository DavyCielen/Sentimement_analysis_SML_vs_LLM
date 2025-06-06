"""
Ossified BERT classifier runner that uses the shared sentiment_core library.
"""
import argparse
import time
import logging

import psycopg2
from transformers import pipeline

from sentiment_core.config import db_params, batch_size
from sentiment_core.parsers import parse_sentiment
from sentiment_core.db_helpers import (
    get_least_used_model_prompt_dataset,
    fetch_batch,
    update_prediction,
    revert_batch_status,
    decrement_count,
)

# Initialize BERT zero-shot classification pipeline once
bert_pipeline = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")

def main():
    parser = argparse.ArgumentParser(description="Run BERT sentiment classification workflow")
    parser.add_argument(
        '--once', action='store_true', help='Process only one batch then exit'
    )
    args = parser.parse_args()

    exclude_prompt_ids = []
    while True:
        # Acquire next model-prompt-dataset for BERT
        model_info = get_least_used_model_prompt_dataset('bert', exclude_prompt_ids)
        if model_info is None:
            print("No available model-prompt-dataset combination found.")
            break

        model_id, prompt_id, dataset_id, model_name, prompt_text, dataset_name, _ = model_info
        print(f"Using model: {model_name} with prompt: {prompt_text} on dataset: {dataset_name}")

        # Fetch and process one batch
        rows = fetch_batch(model_id, prompt_id, dataset_id)
        if not rows:
            decrement_count(model_id, prompt_id, dataset_id)
            if args.once:
                break
            continue

        try:
            for row_id, content in rows:
                start_time = time.time()
                labels = ['positive', 'negative', 'neutral']
                if dataset_id == 2:
                    labels = ['positive', 'negative']
                # Run BERT zero-shot classification
                resp = bert_pipeline(prompt_text.format(content=content), labels)
                sentiment = parse_sentiment(resp['labels'][0])
                duration = time.time() - start_time
                update_prediction(row_id, model_id, prompt_id, dataset_id, sentiment, duration, content)
                logging.info(f"Processed row {row_id} with model {model_name}")
                if args.once:
                    return
        except Exception as e:
            logging.error(f"Error occurred: {e}. Reverting batch status.")
            revert_batch_status(rows, model_id, prompt_id, dataset_id)
            if args.once:
                return

        # After batch, decrement count and exclude this prompt
        decrement_count(model_id, prompt_id, dataset_id)
        exclude_prompt_ids.append(prompt_id)
        if args.once:
            break

if __name__ == "__main__":
    main()