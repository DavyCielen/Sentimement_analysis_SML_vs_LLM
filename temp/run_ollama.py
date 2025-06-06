"""
Ollama sentiment classification runner using shared sentiment_core library.
"""
import argparse
import time
import logging
import subprocess
import requests

import psycopg2
from ollama import chat

from sentiment_core.config import db_params, batch_size
from sentiment_core.parsers import parse_sentiment
from sentiment_core.db_helpers import (
    get_least_used_model_prompt_dataset,
    fetch_batch,
    update_prediction,
    revert_batch_status,
    decrement_count,
)

def main():
    parser = argparse.ArgumentParser(description="Run Ollama sentiment classification workflow")
    parser.add_argument('--once', action='store_true', help='Process only one batch then exit')
    parser.add_argument('--model', required=True, help='Ollama model name')
    args = parser.parse_args()

    model_name = args.model
    # Start Ollama service for the model
    subprocess.Popen(["ollama", "run", model_name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    url = "http://localhost:11434/api/generate"
    # Wait for service readiness
    for _ in range(30):
        try:
            data = {"model": model_name, "messages": [{'role': 'user', 'content': 'ping'}]}
            r = requests.post(url, json=data, timeout=5)
            if r.status_code == 200:
                break
        except Exception:
            time.sleep(1)
    else:
        raise RuntimeError("Ollama service did not start in time")

    exclude_prompt_ids = []
    while True:
        model_info = get_least_used_model_prompt_dataset('ollama', exclude_prompt_ids)
        if model_info is None:
            print('No available model-prompt-dataset combination found.')
            break

        model_id, prompt_id, dataset_id, model_name, prompt_text, dataset_name, _ = model_info
        print(f"Using model: {model_name} with prompt: {prompt_text} on dataset: {dataset_name}")

        rows = fetch_batch(model_id, prompt_id, dataset_id)
        if not rows:
            decrement_count(model_id, prompt_id, dataset_id)
            if args.once:
                break
            continue

        try:
            for row_id, content in rows:
                start_time = time.time()
                formatted = prompt_text.format(content=content)
                resp = chat(model_name, [{'role': 'user', 'content': formatted}])
                sentiment = parse_sentiment(resp['message']['content'])
                duration = time.time() - start_time
                update_prediction(row_id, model_id, prompt_id, dataset_id, sentiment, duration, formatted)
                logging.info(f"Processed row {row_id} with model {model_name}")
                if args.once:
                    return
        except Exception as e:
            logging.error(f"Error occurred: {e}. Reverting batch status.")
            revert_batch_status(rows, model_id, prompt_id, dataset_id)
            if args.once:
                return

        decrement_count(model_id, prompt_id, dataset_id)
        exclude_prompt_ids.append(prompt_id)
        if args.once:
            break

if __name__ == '__main__':
    main()