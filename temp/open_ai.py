"""
OpenAI sentiment classification runner using shared sentiment_core library.
"""
import argparse
import time
import logging
import os

import psycopg2
from openai import OpenAI

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
    parser = argparse.ArgumentParser(description="Run OpenAI sentiment classification workflow")
    parser.add_argument('--once', action='store_true', help='Process only one batch then exit')
    parser.add_argument('--model', required=True, help='OpenAI model name (e.g., text-davinci-003)')
    args = parser.parse_args()

    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        raise RuntimeError('OPENAI_API_KEY environment variable is not set')
    client = OpenAI(api_key=api_key)

    exclude_prompt_ids = []
    while True:
        model_info = get_least_used_model_prompt_dataset('openai', exclude_prompt_ids)
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
                resp = client.chat.completions.create(
                    model=model_name,
                    messages=[
                        {'role': 'system', 'content': 'You are a researcher helping craft prompts.'},
                        {'role': 'user', 'content': formatted},
                    ],
                    max_tokens=3,
                )
                out = resp.choices[0].message.content.strip()
                sentiment = parse_sentiment(out)
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