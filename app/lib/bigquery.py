import google
import pandas

from absl import logging
from google.cloud import bigquery

# Global variables
STREAMING_BATCH_SIZE = 500
BQ_CLIENT = bigquery.Client()


def safe_get_dataset(dataset_name: str):
  try:
    output_dataset = BQ_CLIENT.get_dataset(dataset_name)
  except google.api_core.exceptions.NotFound:
    logging.error('Dataset \'%s\' not found, creating...', dataset_name)
    output_dataset = BQ_CLIENT.create_dataset(dataset_name)

  return output_dataset


def safe_get_table(table_name: str):
  try:
    output_table = BQ_CLIENT.get_table(table_name)
  except google.api_core.exceptions.NotFound:
    logging.error('Table \'%s\' not found, creating...', table_name)
    output_table = bigquery.Table(table_name)
    output_table = BQ_CLIENT.create_table(output_table)

    return output_table


def stream_records(table: str, row_records: list):
  logging.info(
      'Streaming %s rows into BigQuery table %s...',
      len(row_records),
      table
  )

  table_metadata = table.split('.')
  row_df = pandas.DataFrame(row_records)
  safe_get_dataset(table_metadata[1])
  bq_table = safe_get_table(table)
  BQ_CLIENT.load_table_from_dataframe(row_df, bq_table)
  #pubsub.ack_message_batch(messages)
