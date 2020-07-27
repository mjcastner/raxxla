import google

from absl import logging
from google.cloud import bigquery

# Global variables
BQ_CLIENT = bigquery.Client()
JOB_CONFIG = bigquery.LoadJobConfig()


def load_table_from_ndjson(gcs_uri: str, dataset: str, file_type: str):
  JOB_CONFIG.autodetect = True
  JOB_CONFIG.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
  dataset_ref = safe_get_dataset(dataset)
  table_ref = dataset_ref.table(file_type)

  load_job = BQ_CLIENT.load_table_from_uri(
      gcs_uri,
      table_ref,
      job_config=JOB_CONFIG
  )
  load_job.result()

  return table_ref


def safe_get_dataset(dataset_name: str):
  try:
    output_dataset = BQ_CLIENT.get_dataset(dataset_name)
  except google.api_core.exceptions.NotFound:
    logging.error('Dataset \'%s\' not found, creating...', dataset_name)
    output_dataset = BQ_CLIENT.create_dataset(dataset_name)

  return output_dataset
