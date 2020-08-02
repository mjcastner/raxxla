import time

import google

from absl import logging
from google.cloud import bigquery

# Global variables
BQ_CLIENT = bigquery.Client()
JOB_CONFIG = bigquery.LoadJobConfig()
JOB_CONFIG.autodetect = True
JOB_CONFIG.ignore_unknown_values = True


def load_table_from_ndjson(gcs_uri: str, dataset: str, file_type: str):
  logging.info(
      'Creating BigQuery table %s.%s.%s from %s...',
      BQ_CLIENT.project,
      dataset,
      file_type,
      gcs_uri,
  )
  JOB_CONFIG.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
  dataset_ref = safe_get_dataset(dataset)
  table_ref = dataset_ref.table(file_type)
  table_path = '%s.%s.%s' % (BQ_CLIENT.project, dataset, file_type)

  try:
    load_job = BQ_CLIENT.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=JOB_CONFIG
    )
    while not load_job.done():
      time.sleep(2)

    load_job.result()
    logging.info('Table load completed!')
    return BQ_CLIENT.get_table(table_ref)
  except google.api_core.exceptions.BadRequest as e:
    logging.error('Unable to create BigQuery table %s: %s', table_path, e)


def safe_get_dataset(dataset_name: str):
  try:
    output_dataset = BQ_CLIENT.get_dataset(dataset_name)
  except google.api_core.exceptions.NotFound:
    logging.error('Dataset \'%s\' not found, creating...', dataset_name)
    output_dataset = BQ_CLIENT.create_dataset(dataset_name)

  return output_dataset
