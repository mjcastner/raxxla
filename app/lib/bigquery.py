import json

import boto3
import google
from absl import logging
from google.cloud import bigquery
from google.oauth2 import service_account


class client(bigquery.client.Client):
  def __init__(self, aws_secret_key: str):
    # Initialize BQ client from AWS Secrets Manager
    self.job_config = bigquery.LoadJobConfig()
    secrets_manager = boto3.client('secretsmanager')
    service_account_json = secrets_manager.get_secret_value(
        SecretId=aws_secret_key).get('SecretString')
    service_account_info = json.loads(service_account_json)
    bq_credentials = service_account.Credentials.from_service_account_info(
        service_account_info)
    super().__init__(
        credentials=bq_credentials,
        project=bq_credentials.project_id)


  def safe_get_dataset(self, dataset_name: str):
    try:
      output_dataset = self.get_dataset(dataset_name)
    except google.api_core.exceptions.NotFound as e:
      logging.error('Dataset \'%s\' not found, creating...', dataset_name)
      output_dataset = self.create_dataset(dataset_name)

    return output_dataset


  def safe_get_table(self, table_name: str, table_schema: list):
    try:
      output_table = self.get_table(table_name)
    except google.api_core.exceptions.NotFound as e:
      logging.error('Table \'%s\' not found, creating...', table_name)
      output_table = bigquery.Table(table_name, schema=table_schema)
      output_table = self.create_table(output_table)

    return output_table
