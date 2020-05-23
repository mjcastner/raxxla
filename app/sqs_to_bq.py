import json
import multiprocessing
import pprint

import boto3
from absl import app
from absl import flags
from absl import logging
from google.cloud import bigquery
from google.oauth2 import service_account
from lib import schema

# Define args
FLAGS = flags.FLAGS
flags.DEFINE_string('queue_name', None, 'Message queue to read from.')
flags.DEFINE_string('queue_url', None, 'Message queue URL.')
flags.DEFINE_string(
    'service_account_secret',
    None,
    'Name of GCP Service Account information stored in AWS Secrets Manager.')
flags.mark_flag_as_required('queue_name')
flags.mark_flag_as_required('queue_url')
flags.mark_flag_as_required('service_account_secret')

# Global vars
sqs = boto3.resource('sqs')
secrets_manager = boto3.client('secretsmanager')


def stream_rows(bq_client: bigquery.client.Client, json_rows: str, 
                table_name: str, dataset_name: str):
  test = bq_client.get_dataset()

  return True


def main(argv):
  # Instantiate BQ
  service_account_json = secrets_manager.get_secret_value(
      SecretId=FLAGS.service_account_secret).get('SecretString')
  service_account_info = json.loads(service_account_json)

  bq_credentials = service_account.Credentials.from_service_account_info(
      service_account_info)
  bq_client = bigquery.Client(
      credentials=bq_credentials,
      project=bq_credentials.project_id,
  )

  # # Instantiate SQS
  sqs_queue = sqs.get_queue_by_name(QueueName=FLAGS.queue_name)
  available_messages = int(
      sqs_queue.attributes.get('ApproximateNumberOfMessages'))

  # # Process messages
  while available_messages > 0:
    logging.info('%s messages available.', available_messages)
    for message in sqs_queue.receive_messages(MaxNumberOfMessages=10,
                                              MessageAttributeNames=['All']):

      table_name = message.message_attributes.get('table', {}).get(
          'StringValue')
      dataset_name = message.message_attributes.get('dataset', {}).get(
          'StringValue')

      streaming_status = stream_rows(
          bq_client, message.body, table_name, dataset_name)
      if streaming_status:
        message.delete()

    available_messages = int(
        sqs_queue.attributes.get('ApproximateNumberOfMessages'))

  logging.info('Queue processing complete!')


if __name__ == '__main__':
    app.run(main)
