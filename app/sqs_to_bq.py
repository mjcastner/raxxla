import json
import multiprocessing
import pprint

import boto3
from absl import app
from absl import flags
from absl import logging
from google.cloud import bigquery
from lib import schema
#from lib import bq_utils

# Define args
FLAGS = flags.FLAGS
flags.DEFINE_string('queue_name', None, 'Message queue to read from.')
flags.DEFINE_string('queue_url', None, 'Message queue URL.')
flags.mark_flag_as_required('queue_name')
flags.mark_flag_as_required('queue_url')

# Global vars
sqs = boto3.resource('sqs')
bq_client = bigquery.Client()


def stream_rows(json_rows: str, table_name: str, dataset_name: str):
  test = json.loads(json_rows)
  print(len(test))
  #print(json_rows)
  print(table_name)
  print(dataset_name)

  return True


def main(argv):
  # Setup multithreading
  num_cores = multiprocessing.cpu_count()
  num_threads = 10

  if num_cores < num_threads:
    num_threads = num_cores

  # Instantiate SQS queue
  sqs_queue = sqs.get_queue_by_name(QueueName=FLAGS.queue_name)
  available_messages = int(
      sqs_queue.attributes.get('ApproximateNumberOfMessages'))

  # Process messages
  while available_messages > 0:
    logging.info('%s messages available.', available_messages)
    for message in sqs_queue.receive_messages(MaxNumberOfMessages=num_threads,
                                              MessageAttributeNames=['All']):

      table_name = message.message_attributes.get('table', {}).get(
          'StringValue')
      dataset_name = message.message_attributes.get('dataset', {}).get(
          'StringValue')

      streaming_status = stream_rows(message.body, table_name, dataset_name)
      if streaming_status:
        message.delete()
        available_messages = int(
            sqs_queue.attributes.get('ApproximateNumberOfMessages'))

  logging.info('Queue processing complete!')


if __name__ == '__main__':
    app.run(main)
