import json
import multiprocessing

import boto3
from absl import app
from absl import flags
from absl import logging
from lib import schema

# Define args
FLAGS = flags.FLAGS
flags.DEFINE_string('queue_name', None, 'Message queue to send output.')
flags.DEFINE_string('queue_url', None, 'Message queue URL.') 
flags.mark_flag_as_required('queue_name')
flags.mark_flag_as_required('queue_url')

sqs = boto3.resource('sqs')


def main(argv):
  # Setup multithreading
  num_cores = multiprocessing.cpu_count()
  num_threads = 10

  if num_cores < num_threads:
    num_threads = num_cores

  # Instantiate SQS queue
  sqs_queue = sqs.get_queue_by_name(QueueName=FLAGS.queue_name)

  # Process messages
  # Change this to be while messages in flight > 0
  while True:
    for message in sqs_queue.receive_messages(MaxNumberOfMessages=num_threads):
      print(message.body)


if __name__ == '__main__':
    app.run(main)
