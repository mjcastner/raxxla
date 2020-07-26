import time

from absl import flags
from absl import logging
from google.cloud import pubsub
from google.cloud.pubsub import types

# Define flags
FLAGS = flags.FLAGS
flags.DEFINE_string('project_id', None, 'Google Cloud project ID.')
flags.DEFINE_string('pubsub_topic', None, 'Pub/Sub topic ID.')
flags.DEFINE_string('pubsub_subscription', None, 'Pub/Sub subscription ID.')
flags.mark_flag_as_required('project_id')

# Global vars
PUBLISHER = pubsub.PublisherClient(
    batch_settings=types.BatchSettings(max_messages=500),
)
SUBSCRIBER = pubsub.SubscriberClient()


def ack_message_batch(message_batch):
  subscription_path = SUBSCRIBER.subscription_path(
      FLAGS.project_id, 
      FLAGS.pubsub_subscription,
  )
  ack_ids = [x.ack_id for x in message_batch]
  SUBSCRIBER.acknowledge(subscription_path, ack_ids)


def send_bigquery_row(row_json: str, table: str):
  if not FLAGS.pubsub_topic:
    logging.error('--pubsub_topic flag must be provided.')
    raise SyntaxError

  logging.info('Sending row to Pub/Sub topic %s', FLAGS.pubsub_topic)
  topic_path = PUBLISHER.topic_path(FLAGS.project_id, FLAGS.pubsub_topic)
  response_future = PUBLISHER.publish(
      topic_path,
      data=row_json.encode("utf-8"),
      table=table,
  )

  return response_future


def fetch_bigquery_batch(batch_size: int):
  if not FLAGS.pubsub_subscription:
    logging.error('--pubsub_subscription flag must be provided.')
    raise SyntaxError

  logging.info('Fetching message batch (%s messages)...', batch_size)
  subscription_path = SUBSCRIBER.subscription_path(
      FLAGS.project_id, 
      FLAGS.pubsub_subscription,
  )

  response = SUBSCRIBER.pull(subscription_path, max_messages=batch_size)
  row_batch = response.received_messages

  return row_batch
