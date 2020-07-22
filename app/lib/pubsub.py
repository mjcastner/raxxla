from absl import flags
from absl import logging
from google.cloud import pubsub
from google.cloud.pubsub import types

# Define flags
FLAGS = flags.FLAGS
flags.DEFINE_string('project_id', None, 'Google Cloud project ID.')
flags.DEFINE_string('pubsub_topic', None, 'Pub/Sub topic ID.')
flags.mark_flag_as_required('project_id')
flags.mark_flag_as_required('pubsub_topic')

# Global vars
PUBLISHER = pubsub.PublisherClient(
    batch_settings=types.BatchSettings(max_messages=500),
)
SUBSCRIBER = pubsub.SubscriberClient()


def send_bigquery_row(row_json: str, table: str):
  logging.info('Sending message')
  topic_path = PUBLISHER.topic_path(FLAGS.project_id, FLAGS.pubsub_topic)
  response_future = PUBLISHER.publish(
      topic_path,
      data=row_json.encode("utf-8"),
      table=table,
  )

  return response_future


def fetch_bigquery_batch(batch_size: int):
  logging.info('Fetching message batch (%s messages)...', batch_size)
  print(dir(SUBSCRIBER))
