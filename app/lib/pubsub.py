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
pubsub_client = pubsub.PublisherClient(
    batch_settings=types.BatchSettings(max_messages=500),
)


def send_bigquery_row(row_json, table):
  logging.info('Sending message')
  topic_path = pubsub_client.topic_path(FLAGS.project_id, FLAGS.pubsub_topic)
  response_future = pubsub_client.publish(
      topic_path,
      data=row_json.encode("utf-8"),
      table=table,
  )

  return response_future
