from absl import flags
from absl import logging
from google.cloud import pubsub_v1

# Define flags
FLAGS = flags.FLAGS
flags.DEFINE_string('project', None, 'Google Cloud project ID.')
flags.DEFINE_string('topic', None, 'Pub/Sub topic ID.')
flags.mark_flag_as_required('project')
flags.mark_flag_as_required('topic')

# Global vars
pubsub_client = pubsub_v1.PublisherClient()


def send_bigquery_row(row_json, table):
  logging.info('Sending message')
  topic_path = pubsub_client.topic_path(FLAGS.project, FLAGS.topic)
  response_future = pubsub_client.publish(
      topic_path,
      data=row_json.encode("utf-8"),
      table=table,
  )

  return response_future
