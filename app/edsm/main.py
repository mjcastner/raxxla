import gzip
import io
import re
import schema
import time

from lib import gcs
from lib import pubsub

from absl import app, flags, logging

# Define flags
FLAGS = flags.FLAGS
flags.DEFINE_enum(
    'file_type',
    None,
    schema.file_types_meta,
    'EDSM file(s) to process.'
)


def fetch_edsm_file(file_type: str, url: str):
  gcs_path = 'edsm/%s.gz' % file_type
  gcs_blob = gcs.fetch_url(gcs_path, url)
  return io.BytesIO(gcs_blob.download_as_string())


def process_edsm_file(gcs_file: io.BytesIO, bq_table: str):
  pubsub_responses = []

  logging.info('Extracting JSON rows and sending to Pub/Sub...')
  with gzip.GzipFile(fileobj=gcs_file, mode='rb') as file:
    for line in file:
      try:
        json_string = None
        json_re_match = re.search(r'(\{.*\})', line.decode())
        if json_re_match:
          json_string = json_re_match.group(1)
          response = pubsub.send_bigquery_row(json_string, bq_table)
          pubsub_responses.append(response)
      except ValueError:
        logging.error('Failed to process JSON string: %s', json_string)
      except AttributeError:
        logging.error('Failed to process JSON string: %s', json_string)

  return pubsub_responses


def main(argv):
  del argv

  if FLAGS.file_type == 'all':
    logging.info('Processing all EDSM files...')
    gcs_files = [fetch_edsm_file(x, schema.urls[x]) for x in schema.file_types]
  else:
    gcs_file = fetch_edsm_file(FLAGS.file_type, schema.urls[FLAGS.file_type])
    bq_table = '%s.edsm.%s' % (FLAGS.project_id, FLAGS.file_type)
    pubsub_responses = process_edsm_file(gcs_file, bq_table)

  logging.info('%s messages in flight...', len(pubsub_responses))
  while pubsub_responses:
    errors = [x for x in pubsub_responses if x.exception()]
    pubsub_responses = [x for x in pubsub_responses if not x.done()]
    time.sleep(5)

  logging.info('Message processing complete.')
  if errors:
    [logging.error(x) for x in errors]

if __name__ == '__main__':
  app.run(main)
