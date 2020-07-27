import io
import urllib.request
import urllib.error

from absl import flags, logging
from google.cloud import storage

# Global vars
GCS_CLIENT = storage.Client()

# Define flags
FLAGS = flags.FLAGS
flags.DEFINE_string('gcs_bucket', None, 'Google Cloud Storage bucket ID.')
flags.mark_flag_as_required('gcs_bucket')


def get_gcs_uri(destination_path: str):
  gcs_uri = 'gs://%s/%s' % (FLAGS.gcs_bucket, destination_path)
  return gcs_uri


def upload_file(file_obj: io.BufferedRandom, destination_path: str):
  gcs_bucket = GCS_CLIENT.bucket(FLAGS.gcs_bucket)
  gcs_blob = gcs_bucket.blob(destination_path)
  gcs_blob.upload_from_file(file_obj)
  logging.info('Upload completed successfully.')

  return gcs_bucket.get_blob(destination_path)


def fetch_url(destination_path: str, url: str):
  gcs_bucket = GCS_CLIENT.bucket(FLAGS.gcs_bucket)
  gcs_blob = gcs_bucket.blob(destination_path)

  try:
    with urllib.request.urlopen(url) as http_file_response:
      http_file_data = io.BytesIO(http_file_response.read())
      gcs_blob.upload_from_file(http_file_data)
      logging.info('Upload completed successfully.')
      return gcs_bucket.get_blob(destination_path)
  except (urllib.error.URLError, urllib.error.HTTPError) as exception:
    logging.error('Error fetching %s file: %s', url, exception)
    return
