import gzip
import io
import json
import tempfile

import utils
from commonlib.google import bigquery
from commonlib.google import gcs
from commonlib.google import pubsub

from absl import app, flags, logging
from google.protobuf.json_format import MessageToJson

# Global vars
DATASET = 'edsm'
URLS = {
    'bodies': 'https://www.edsm.net/dump/bodies7days.json.gz',
    'population': 'https://www.edsm.net/dump/systemsPopulated.json.gz',
    'powerplay': 'https://www.edsm.net/dump/powerPlay.json.gz',
    'stations': 'https://www.edsm.net/dump/stations.json.gz',
    'systems': 'https://www.edsm.net/dump/systemsWithCoordinates.json.gz',
}
FILE_TYPES = list(URLS.keys())
FILE_TYPES_META = FILE_TYPES.copy()
FILE_TYPES_META.append('all')

# Define flags
FLAGS = flags.FLAGS
flags.DEFINE_boolean('cleanup_files', False, 'Cleanup GCS files.')
flags.DEFINE_enum(
    'file_type',
    None,
    FILE_TYPES_META,
    'EDSM file(s) to process.'
)
flags.mark_flag_as_required('file_type')


def bigquery_loader(file_type: str, ndjson_file):
  return bigquery.load_table_from_gcs(
      gcs.get_gcs_uri(ndjson_file.name),
      DATASET,
      file_type,
      'ndjson'
  )


def gcs_fetch(file_type: str, url: str):
  gcs_path = '%s/%s.gz' % (DATASET, file_type)
  gcs_uri = gcs.get_gcs_uri(gcs_path)
  logging.info('Downloading %s as %s', url, gcs_uri)
  gcs_blob = gcs.fetch_url(gcs_path, url)

  return gcs_blob


def generate_ndjson_file(file_type: str, gcs_blob):
  gcs_file = io.BytesIO(gcs_blob.download_as_string())
  decompressed_file = gzip.open(gcs_file, mode='rt')
  ndjson_file = tempfile.TemporaryFile()

  for line in decompressed_file:
    json_data = utils.extract_json(line)
    if json_data:
      edsm_proto = utils.edsm_json_to_proto(file_type, json_data)
      edsm_json = MessageToJson(edsm_proto, indent=0).replace('\n','')
      ndjson_file.write(edsm_json.encode() + b'\n')

  ndjson_file.seek(0)
  gcs_path = '%s/%s.ndjson' % (DATASET, file_type)
  gcs_uri = gcs.get_gcs_uri(gcs_path)
  logging.info('Generating NDJSON file at %s...', gcs_uri)
  ndjson_gcs_file = gcs.upload_file(ndjson_file, gcs_path)
  ndjson_file.close()

  return ndjson_gcs_file


def main(argv):
  del argv
  gcs_files = []

  if FLAGS.file_type == 'all':
    logging.info('Fetching all EDSM file(s)...')
    edsm_file_blobs = list(map(gcs_fetch, FILE_TYPES, URLS.values()))
    gcs_files.extend(edsm_file_blobs)
  else:
    logging.info('Fetching %s from EDSM...', FLAGS.file_type)
    edsm_file_blob = gcs.get_blob('%s/%s.gz' % (DATASET, FLAGS.file_type))
    # edsm_file_blob = gcs_fetch(FLAGS.file_type, URLS[FLAGS.file_type])
    gcs_files.append(edsm_file_blob)

    logging.info('Generating NDJSON file(s)...')
    ndjson_file = generate_ndjson_file(FLAGS.file_type, edsm_file_blob)
    gcs_files.append(ndjson_file)

    logging.info('Generating BigQuery table at %s.%s', DATASET, FLAGS.file_type)
    bigquery_table = bigquery_loader(FLAGS.file_type, ndjson_file)
    print(bigquery_table.result())


if __name__ == '__main__':
  app.run(main)
