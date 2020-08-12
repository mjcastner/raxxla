import gzip
import io
import multiprocessing
import re
import tempfile
import time

from lib import bigquery
from lib import gcs
from lib import utils
from pprint import pprint

from absl import app, flags, logging

# Global vars
CORE_COUNT = multiprocessing.cpu_count()
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


def fetch_edsm_file(file_type: str, url: str):
  gcs_path = '%s/%s.gz' % (DATASET, file_type)
  gcs_uri = gcs.get_gcs_uri(gcs_path)
  logging.info('Downloading %s as %s', url, gcs_uri)
  gcs_blob = gcs.fetch_url(gcs_path, url)

  return gcs_blob


def generate_ndjson_file(file_type: str, gcs_blob):
  gcs_file = io.BytesIO(gcs_blob.download_as_string())
  decompressed_file = gzip.open(gcs_file, mode='rt')
  ndjson_file = tempfile.TemporaryFile()

  # TODO(mjcastner): This needs an optimization pass
  with multiprocessing.Pool(CORE_COUNT) as pool:
    line_batch = []
    for line in decompressed_file:
      if len(line_batch) < CORE_COUNT:
        line_batch.append(line)
      else:
        raw_json_batch = pool.map(utils.extract_json, line_batch)
        json_batch = list(filter(None, raw_json_batch))
        formatted_json = pool.starmap(
            utils.format_edsm_json,
            [(x, file_type) for x in json_batch]
        )
        [ndjson_file.write(x.encode() + b'\n') for x in formatted_json]
        line_batch.clear()
        raw_json_batch.clear()
        json_batch.clear()

  ndjson_file.seek(0)
  gcs_path = '%s/%s.ndjson' % (DATASET, file_type)
  gcs_uri = gcs.get_gcs_uri(gcs_path)
  logging.info('Generating NDJSON file at %s...', gcs_uri)
  ndjson_gcs_file = gcs.upload_file(ndjson_file, gcs_path)
  ndjson_file.close()

  return ndjson_gcs_file


def main(argv):
  del argv

  debug_start = time.time()
  gcs_files = []

  logging.info('Initializing EDSM BigQuery ETL pipeline...')
  if FLAGS.file_type == 'all':
    logging.info('Processing all EDSM files...')
    with multiprocessing.Pool(len(FILE_TYPES)) as pool:
      edsm_file_blobs = pool.starmap(fetch_edsm_file, URLS.items())
      gcs_files.append(edsm_file_blobs)
  else:
    edsm_file_blob = gcs.get_blob('%s/%s.gz' % (DATASET, FLAGS.file_type))
    #edsm_file_blob = fetch_edsm_file(FLAGS.file_type, URLS[FLAGS.file_type])
    # gcs_files.append(edsm_file_blob)

    ndjson_file_blob = generate_ndjson_file(FLAGS.file_type, edsm_file_blob)
    gcs_files.append(ndjson_file_blob)

    bigquery_table = bigquery.load_table_from_ndjson(
        gcs.get_gcs_uri(ndjson_file_blob.name),
        DATASET,
        FLAGS.file_type
    )
    logging.info(
        'Successfully created table %s.%s.%s',
        bigquery_table.project,
        bigquery_table.dataset_id,
        bigquery_table.table_id,
    )

  if FLAGS.cleanup_files:
    for file in gcs_files:
      logging.info('Cleaning up file %s...', gcs.get_gcs_uri(file.name))
      file.delete()

  logging.info('Pipeline completed in %s seconds.', int(time.time()-debug_start))


if __name__ == '__main__':
  app.run(main)
