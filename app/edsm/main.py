import gzip
import io
import re
import tempfile

from lib import bigquery
from lib import gcs
from lib import utils
from multiprocessing import Pool

from absl import app, flags, logging

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


def fetch_edsm_file(file_type: str, url: str):
  gcs_path = '%s/%s.gz' % (DATASET, file_type)
  gcs_uri = gcs.get_gcs_uri(gcs_path)
  logging.info('Downloading %s as %s', url, gcs_uri)
  gcs_blob = gcs.fetch_url(gcs_path, url)

  return gcs_blob


def generate_ndjson(file_type: str, gcs_blob):
  gcs_path = '%s/%s.ndjson' % (DATASET, file_type)
  gcs_uri = gcs.get_gcs_uri(gcs_path)
  logging.info('Generating NDJSON file at %s...', gcs_uri)
  gcs_file = io.BytesIO(gcs_blob.download_as_string())
  ndjson_file = tempfile.TemporaryFile()
  with gzip.GzipFile(fileobj=gcs_file, mode='rb') as file:
    for line in file:
      try:
        json_re_match = re.search(r'(\{.*\})', line.decode())
        if json_re_match:
          json_string = json_re_match.group(1)
          edsm_object = utils.edsmObject(file_type, json_string)
          formatted_json_string = edsm_object.format_json()
          ndjson_file.write(formatted_json_string.encode())
          ndjson_file.write(b'\n')
      except ValueError:
        logging.error('Failed to process JSON string: %s', line)
      except AttributeError:
        logging.error('Failed to process JSON string: %s', line)

  ndjson_file.seek(0)
  ndjson_gcs_file = gcs.upload_file(ndjson_file, gcs_path)
  ndjson_file.close()

  return ndjson_gcs_file


def main(argv):
  del argv
  gcs_files = []

  if FLAGS.file_type == 'all':
    logging.info('Processing all EDSM files...')
    with Pool(len(FILE_TYPES)) as pool:
      edsm_files = pool.starmap(fetch_edsm_file, URLS.items())
      gcs_files.append(edsm_files)
  else:
    #edsm_file_blob = fetch_edsm_file(FLAGS.file_type, URLS[FLAGS.file_type])
    edsm_file_blob = gcs.get_blob('%s/%s.gz' % (DATASET, FLAGS.file_type))
    #gcs_files.append(edsm_file_blob)
    print(FLAGS.file_type)
    ndjson_file_blob = generate_ndjson(FLAGS.file_type, edsm_file_blob)
    gcs_files.append(ndjson_file_blob)

    bigquery_table = bigquery.load_table_from_ndjson(
        gcs.get_gcs_uri(ndjson_file_blob.name),
        DATASET,
        FLAGS.file_type
    )
    print(type(bigquery_table))
    print(dir(bigquery_table))
    # logging.info(
    #     'Successfully created table %s.%s.%s',
    #     bigquery_table.project,
    #     bigquery_table.dataset_id,
    #     bigquery_table.table_id,
    # )

  if FLAGS.cleanup_files:
    for file in gcs_files:
      logging.info('Cleaning up file %s...', gcs.get_gcs_uri(file.name))
      file.delete()


if __name__ == '__main__':
  app.run(main)
