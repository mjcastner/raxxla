import gzip
import io
import json
import multiprocessing
import tempfile
from concurrent.futures import ThreadPoolExecutor

import utils
from commonlib.google import bigquery
from commonlib.google import gcs

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
NUM_CPUS = multiprocessing.cpu_count()
PROCESS_POOL = multiprocessing.Pool(processes=multiprocessing.cpu_count())
DOWNLOAD_POOL = ThreadPoolExecutor(max_workers=len(URLS))

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


def format_json(file_type, input_queue, output_queue):
  while True:
    json_data = utils.extract_json(input_queue.get())
    if json_data:
      edsm_proto_json = utils.edsm_json_to_proto(file_type, json_data).replace('\n','')
      output_queue.put(edsm_proto_json)


def write_ndjson(input_queue, output_file):
  while True:
    json_line = input_queue.get()
    output_file.write(json_line.encode() + b'\n')


def generate_ndjson_file(file_type: str, gcs_blob):
  gcs_file = io.BytesIO(gcs_blob.download_as_string())
  decompressed_file = gzip.open(gcs_file, mode='rt')
  ndjson_file = tempfile.TemporaryFile()
  transform_queue = multiprocessing.Queue(maxsize=NUM_CPUS)
  write_queue = multiprocessing.Queue(maxsize=1)

  transform_process = multiprocessing.Process(target=format_json, args=(file_type, transform_queue, write_queue))
  transform_process.daemon = True
  transform_process.start()

  write_process = multiprocessing.Process(target=write_ndjson, args=(write_queue, ndjson_file))
  write_process.daemon = True
  write_process.start()

  logging.info('Transforming raw JSON into formatted NDJSON...')
  for line in decompressed_file:
    transform_queue.put(line)

  transform_queue.close()
  write_queue.close()
  transform_process.join(3)
  write_process.join(3)

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

  # 60 seconds
  if FLAGS.file_type == 'all':
    logging.info('Fetching all EDSM files...')
    edsm_file_blobs = list(DOWNLOAD_POOL.map(gcs_fetch, FILE_TYPES, URLS.values()))
    gcs_files.extend(edsm_file_blobs)

    logging.info('Generating NDJSON files...')
    ndjson_files = list(map(generate_ndjson_file, FILE_TYPES, gcs_files))
    gcs_files.extend(ndjson_files)

    logging.info('Generating BigQuery tables...')
    bigquery_tables = list(map(bigquery_loader, FILE_TYPES, ndjson_files))

  else:
    logging.info('Fetching %s from EDSM...', FLAGS.file_type)
    edsm_file_blob = gcs.get_blob('%s/%s.gz' % (DATASET, FLAGS.file_type))
    # edsm_file_blob = gcs_fetch(FLAGS.file_type, URLS[FLAGS.file_type])
    gcs_files.append(edsm_file_blob)

    ndjson_file = generate_ndjson_file(FLAGS.file_type, edsm_file_blob)
    gcs_files.append(ndjson_file)

    logging.info('Generating BigQuery table at %s.%s', DATASET, FLAGS.file_type)
    bigquery_table = bigquery_loader(FLAGS.file_type, ndjson_file)
  
  if FLAGS.cleanup_files:
    gcs_deleted_files = [x.delete() for x in gcs_files]

  logging.info('Pipeline complete!')
  PROCESS_POOL.close()
  DOWNLOAD_POOL.shutdown()


if __name__ == '__main__':
  app.run(main)
