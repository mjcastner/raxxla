import functools
import gzip
import io
import json
import itertools
import multiprocessing
import tempfile
import time
from concurrent import futures

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
PROCESS_POOL = multiprocessing.Pool(processes=NUM_CPUS)
DOWNLOAD_POOL = futures.ThreadPoolExecutor(max_workers=len(URLS))
BATCH_SIZE = 536870912  # 0.5 GB

# Define flags
FLAGS = flags.FLAGS
flags.DEFINE_boolean('cleanup_files', False, 'Cleanup GCS files.')
flags.DEFINE_enum('file_type', None, FILE_TYPES_META,
                  'EDSM file(s) to process.')
flags.mark_flag_as_required('file_type')


def bigquery_loader(file_type: str, ndjson_file):
    return bigquery.load_table_from_gcs(gcs.get_gcs_uri(ndjson_file.name),
                                        DATASET, file_type, 'ndjson')


def gcs_fetch(file_type: str, url: str):
    gcs_path = '%s/%s.gz' % (DATASET, file_type)
    gcs_uri = gcs.get_gcs_uri(gcs_path)
    logging.info('Downloading %s as %s', url, gcs_uri)
    gcs_blob = gcs.fetch_url(gcs_path, url)

    return gcs_blob


@profile
def generate_ndjson_file(file_type: str, gcs_blob):
    gz_file = tempfile.TemporaryFile()
    gcs_blob.download_to_file(gz_file)
    gz_file.seek(0)

    decompressed_file = gzip.open(gz_file, mode='rt')
    ndjson_file = tempfile.TemporaryFile()

    line_batch = decompressed_file.readlines(BATCH_SIZE)
    total_batch_size = 0
    with futures.ProcessPoolExecutor(max_workers=NUM_CPUS) as executor:
        while len(line_batch) > 0:
            json_data = PROCESS_POOL.imap(utils.extract_json, line_batch,
                                          NUM_CPUS)

            proto_futures = [
                executor.submit(utils.edsm_json_to_proto, file_type, x)
                for x in json_data if xq
            ]

            futures.wait(proto_futures)
            json_lines = [x.result().replace('\n', '') for x in proto_futures]
            ndjson_lines = [x.encode() + b'\n' for x in json_lines]
            ndjson_file.writelines(ndjson_lines)
            proto_futures.clear()

            total_batch_size += len(line_batch)
            logging.info('Processed %s lines...', total_batch_size)
            line_batch = decompressed_file.readlines(BATCH_SIZE)

    ndjson_file.seek(0)
    gcs_path = '%s/%s.ndjson' % (DATASET, file_type)
    gcs_uri = gcs.get_gcs_uri(gcs_path)
    logging.info('Generating NDJSON file at %s...', gcs_uri)
    ndjson_gcs_file = gcs.upload_file(ndjson_file, gcs_path)
    decompressed_file.close()
    ndjson_file.close()

    return ndjson_gcs_file


def main(argv):
    debug_start = time.time()
    del argv
    gcs_files = []

    if FLAGS.file_type == 'all':
        logging.info('Fetching all EDSM files...')
        edsm_file_blobs = list(
            DOWNLOAD_POOL.map(gcs_fetch, FILE_TYPES, URLS.values()))
        gcs_files.extend(edsm_file_blobs)

        logging.info('Generating NDJSON files...')
        ndjson_files = list(map(generate_ndjson_file, FILE_TYPES, gcs_files))
        gcs_files.extend(ndjson_files)

        logging.info('Generating BigQuery tables...')
        bigquery_tables = list(map(bigquery_loader, FILE_TYPES, ndjson_files))

    else:
        logging.info('Fetching %s from EDSM...', FLAGS.file_type)
        gcs_path = '%s/%s.gz' % (DATASET, FLAGS.file_type)
        edsm_file_blob = gcs.get_blob(gcs_path)
        # edsm_file_blob = gcs_fetch(FLAGS.file_type, URLS[FLAGS.file_type])
        gcs_files.append(edsm_file_blob)

        logging.info('Reformatting EDSM data...')
        ndjson_file = generate_ndjson_file(FLAGS.file_type, edsm_file_blob)
        gcs_files.append(ndjson_file)

        logging.info('Generating BigQuery table at %s.%s', DATASET,
                     FLAGS.file_type)
        bigquery_table = bigquery_loader(FLAGS.file_type, ndjson_file)

    if FLAGS.cleanup_files:
        gcs_deleted_files = [x.delete() for x in gcs_files]

    PROCESS_POOL.close()
    DOWNLOAD_POOL.shutdown()
    debug_end = time.time()
    debug_duration = debug_end - debug_start
    logging.info('NDJSON creation completed in: %s', debug_duration)


if __name__ == '__main__':
    app.run(main)
