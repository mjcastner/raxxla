import concurrent.futures
import gzip
import io
import json
import urllib.error
import urllib.request

import api_raxxla_pb2
import api_raxxla_pb2_grpc
import grpc
import society_pb2
from absl import app, flags, logging
from commonlib import utils
from commonlib.google.storage import gcs

# Global vars
BATCH_SIZE = 500
DATASET = 'edsm'
CONVERTERS = {
    'powerplay': 'ConvertPowerplay',
}
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

# Flags
FLAGS = flags.FLAGS
flags.DEFINE_boolean('cleanup_files', False, 'Cleanup GCS files.')
flags.DEFINE_boolean('resume', False, 'Resume batch from staged GCS files.')
flags.DEFINE_enum('file_type', None, FILE_TYPES_META,
                  'EDSM file(s) to process.')
flags.DEFINE_string('server_host', '172.17.0.2',
                    'Host for Raxxla debug gRPC server.')
flags.DEFINE_string('server_port', '50051',
                    'Port for Raxxla debug gRPC server.')
flags.DEFINE_enum('channel_type', 'insecure', ['insecure', 'secure'],
                  'gRPC channel type to use.')
flags.mark_flag_as_required('file_type')


def grpc_batch_process(stub, file_type: str, gcs_blob):
    logging.info('Converting data into protobuf format...')
    gcs_blob_stream = io.BytesIO(gcs_blob.download_as_bytes())
    gcs_file = gzip.open(gcs_blob_stream, 'rt')
    converter_method = getattr(stub, CONVERTERS[file_type])

    for line in gcs_file:
        edsm_request = api_raxxla_pb2.EdsmRequest(json=line)
        edsm_response = converter_method(edsm_request)
        print(edsm_response)

    return


def gcs_fetch(file_type: str, url: str):
    gcs_path = '%s/%s.json.gz' % (DATASET, file_type)

    if FLAGS.resume:
        return gcs.get_blob(gcs_path)
    else:
        try:
            logging.info('Downloading %s...', url)
            with urllib.request.urlopen(url) as http_file_response:
                gz_file_data = io.BytesIO(http_file_response.read())
                logging.info('Uploading %s.json.gz to GCS...', file_type)
                return gcs.upload_file(gz_file_data, gcs_path)
        except (urllib.error.URLError, urllib.error.HTTPError) as exception:
            logging.error('Error fetching %s file: %s', url, exception)
            return


def main(argv):
    logging.info('Starting pipeline...')
    if FLAGS.channel_type == 'insecure':
        channel = grpc.insecure_channel('%s:%s' %
                                        (FLAGS.server_host, FLAGS.server_port))
    else:
        channel = grpc.secure_channel(
            '%s:%s' % (FLAGS.server_host, FLAGS.server_port),
            grpc.ssl_channel_credentials())

    stub = api_raxxla_pb2_grpc.RaxxlaStub(channel)
    gcs_files = []

    if FLAGS.file_type == 'all':
        logging.info('Fetching all files...')
    else:
        logging.info('Fetching %s from EDSM...', FLAGS.file_type)
        edsm_file_blob = gcs_fetch(FLAGS.file_type,
                                   URLS[FLAGS.file_type])
        grpc_batch_status = grpc_batch_process(stub, FLAGS.file_type,
                                               edsm_file_blob)
        gcs_files.append(edsm_file_blob)

    channel.close()


if __name__ == '__main__':
    app.run(main)
