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
BATCH_SIZE = 100
DATASET = 'edsm'
URLS = {
    # 'bodies': 'https://www.edsm.net/dump/bodies7days.json.gz',
    # 'population': 'https://www.edsm.net/dump/systemsPopulated.json.gz',
    'powerplay': 'https://www.edsm.net/dump/powerPlay.json.gz',
    # 'stations': 'https://www.edsm.net/dump/stations.json.gz',
    # 'systems': 'https://www.edsm.net/dump/systemsWithCoordinates.json.gz',
}
TYPES = {
    'powerplay': {
        'url': 'https://www.edsm.net/dump/powerPlay.json.gz',
        'proto': society_pb2.Powerplay,
        'endpoint': 'BatchSetPowerplay',
    }
}
FILE_TYPES = list(URLS.keys())
FILE_TYPES_META = FILE_TYPES.copy()
FILE_TYPES_META.append('all')

# Flags
FLAGS = flags.FLAGS
flags.DEFINE_boolean('cleanup_files', False, 'Cleanup GCS files.')
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
    convert_responses = [
        stub.ConvertEdsm(api_raxxla_pb2.EdsmRequest(type=file_type, json=line))
        for line in gcs_file
    ]
    converted_proto_bytes = [
        x.protobuf for x in convert_responses if x.code == 0
    ]

    proto_batch = []
    batch_futures = []
    for proto_bytes in converted_proto_bytes:
        if len(proto_batch) < BATCH_SIZE:
            proto = TYPES[FLAGS.file_type].get('proto')()
            proto.ParseFromString(proto_bytes)
            proto_batch.append(proto)
        else:
            logging.info('Inserting batch of size %s...', len(proto_batch))
            batch_request = api_raxxla_pb2.BatchInsertRequest()
            batch_field = getattr(batch_request, file_type)
            batch_field.extend(proto_batch)
            batch_endpoint = getattr(stub, TYPES[FLAGS.file_type].get('endpoint'))
            batch_futures.append(batch_endpoint.future(batch_request))
            proto_batch.clear()

    logging.info('Inserting batch of size %s...', len(proto_batch))
    batch_request = api_raxxla_pb2.BatchInsertRequest()
    batch_field = getattr(batch_request, file_type)
    batch_field.extend(proto_batch)
    batch_endpoint = getattr(stub, TYPES[FLAGS.file_type].get('endpoint'))
    batch_futures.append(batch_endpoint.future(batch_request))
    proto_batch.clear()

    logging.info('Waiting for batches to finish...')
    batch_results = [x.result() for x in batch_futures]
    logging.info('Complete!')

    return


def gcs_fetch(file_type: str, url: str):
    try:
        logging.info('Downloading and decompressing %s...', url)
        with urllib.request.urlopen(url) as http_file_response:
            gz_file_data = io.BytesIO(http_file_response.read())
            logging.info('Uploading %s.json.gz to GCS...', file_type)
            gcs_path = '%s/%s.json.gz' % (DATASET, file_type)
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
        edsm_file_blob = gcs_fetch(FLAGS.file_type, TYPES[FLAGS.file_type].get('url'))
        grpc_batch_status = grpc_batch_process(stub, FLAGS.file_type,
                                               edsm_file_blob)
        gcs_files.append(edsm_file_blob)

    # channel.close()


if __name__ == '__main__':
    app.run(main)
