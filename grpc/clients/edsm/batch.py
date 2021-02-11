import concurrent.futures
import gzip
import io
import json
import urllib.error
import urllib.request

import api_raxxla_pb2
import api_raxxla_pb2_grpc
import grpc
from absl import app, flags, logging
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
    edsm_protos = [stub.ConvertPowerplayJson(api_raxxla_pb2.EdsmRequest(json=line)) for line in gcs_file]
    filtered_edsm_protos = [x for x in edsm_protos if x.system_id]

    logging.info('Inserting batch...')
    for edsm_proto in filtered_edsm_protos:
        stub.SetPowerplay(
            api_raxxla_pb2.PowerplayRequest(
                id=edsm_proto.system_id,
                powerplay=edsm_proto,
            )
        )

    # proto_batch = []
    # batch_responses = []
    # for edsm_proto in concurrent.futures.as_completed(edsm_protos):
    #     if len(proto_batch) <= BATCH_SIZE and edsm_proto.system_id:
    #         proto_batch.append(edsm_proto)
    #     else:
    #         batch_responses.append(stub.BatchSetPowerplay.future(iter(proto_batch)))

    # for batch_response in concurrent.futures.as_completed(batch_responses):
    #     logging.info('Batch completed with status: %s', batch_response.code)
    # json_batch = []
    # for line in gcs_file:
    #     if len(json_batch) <= BATCH_SIZE:
    #         json_batch.append(line)
    #     else:
    #         edsm_protos = [stub.ConvertPowerplayJson.future(api_raxxla_pb2.EdsmRequest(json=line)) for line in json_batch]
    #         filtered_edsm_protos = [x for x in edsm_protos if x.system_id]
    #         response = stub.BatchSetPowerplay(iter(filtered_edsm_protos))
    #         logging.info('Batch of %s items processed with %s', len(json_batch), response.code)
    #         json_batch.clear()

    # filtered_edsm_protos = [x for x in edsm_protos if x.system_id]
    # response = stub.BatchSetPowerplay(iter(filtered_edsm_protos))
    # logging.info('Batch of %s items processed with %s', len(json_batch), response.code)

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
        edsm_file_blob = gcs_fetch(FLAGS.file_type, URLS[FLAGS.file_type])
        grpc_batch_status = grpc_batch_process(stub, FLAGS.file_type,
                                               edsm_file_blob)
        gcs_files.append(edsm_file_blob)

    # channel.close()

if __name__ == '__main__':
    app.run(main)
