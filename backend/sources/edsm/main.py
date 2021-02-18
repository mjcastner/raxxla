import gzip
import io
import json
import re
import time
import urllib.error
import urllib.request

import apache_beam
from absl import app, flags, logging
from apache_beam.io.gcp.datastore.v1new.datastoreio import WriteToDatastore
from apache_beam.io.gcp.datastore.v1new.types import Entity, Key
from commonlib.google.dataflow import beam
from commonlib.google.datastore import datastore
from commonlib.google.storage import gcs
from google.protobuf.json_format import MessageToJson

import schema

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
flags.DEFINE_boolean('resume', False, 'Use existing GCS files.')
flags.DEFINE_enum('file_type', None, FILE_TYPES_META,
                  'EDSM file(s) to process.')
flags.mark_flag_as_required('file_type')


class CreateEdsmEntity(apache_beam.DoFn):
    def __init__(self, file_type, project_id):
        self.file_type = file_type
        self.project_id = project_id

    def process(self, element):
        try:
            # TODO(mjcastner): Change this to be a method on EdsmObject (e.g. generate_entity)
            edsm_obj = schema.EdsmObject(self.file_type, element)
            edsm_proto = edsm_obj.generate_proto()
            key_field_value = getattr(edsm_proto, edsm_obj.key_field)

            if key_field_value != 0:
                edsm_dict = json.loads(MessageToJson(edsm_proto))
                edsm_dict['protobuf'] = edsm_proto.SerializeToString()

                entity_key = Key([edsm_obj.kind, key_field_value])
                edsm_entity = Entity(entity_key,
                                     exclude_from_indexes=edsm_dict.keys())
                edsm_entity.set_properties(edsm_dict)

                yield edsm_entity
        except Exception as e:
            logging.info(e)
            logging.warning('Unable to parse JSON line: %s', element)


def execute_beam_pipeline(file_type: str, gcs_path: str):
    full_path = 'gs://raxxla/%s' % gcs_path
    pipeline_options = beam.get_default_pipeline_options()
    pipeline_options_dict = pipeline_options.get_all_options()
    project_id = pipeline_options_dict.get('project')

    with apache_beam.Pipeline(options=pipeline_options) as p:
        pcollection = (p
                       | apache_beam.io.ReadFromText(full_path)
                       | apache_beam.ParDo(
                           CreateEdsmEntity(file_type, project_id))
                       | WriteToDatastore(project_id))


def gcs_fetch(file_type: str, url: str, gcs_path: str):
    if FLAGS.resume:
        return gcs.get_blob(gcs_path)
    else:
        try:
            logging.info('Downloading and decompressing %s...', url)
            with urllib.request.urlopen(url) as http_file_response:
                gz_file_data = io.BytesIO(http_file_response.read())
                logging.info('Uploading %s.json to GCS...', file_type)
                return gcs.upload_file(gzip.open(gz_file_data), gcs_path)

        except (urllib.error.URLError, urllib.error.HTTPError) as exception:
            logging.error('Error fetching %s file: %s', url, exception)
            return


def main(argv):
    del argv  # Unused.

    debug_start = time.time()
    gcs_files = []

    if FLAGS.file_type == 'all':
        logging.info('Fetching all EDSM files...')
    else:
        logging.info('Fetching %s data from EDSM...', FLAGS.file_type)
        gcs_path = '%s/%s.json' % (DATASET, FLAGS.file_type)
        gcs_blob = gcs_fetch(FLAGS.file_type, URLS[FLAGS.file_type], gcs_path)
        gcs_files.append(gcs_blob)
        execute_beam_pipeline(FLAGS.file_type, gcs_path)

    if FLAGS.cleanup_files:
        logging.info('Cleaning up %s GCS file(s)...', len(gcs_files))
        [x.delete() for x in gcs_files]

    debug_end = time.time()
    debug_duration = debug_end - debug_start
    logging.info('Pipeline completed in: %s', debug_duration)


if __name__ == '__main__':
    app.run(main)
