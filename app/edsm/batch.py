import gzip
import io
import json
import re
import time
import urllib.request
import urllib.error

from commonlib.google.dataflow import beam
from commonlib.google import bigquery
from commonlib.google import gcs

import apache_beam
from absl import app, flags, logging

# Global vars
DATASET = 'edsm'
URLS = {
    # 'bodies': 'https://www.edsm.net/dump/bodies7days.json.gz',
    'population': 'https://www.edsm.net/dump/systemsPopulated.json.gz',
    'powerplay': 'https://www.edsm.net/dump/powerPlay.json.gz',
    # 'stations': 'https://www.edsm.net/dump/stations.json.gz',
    # 'systems': 'https://www.edsm.net/dump/systemsWithCoordinates.json.gz',
}
FILE_TYPES = list(URLS.keys())
FILE_TYPES_META = FILE_TYPES.copy()
FILE_TYPES_META.append('all')

# Define flags
FLAGS = flags.FLAGS
flags.DEFINE_boolean('cleanup_files', False, 'Cleanup GCS files.')
flags.DEFINE_enum('file_type', None, FILE_TYPES_META,
                  'EDSM file(s) to process.')
flags.mark_flag_as_required('file_type')


class FormatEdsmJson(apache_beam.DoFn):
    def __init__(self, file_type):
        self.file_type = file_type
        self.input_dict = {}
        self.json_re_pattern = re.compile(r'(\{.*\})')
        self.json_re_search = self.json_re_pattern.search
        self.output_dict = {}
        self.schema_mappings = {
            'bodies': {},
            'population': {
                'planet_id': 'id64',
                'security': 'security',
                'allegiance': 'allegiance',
                'economy': 'economy',
                'government': 'government',
                'population': 'population',
                'state': 'state',
                'timestamp_fields': {
                    'updated': 'date',
                },
            },
            'powerplay': {
                'system_id': 'id64',
                'power.name': 'power',
                'power.state': 'powerState',
                'allegiance': 'allegiance',
                'government': 'government',
                'state': 'state',
                'timestamp_fields': {
                    'updated': 'date',
                },
            },
            'stations': {
                'id': 'id',
                'system_id': 'systemId64',
                'name': 'name',
                'metadata.type': 'type',
                'services.market': 'haveMarket',
                'services.shipyard': 'haveShipyard',
                'services.outfitting': 'haveOutfitting',
                'metadata.distance': 'distanceToArrival',
                'metadata.allegiance': 'allegiance',
                'metadata.government': 'government',
                'economy.id': 'marketId',
                'economy.type': 'economy',
                'economy.sub_type': 'secondEconomy',
                'parent.id': 'body.id',
                'parent.name': 'body.name',
                'parent.latitude': 'body.latitude',
                'parent.longitude': 'body.longitude',
                'metadata.controlling_faction': 'controllingFaction.id',
                'timestamp_fields': {
                    'updated': 'updateTime.information',
                },
            },
            'systems': {
                'id': 'id64',
                'name': 'name',
                'coordinates.x': 'coords.x',
                'coordinates.y': 'coords.y',
                'coordinates.z': 'coords.z',
                'timestamp_fields': {
                    'timestamp': 'date',
                },
            },
        }

    def _rdictget(self, input_dict: dict, path: str):
        import functools
        return functools.reduce(dict.get, path.split('.'), input_dict)

    def _rdictset(self, input_dict, path, value):
        keys = path.split('.')
        for key in keys[:-1]:
            input_dict = input_dict.setdefault(key, {})
        input_dict[keys[-1]] = value

    def _map_dict_fields(self, field_mappings):
        from datetime import datetime
        for k, v in field_mappings.items():
            if k == 'timestamp_fields':
                for ts_k, ts_v in v.items():
                    int_ts = int(
                        datetime.strptime(
                            self._rdictget(self.input_dict, ts_v),
                            '%Y-%m-%d %H:%M:%S').timestamp())
                    self._rdictset(self.output_dict, ts_k, int_ts)
            else:
                self._rdictset(self.output_dict, k,
                               self._rdictget(self.input_dict, v))

    def _extract_json(self, raw_input: str):
        json_re_match = self.json_re_search(raw_input)
        if json_re_match:
            json_string = json_re_match.group(1)
            return json_string

    def process(self, element):
        edsm_json = self._extract_json(element)
        try:
            self.input_dict = json.loads(edsm_json)
            self._map_dict_fields(self.schema_mappings.get(self.file_type))
            yield json.dumps(self.output_dict)
        except Exception as e:
            logging.info(e)
            logging.warning('Unable to parse JSON line: %s', element)


def bigquery_loader(file_type: str, ndjson_file):
    return bigquery.load_table_from_gcs(gcs.get_gcs_uri(ndjson_file.name),
                                        DATASET, file_type, 'ndjson')


def execute_beam_pipeline(file_type: str, gcs_path: str):
    with apache_beam.Pipeline(
            options=beam.get_default_pipeline_options()) as p:
        pipeline_output = (p
                           | apache_beam.io.ReadFromText(gcs_path)
                           | apache_beam.ParDo(FormatEdsmJson(file_type))
                           | apache_beam.io.WriteToText(
                               'gs://%s/%s/%s_ndjson' %
                               (FLAGS.gcs_bucket, DATASET, file_type)))


def gcs_fetch(file_type: str, url: str):
    # Working GCS download
    # logging.info('Downloading and decompressing %s...', url)
    # file_blob = gcs.get_blob('%s/%s.gz' % (DATASET, file_type))
    # gz_file_data = io.BytesIO(file_blob.download_as_bytes())
    # decompressed_file = gzip.open(gz_file_data)

    # gcs_path = '%s/%s.json' % (DATASET, file_type)
    # logging.info('Uploading gs://%s...', gcs_path)
    # return gcs.upload_file(decompressed_file, gcs_path)

    # Working EDSM download
    try:
        logging.info('Downloading and decompressing %s...', url)
        with urllib.request.urlopen(url) as http_file_response:
            gz_file_data = io.BytesIO(http_file_response.read())
            logging.info('Uploading %s.json to GCS...', file_type)
            gcs_path = '%s/%s.json' % (DATASET, file_type)
            return gcs.upload_file(gzip.open(gz_file_data), gcs_path)

    except (urllib.error.URLError, urllib.error.HTTPError) as exception:
        logging.error('Error fetching %s file: %s', url, exception)
        return


def main(argv):
    del argv  # Unused.

    debug_start = time.time()
    gcs_files = []

    def edsm_to_bigquery(file_type: str):
        logging.info('Fetching %s from EDSM...', file_type)
        edsm_file_blob = gcs_fetch(file_type, URLS[file_type])
        gcs_files.append(edsm_file_blob)

        logging.info('Generating NDJSON file via DataFlow %s...',
                     FLAGS.beam_runner)
        execute_beam_pipeline(file_type, gcs.get_gcs_uri(edsm_file_blob.name))
        ndjson_shards = gcs.get_blobs('%s/%s_ndjson' % (DATASET, file_type))
        gcs_files.extend(ndjson_shards)
        edsm_ndjson_blob = gcs.combine_files(
            ndjson_shards, '%s/%s.ndjson' % (DATASET, file_type))
        gcs_files.append(edsm_ndjson_blob)

        logging.info('Generating BigQuery table at %s.%s', DATASET, file_type)
        return bigquery_loader(file_type, edsm_ndjson_blob)

    if FLAGS.file_type == 'all':
        logging.info('Fetching all EDSM files...')
        edsm_bq_tables = [edsm_to_bigquery(x) for x in URLS.keys()]
    else:
        edsm_bq_table = edsm_to_bigquery(FLAGS.file_type)
        print(edsm_bq_table.result())

    if FLAGS.cleanup_files:
        logging.info('Cleaning up %s GCS file(s)...', len(gcs_files))
        [x.delete() for x in gcs_files]

    debug_end = time.time()
    debug_duration = debug_end - debug_start
    logging.info('Pipeline completed in: %s', debug_duration)


if __name__ == '__main__':
    app.run(main)
