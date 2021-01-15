import functools
import json
import re
import tempfile
import time

from concurrent import futures
from datetime import datetime
from pprint import pprint

from commonlib.google.dataflow import beam
from commonlib.google import bigquery
from commonlib.google import gcs

import apache_beam
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
DOWNLOAD_POOL = futures.ThreadPoolExecutor(max_workers=len(URLS))
JSON_RE_PATTERN = re.compile(r'(\{.*\})')
JSON_RE_SEARCH = JSON_RE_PATTERN.search

# Define flags
FLAGS = flags.FLAGS
flags.DEFINE_boolean('cleanup_files', False, 'Cleanup GCS files.')
flags.DEFINE_enum('file_type', None, FILE_TYPES_META,
                  'EDSM file(s) to process.')
flags.mark_flag_as_required('file_type')

# Schema mappings
POPULATION_MAPPING = {
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
}

POWERPLAY_MAPPING = {
    'system_id': 'id64',
    'power.name': 'power',
    'power.state': 'powerState',
    'allegiance': 'allegiance',
    'government': 'government',
    'state': 'state',
    'timestamp_fields': {
        'updated': 'date',
    },
}

STATION_MAPPING = {
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
}

SYSTEM_MAPPING = {
    'id': 'id64',
    'name': 'name',
    'coordinates.x': 'coords.x',
    'coordinates.y': 'coords.y',
    'coordinates.z': 'coords.z',
    'timestamp_fields': {
        'timestamp': 'date',
    },
}


class FormatEdsmJson(apache_beam.DoFn):
    def __init__(self, file_type):
        self.file_type = file_type
        self.input_dict = {}
        self.json_re_pattern = re.compile(r'(\{.*\})')
        self.json_re_search = self.json_re_pattern.search
        self.output_dict = {}
        self.schema_mappings = {
            'bodies': {},
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
        return functools.reduce(dict.get, path.split('.'), input_dict)

    def _rdictset(self, input_dict, path, value):
        keys = path.split('.')
        for key in keys[:-1]:
            input_dict = input_dict.setdefault(key, {})
        input_dict[keys[-1]] = value

    def _map_dict_fields(self, field_mappings):
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


def generate_ndjson_file(file_type: str, gcs_path: str):
    with apache_beam.Pipeline(
            options=beam.get_default_pipeline_options()) as p:
        pipeline_output = (p
                           | apache_beam.io.ReadFromText(gcs_path)
                           | apache_beam.ParDo(FormatEdsmJson(file_type))
                           | apache_beam.Reshuffle()
                           | apache_beam.io.WriteToText(
                               'gs://%s/%s/%s_ndjson' %
                               (FLAGS.gcs_bucket, DATASET, file_type)))

    ndjson_shards = gcs.get_blobs('%s/%s_ndjson' % (DATASET, file_type))
    return gcs.combine_files(ndjson_shards,
                             '%s/%s.ndjson' % (DATASET, file_type))


def gcs_fetch(file_type: str, url: str):
    gcs_path = '%s/%s.gz' % (DATASET, file_type)
    gcs_uri = gcs.get_gcs_uri(gcs_path)
    logging.info('Downloading %s as %s', url, gcs_uri)
    gcs_blob = gcs.fetch_url(gcs_path, url)

    return gcs_blob


def main(argv):
    del argv  # Unused.

    debug_start = time.time()
    gcs_files = []

    if FLAGS.file_type == 'all':
        logging.info('Fetching all EDSM files...')
        edsm_file_blobs = list(
            DOWNLOAD_POOL.map(gcs_fetch, FILE_TYPES, URLS.values()))
        gcs_files.extend(edsm_file_blobs)

        logging.info('Generating BigQuery tables...')
        bigquery_tables = list(map(bigquery_loader, FILE_TYPES, ndjson_files))

    else:
        logging.info('Fetching %s from EDSM...', FLAGS.file_type)
        edsm_file_blob = gcs.get_blob('%s/%s.gz' % (DATASET, FLAGS.file_type))
        # edsm_file_blob = gcs_fetch(FLAGS.file_type, URLS[FLAGS.file_type])
        gcs_files.append(edsm_file_blob)

        logging.info('Generating NDJSON file via DataFlow %s...',
                     FLAGS.beam_runner)
        edsm_ndjson_blob = generate_ndjson_file(
            FLAGS.file_type, gcs.get_gcs_uri(edsm_file_blob.name))

        logging.info('Generating BigQuery table at %s.%s', DATASET,
                     FLAGS.file_type)
        bigquery_table = bigquery_loader(FLAGS.file_type, edsm_ndjson_blob)

    if FLAGS.cleanup_files:
        gcs_deleted_files = [x.delete() for x in gcs_files]

    DOWNLOAD_POOL.shutdown()
    debug_end = time.time()
    debug_duration = debug_end - debug_start
    logging.info('Pipeline completed in: %s', debug_duration)


if __name__ == '__main__':
    app.run(main)
