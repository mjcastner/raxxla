import functools
import json
import re
from collections import defaultdict
from datetime import datetime
from pprint import pprint

from commonlib.google.dataflow import beam

import apache_beam
from absl import app, flags, logging

# Global vars
JSON_RE_PATTERN = re.compile(r'(\{.*\})')
JSON_RE_SEARCH = JSON_RE_PATTERN.search

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

# Define flags
FLAGS = flags.FLAGS


class FormatEdsmJson(apache_beam.DoFn):
    def __init__(self, file_type):
        self.file_type = file_type
        self.input_dict = {}
        self.output_dict = {}
        self.schema_mappings = {
            'bodies': {},
            'powerplay': {},
            'systems': {},
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
        json_re_match = JSON_RE_SEARCH(raw_input)
        if json_re_match:
            json_string = json_re_match.group(1)
            return json_string

    def process(self, element):
        edsm_json = self._extract_json(element)
        try:
            self.input_dict = json.loads(edsm_json)
            print('Input:')
            pprint(self.input_dict)

            if self.file_type == 'powerplay':
                self._map_dict_fields(POWERPLAY_MAPPING)
            elif self.file_type == 'population':
                self._map_dict_fields(POPULATION_MAPPING)
            elif self.file_type == 'systems':
                self._map_dict_fields(SYSTEM_MAPPING)

            print('Output')
            pprint(self.output_dict)
            print('\n\n')
            yield json.dumps(self.output_dict)
        except Exception as e:
            logging.info(e)
            logging.warning('Unable to parse JSON line: %s', element)


def main(argv):
    del argv  # Unused.
    with apache_beam.Pipeline(
            options=beam.get_default_pipeline_options()) as p:
        lines = (p
                 | apache_beam.io.ReadFromText('gs://raxxla/edsm/powerplay.gz')
                 | apache_beam.ParDo(FormatEdsmJson('powerplay'))
                 |
                 apache_beam.io.WriteToText('gs://raxxla/edsm/powerplay_ndjson'))


if __name__ == '__main__':
    app.run(main)
