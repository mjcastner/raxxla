import json

from absl import logging
from google.cloud import bigquery


# File metadata / schema
urls = {
    'bodies': 'https://www.edsm.net/dump/bodies7days.json.gz',
    'population': 'https://www.edsm.net/dump/systemsPopulated.json.gz',
    'powerplay': 'https://www.edsm.net/dump/powerPlay.json.gz',
    'stations': 'https://www.edsm.net/dump/stations.json.gz',
    'systems': 'https://www.edsm.net/dump/systemsWithCoordinates.json.gz',
}
file_types = list(urls.keys())

bodies = [
    bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("system_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("relative_id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("properties", "RECORD", mode="NULLABLE", fields=[
        bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("subtype", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("distance", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mass", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("gravity", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("landable", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("radius", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("temperature", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pressure", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("volcanism", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("terraforming", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("luminosity", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("solar_masses", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("solar_radius", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("spectral_class", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("reserve_level", "STRING", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("atmosphere", "RECORD", mode="NULLABLE", fields=[
        bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("composition", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("percentage", "FLOAT", mode="NULLABLE"),
        ]),
    ]),
    bigquery.SchemaField("belts", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("mass", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("inner_radius", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("outer_radius", "INTEGER", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("composition", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("percentage", "FLOAT", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("materials", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("percentage", "FLOAT", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("orbit", "RECORD", mode="NULLABLE", fields=[
        bigquery.SchemaField("period", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("rotational_period", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("tidally_locked", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("periapsis", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("eccentricity", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("inclination", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("semimajor_axis", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("axial_tilt", "FLOAT", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("parents", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("relative_id", "INTEGER", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("rings", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("mass", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("inner_radius", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("outer_radius", "INTEGER", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("updated", "TIMESTAMP", mode="REQUIRED"),
]

population = [
    bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("society", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("factions", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("allegiance", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("controlling", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("government", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("influence", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("happiness", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("player_faction", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("states", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
        ]),
    ]),
    bigquery.SchemaField("updated", "TIMESTAMP", mode="NULLABLE"),
]

powerplay = [
    bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("power", "RECORD", mode="NULLABLE", fields=[
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("allegiance", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("government", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("updated", "TIMESTAMP", mode="NULLABLE"),
]

stations = [
    bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("system_id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("distance", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("economy", "RECORD", mode="NULLABLE", fields=[
        bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("subtype", "STRING", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("services", "RECORD", mode="NULLABLE", fields=[
        bigquery.SchemaField("market", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("shipyard", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("outfitting", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("other", "STRING", mode="REPEATED"),
    ]),
    bigquery.SchemaField("allegiance", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("faction", "RECORD", mode="NULLABLE", fields=[
        bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("government", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("updated", "TIMESTAMP", mode="NULLABLE"),

]

systems = [
    bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("coordinates", "RECORD", mode="NULLABLE", fields=[
        bigquery.SchemaField("x", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("y", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("z", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("coordinates", "STRING", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("updated", "TIMESTAMP", mode="NULLABLE"),
]


# EDSM Class
class edsmObject:
  def __init__(self, filetype):
    assert filetype in file_types
    self.filetype = filetype
    self.attributes = {
        'dataset': {
            'StringValue': 'edsm',
            'DataType': 'String'
        }
    }

    # TODO(mjcastner): Add self.batch_size for each file to get around SQS
    # message size issue.
    if filetype == 'systems':
      self.schema = systems
      self.attributes['table'] = {
          'StringValue': 'systems',
          'DataType': 'String'}
    elif filetype == 'population':
      self.schema = population
      self.attributes['table'] = {
          'StringValue': 'population',
          'DataType': 'String'}
    elif filetype == 'bodies':
      self.schema = bodies
      self.attributes['table'] = {
          'StringValue': 'bodies',
          'DataType': 'String'}
    elif filetype == 'powerplay':
      self.schema = powerplay
      self.attributes['table'] = {
          'StringValue': 'powerplay',
          'DataType': 'String'}
    elif filetype == 'stations':
      self.schema = stations
      self.attributes['table'] = {
          'StringValue': 'stations',
          'DataType': 'String'}
    else:
      self.schema = None
      self.attributes['table'] = None

  @staticmethod
  def __format_faction_states(input_dict):
    output_list = []

    # Append current state
    current_state = {
        'name': input_dict.get('state'),
        'type': 'Current',
    }
    if current_state.get('name') != 'None':
      output_list.append(current_state)

    # Append recovering states
    recovering_states = input_dict.get('recoveringStates')
    for state in recovering_states:
      recovering_state = {
          'name': state.get('state'),
          'type': 'Recovering',
      }
      output_list.append(recovering_state)

    # Append pending states
    pending_states = input_dict.get('pendingStates')
    for state in pending_states:
      pending_state = {
          'name': state.get('state'),
          'type': 'Pending',
      }
      output_list.append(pending_state)

    return output_list

  def __format_factions(self, input_dict):
    output_list = []
    factions = input_dict.get('factions')

    if factions:
      for faction in factions:
        output_dict = {
            'id': faction.get('id'),
            'name': faction.get('name'),
            'allegiance': faction.get('allegiance'),
            'controlling': False,
            'government': faction.get('government'),
            'influence': faction.get('influence'),
            'happiness': faction.get('happiness'),
            'player_faction': faction.get('isPlayer'),
            'states': self.__format_faction_states(faction),
        }

        # Toggle controlling faction bool
        controlling_faction_id = input_dict.get(
            'controllingFaction', {}).get('id')

        if faction.get('id') == controlling_faction_id:
          output_dict['controlling'] = True

        output_list.append(output_dict)

    return output_list

  @staticmethod
  def __format_parents(input_list):
    output_list = []

    if input_list:
      for parent in input_list:
        for key, value in parent.items():
          if key != 'Null':
            output_dict = {
                'type': key,
                'relative_id': value,
            }
            output_list.append(output_dict)

    return output_list

  @staticmethod
  def __format_percentage(input_dict):
    output_list = []

    if input_dict:
      for key, value in input_dict.items():
        output_dict = {
            'type': key,
            'percentage': value,
        }
        output_list.append(output_dict)

    return output_list

  @staticmethod
  def __format_ringlike(input_list):
    output_list = []

    if input_list:
      for belt in input_list:
        output_dict = {
            'name': belt.get('name'),
            'type': belt.get('type'),
            'mass': belt.get('mass'),
            'inner_radius': belt.get('innerRadius'),
            'outer_radius': belt.get('outerRadius'),
        }
        output_list.append(output_dict)

    return output_list

  def _format_body(self, raw_json) -> str:
    input_dict = json.loads(raw_json)
    formatted_dict = {
        'id': input_dict.get('id64'),
        'system_id': input_dict.get('systemId64'),
        'relative_id': input_dict.get('bodyId'),
        'name': input_dict.get('name'),
        'properties': {
            'type': input_dict.get('type'),
            'subtype': input_dict.get('subType'),
            'distance': input_dict.get('distanceToArrival'),
            'mass': input_dict.get('earthMasses'),
            'gravity': input_dict.get('gravity'),
            'landable': input_dict.get('isLandable'),
            'radius': input_dict.get('radius'),
            'temperature': input_dict.get('surfaceTemperature'),
            'pressure': input_dict.get('surfacePressure'),
            'volcanism': input_dict.get('volcanismType'),
            'terraforming': input_dict.get('terraformingState'),
            'luminosity': input_dict.get('luminosity'),
            'solar_masses': input_dict.get('solarMasses'),
            'solar_radius': input_dict.get('solarRadius'),
            'spectral_class': input_dict.get('spectralClass'),
            'reserve_level': input_dict.get('reserveLevel'),
        },
        'atmosphere': {
            'type': input_dict.get('atmosphereType'),
            'composition': self.__format_percentage(
                input_dict.get('atmosphereComposition')),
        },
        'belts': self.__format_ringlike(input_dict.get('belts')),
        'composition': self.__format_percentage(
            input_dict.get('solidComposition')),
        'materials': self.__format_percentage(input_dict.get('materials')),
        'orbit': {
            'period': input_dict.get('orbitalPeriod'),
            'rotational_period': input_dict.get('rotationalPeriod'),
            'tidally_locked': input_dict.get('rotationalPeriodTidallyLocked'),
            'periapsis': input_dict.get('argOfPeriapsis'),
            'eccentricity': input_dict.get('orbitalEccentricity'),
            'inclination': input_dict.get('orbitalInclination'),
            'semimajor_axis': input_dict.get('semiMajorAxis'),
            'axial_tilt': input_dict.get('axialTilt'),
        },
        'parents': self.__format_parents(input_dict.get('parents')),
        'rings': self.__format_ringlike(input_dict.get('rings')),
        'updated': input_dict.get('updateTime'),
    }

    formatted_json = json.dumps(formatted_dict)
    return formatted_json

  def _format_population(self, raw_json) -> str:
    input_dict = json.loads(raw_json)
    formatted_dict = {
        'id': input_dict.get('id64'),
        'name': input_dict.get('name'),
        'society': {
            'allegiance': input_dict.get('allegiance'),
            'government': input_dict.get('government'),
            'state': input_dict.get('state'),
            'economy': input_dict.get('economy'),
            'security': input_dict.get('security'),
            'population': input_dict.get('population'),
        },
        'factions': self.__format_factions(input_dict),
        'updated': input_dict.get('date'),
    }

    formatted_json = json.dumps(formatted_dict)
    return formatted_json

  def _format_powerplay(self, raw_json) -> str:
    input_dict = json.loads(raw_json)
    formatted_dict = {
        'id': input_dict.get('id64'),
        'state': input_dict.get('state'),
        'power': {
            'name': input_dict.get('power'),
            'state': input_dict.get('powerState'),
        },
        'allegiance': input_dict.get('allegiance'),
        'government': input_dict.get('government'),
        'updated': input_dict.get('date'),
    }

    formatted_json = json.dumps(formatted_dict)
    return formatted_json

  def _format_station(self, raw_json) -> str:
    input_dict = json.loads(raw_json)
    formatted_dict = {
        'id': input_dict.get('id'),
        'system_id': input_dict.get('systemId64'),
        'name': input_dict.get('name'),
        'type': input_dict.get('type'),
        'distance': input_dict.get('distanceToArrival'),
        'economy': {
            'id': input_dict.get('marketId'),
            'type': input_dict.get('economy'),
            'sub_type': input_dict.get('secondEconomy'),
        },
        'services': {
            'market': input_dict.get('haveMarket'),
            'shipyard': input_dict.get('haveShipyard'),
            'outfitting': input_dict.get('haveOutfitting'),
            'other': input_dict.get('otherServices'),
        },
        'allegiance': input_dict.get('allegiance'),
        'faction': {
            'id': input_dict.get('controllingFaction', {}).get('id'),
            'name': input_dict.get('controllingFaction', {}).get('name'),
        },
        'government': input_dict.get('government'),
        'updated': input_dict.get('updateTime', {}).get('information'),
    }

    formatted_json = json.dumps(formatted_dict)
    return formatted_json

  def _format_system(self, raw_json) -> str:
    input_dict = json.loads(raw_json)
    formatted_dict = {
        'id': input_dict.get('id64'),
        'name': input_dict.get('name'),
        'coordinates': {
            'x': input_dict.get('coords', {}).get('x'),
            'y': input_dict.get('coords', {}).get('y'),
            'z': input_dict.get('coords', {}).get('z'),
            'coordinates': '%s, %s, %s' % (input_dict.get('coords', {}).get('x'),
                                           input_dict.get('coords', {}).get('y'),
                                           input_dict.get('coords', {}).get('z'))
        },
        'updated': input_dict.get('date'),
    }

    formatted_json = json.dumps(formatted_dict)
    return formatted_json

  def format_json(self, raw_json) -> str:
    formatted_json = None

    if self.filetype == 'bodies':
      formatted_json = self._format_body(raw_json)
    elif self.filetype == 'population':
      formatted_json = self._format_population(raw_json)
    elif self.filetype == 'powerplay':
      formatted_json = self._format_powerplay(raw_json)
    elif self.filetype == 'stations':
      formatted_json = self._format_station(raw_json)
    elif self.filetype == 'systems':
      formatted_json = self._format_system(raw_json)
    else:
      logging.error('Unexpected filetype %s', self.filetype)
      raise KeyError

    return formatted_json
