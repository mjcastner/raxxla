import json

from absl import logging


class body:
  def __init__(self):
    self.id = None
    self.system_id = None
    self.relative_id = None
    self.name = None
    self.details = {
      'type': None,
      'subtype': None,
      'distance': None,
      'mass': None,
      'gravity': None,
      'landable': None,
      'radius': None,
      'temperature': None,
      'pressure': None,
      'volcanism': None,
      'terraforming': None,
    }
    self.atmosphere = {
      'type': None,
      'composition': [],
    }
    self.composition = []
    self.parents = []
    self.orbit = {
      'period': None,
      'rotational_period': None,
      'tidally_locked': None,
      'periapsis': None,
      'eccentricity': None,
      'inclination': None,
      'semimajor_axis': None,
      'axial_tilt': None,
    }
    self.updated = None


  def _format_parents(self, input_list):
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


  def _format_percentage(self, input_dict):
    output_list = []

    if input_dict:
      for key, value in input_dict.items():
        output_dict = {
          'type': key,
          'percentage': value,
        }
        output_list.append(output_dict)

    return output_list


  def from_json(self, input_json):
    try:
      input_dict = json.loads(input_json)
      
      self.id = input_dict.get('id64')
      self.system_id = input_dict.get('systemId64')
      self.relative_id = input_dict.get('bodyId')
      self.name = input_dict.get('name')
      self.details = {
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
      }
      self.atmosphere = {
        'type': input_dict.get('atmosphereType'),
        'composition': self._format_percentage(input_dict.get('atmosphereComposition')),
      }
      self.composition = self._format_percentage(input_dict.get('solidComposition'))
      self.parents = self._format_parents(input_dict.get('parents'))
      self.orbit = {
        'period': input_dict.get('orbitalPeriod'),
        'rotational_period': input_dict.get('rotationalPeriod'),
        'tidally_locked': input_dict.get('rotationalPeriodTidallyLocked'),
        'periapsis': input_dict.get('argOfPeriapsis'),
        'eccentricity': input_dict.get('orbitalEccentricity'),
        'inclination': input_dict.get('orbitalInclination'),
        'semimajor_axis': input_dict.get('semiMajorAxis'),
        'axial_tilt': input_dict.get('axialTilt'),
      }
      self.updated = input_dict.get('updateTime')
    except KeyError as e:
      logging.warning('Unable to map field %s, double check --input_type flag.',
                      e)


  def to_json(self):
    return json.dumps(self, 
                      default=lambda 
                      o: o.__dict__)


class powerplay:
  def __init__(self):
    self.system_id = None
    self.system_state = None
    self.power = None
    self.power_state = None
    self.allegiance = None
    self.government = None
    self.updated = None

  def from_json(self, input_json):
    try:
      input_dict = json.loads(input_json)
      
      self.system_id = input_dict['id64']
      self.system_state = None
      self.power = input_dict['power']
      self.power_state = input_dict['powerState']
      self.allegiance = input_dict['allegiance']
      self.government = input_dict['government']
      self.updated = input_dict['date']
    except KeyError as e:
      logging.warning('Unable to map field %s, double check --input_type flag.',
                      e)

  def to_json(self):
    return json.dumps(self, 
                      default=lambda 
                      o: o.__dict__)


class station:
  def __init__(self):
    self.system_id = None
    self.id = None
    self.name = None
    self.type = None
    self.distance = None
    self.services = None
    self.allegiance = None
    self.government = None
    self.updated = None

  def from_json(self, input_json):
    try:
      input_dict = json.loads(input_json)
      
      self.system_id = input_dict['systemId64']
      self.id = input_dict['id']
      self.name = input_dict['name']
      self.type = input_dict['type']
      self.distance = input_dict['distanceToArrival']
      self.services = {
        'market': input_dict['haveMarket'],
        'shipyard': input_dict['haveShipyard'],
        'outfitting': input_dict['haveOutfitting'],
        'other': input_dict['otherServices'],
        'market_metadata': {
          'id': input_dict['marketId'],
          'economy': input_dict['economy'],
        }
      }
      self.allegiance = input_dict['allegiance']
      self.government = input_dict['government']
      self.updated = input_dict.get('updateTime', {}).get('information')
    except KeyError as e:
      logging.warning('Unable to map field %s, double check --input_type flag.',
                      e)

  def to_json(self):
    return json.dumps(self, 
                      default=lambda 
                      o: o.__dict__)


class system:
  def __init__(self):
    self.id = None
    self.name = None
    self.coordinates = None
    self.updated = None

  def from_json(self, input_json):
    try:
      input_dict = json.loads(input_json)
      
      self.id = input_dict['id64']
      self.name = input_dict['name']
      self.coordinates = {
        'x': input_dict['coords']['x'],
        'y': input_dict['coords']['y'],
        'z': input_dict['coords']['z'],
        'coordinates': '%s, %s, %s' % (input_dict['coords']['x'],
                                       input_dict['coords']['y'],
                                       input_dict['coords']['z'])
      }
      self.updated = input_dict['date']
    except KeyError as e:
      logging.warning('Unable to map field %s, double check --input_type flag.',
                      e)

  def to_json(self):
    return json.dumps(self, 
                      default=lambda 
                      o: o.__dict__)
