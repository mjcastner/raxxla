import json

from absl import logging


class body:
  def __init__(self):
    self.id = None
    self.system_id = None
    self.relative_id = None
    self.name = None
    self.details = None
    self.atmosphere = None
    self.composition = None
    self.parents = None
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




  def from_json(self, input_json):
    try:
      input_dict = json.loads(input_json)
      
      self.id = input_dict['id64']
      self.system_id = input_dict['systemId64']
      self.relative_id = input_dict['bodyId']
      self.name = input_dict['name']
      self.details = {
        'type': input_dict['type'],
        'subtype': input_dict['subType'],
        'distance': input_dict['distanceToArrival'],
        'mass': input_dict['earthMasses'],
        'gravity': input_dict['gravity'],
        'landable': input_dict['isLandable'],
        'radius': input_dict['radius'],
        'temperature': input_dict['surfaceTemperature'],
        'pressure': input_dict['surfacePressure'],
        'volcanism': input_dict['volcanismType'],
        'terraforming': input_dict['terraformingState'],
      }
      self.atmosphere = 'atmosphere': {
        'type': None,
        'composition': None,
      }

      

      self.coordinates['x'] = input_dict['coords']['x']
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
        'coordinates': '%s, %s, %s' % (self.coordinates['x'],
                                       self.coordinates['y'],
                                       self.coordinates['z'])
      }
      self.updated = input_dict['date']
    except KeyError as e:
      logging.warning('Unable to map field %s, double check --input_type flag.',
                      e)

  def to_json(self):
    return json.dumps(self, 
                      default=lambda 
                      o: o.__dict__)
