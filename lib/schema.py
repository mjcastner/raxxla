import json

from absl import logging


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
    self.services = {
      'market': None,
      'shipyard': None,
      'outfitting': None,
      'other': [],
      'market_metadata': {
        'id': None,
        'economy': None,
      }
    }
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
    self.date = None
    self.coordinates = {
      'x': None,
      'y': None,
      'z': None,
      'coordinates': None
    }

  def from_json(self, input_json):
    try:
      input_dict = json.loads(input_json)
      
      self.id = input_dict['id64']
      self.name = input_dict['name']
      self.date = input_dict['date']
      self.coordinates['x'] = input_dict['coords']['x']
      self.coordinates['y'] = input_dict['coords']['y']
      self.coordinates['z'] = input_dict['coords']['z']
      self.coordinates['coordinates'] = '%s, %s, %s' % (self.coordinates['x'],
                                                        self.coordinates['y'],
                                                        self.coordinates['z'])
    except KeyError as e:
      logging.warning('Unable to map field %s, double check --input_type flag.',
                      e)

  def to_json(self):
    return json.dumps(self, 
                      default=lambda 
                      o: o.__dict__)
