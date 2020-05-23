import json

from absl import logging


class body:
  def __init__(self):
    self.id = None
    self.system_id = None
    self.relative_id = None
    self.name = None
    self.properties = {
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
    self.belts = []
    self.composition = []
    self.materials = []
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
    self.parents = []
    self.rings = []
    self.updated = None

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

  def from_json(self, input_json):
    try:
      input_dict = json.loads(input_json)

      self.id = input_dict.get('id64')
      self.system_id = input_dict.get('systemId64')
      self.relative_id = input_dict.get('bodyId')
      self.name = input_dict.get('name')
      self.properties = {
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
      }
      self.atmosphere = {
          'type': input_dict.get('atmosphereType'),
          'composition': self.__format_percentage(
              input_dict.get('atmosphereComposition')),
      }
      self.belts = self.__format_ringlike(input_dict.get('belts'))
      self.composition = self.__format_percentage(
          input_dict.get('solidComposition'))
      self.materials = self.__format_percentage(input_dict.get('materials'))
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
      self.parents = self.__format_parents(input_dict.get('parents'))
      self.rings = self.__format_ringlike(input_dict.get('rings'))
      self.updated = input_dict.get('updateTime')
    except KeyError as e:
      logging.warning('Unable to map field %s, double check --type flag.',
                      e)

  def to_json(self):
    return json.dumps(self,
                      default=lambda o: o.__dict__)


class population:
  def __init__(self):
    self.id = None
    self.name = None
    self.society = None
    self.factions = []
    self.updated = None

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

  def from_json(self, input_json):
    try:
      input_dict = json.loads(input_json)

      self.id = input_dict.get('id64')
      self.name = input_dict.get('name')
      self.society = {
          'allegiance': input_dict.get('allegiance'),
          'government': input_dict.get('government'),
          'state': input_dict.get('state'),
          'economy': input_dict.get('economy'),
          'security': input_dict.get('security'),
          'population': input_dict.get('population'),
      }
      self.factions = self.__format_factions(input_dict)
      self.updated = input_dict.get('date')
    except KeyError as e:
      logging.warning('Unable to map field %s, double check --type flag.',
                      e)

  def to_json(self):
    return json.dumps(self,
                      default=lambda o: o.__dict__)


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

      self.system_id = input_dict.get('id64')
      self.system_state = None
      self.power = input_dict.get('power')
      self.power_state = input_dict.get('powerState')
      self.allegiance = input_dict.get('allegiance')
      self.government = input_dict.get('government')
      self.updated = input_dict.get('date')
    except KeyError as e:
      logging.warning('Unable to map field %s, double check --type flag.',
                      e)

  def to_json(self):
    return json.dumps(self,
                      default=lambda o: o.__dict__)


class station:
  def __init__(self):
    self.system_id = None
    self.id = None
    self.name = None
    self.type = None
    self.distance = None
    self.economy = None
    self.services = None
    self.allegiance = None
    self.government = None
    self.faction = None
    self.updated = None

  def from_json(self, input_json):
    try:
      input_dict = json.loads(input_json)

      self.system_id = input_dict.get('systemId64')
      self.id = input_dict.get('id')
      self.name = input_dict.get('name')
      self.type = input_dict.get('type')
      self.distance = input_dict.get('distanceToArrival')
      self.economy = {
          'id': input_dict.get('marketId'),
          'type': input_dict.get('economy'),
          'sub_type': input_dict.get('secondEconomy'),
      }
      self.services = {
          'market': input_dict.get('haveMarket'),
          'shipyard': input_dict.get('haveShipyard'),
          'outfitting': input_dict.get('haveOutfitting'),
          'other': input_dict.get('otherServices'),
      }
      self.allegiance = input_dict.get('allegiance')
      self.government = input_dict.get('government')
      self.faction = input_dict.get('controllingFaction')
      self.updated = input_dict.get('updateTime', {}).get('information')
    except KeyError as e:
      logging.warning('Unable to map field %s, double check --type flag.',
                      e)

  def to_json(self):
    return json.dumps(self,
                      default=lambda o: o.__dict__)


class system:
  def __init__(self):
    self.id = None
    self.name = None
    self.coordinates = None
    self.updated = None

  def from_json(self, input_json):
    try:
      input_dict = json.loads(input_json)

      self.id = input_dict.get('id64')
      self.name = input_dict.get('name')
      self.coordinates = {
          'x': input_dict.get('coords', {}).get('x'),
          'y': input_dict.get('coords', {}).get('y'),
          'z': input_dict.get('coords', {}).get('z'),
          'coordinates': '%s, %s, %s' % (input_dict.get('coords', {}).get('x'),
                                         input_dict.get('coords', {}).get('y'),
                                         input_dict.get('coords', {}).get('z'))
      }
      self.updated = input_dict.get('date')
    except KeyError as e:
      logging.warning('Unable to map field %s, double check --type flag.',
                      e)

  def to_json(self):
    return json.dumps(self,
                      default=lambda o: o.__dict__)
