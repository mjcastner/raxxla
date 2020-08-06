import json
import re

from pprint import pprint

from absl import logging

import apache_beam


class FormatEdsmJson(apache_beam.DoFn):
  def __init__(self, file_type: str):
    self.file_type = file_type

  @staticmethod
  def __format_faction_states(input_object):
    output_list = []

    current_state = {
        'name': input_object.get('state'),
        'type': 'Current',
    }
    if current_state.get('name') != 'None':
      output_list.append(current_state)

    recovering_states = input_object.get('recoveringStates')
    for state in recovering_states:
      recovering_state = {
          'name': state.get('state'),
          'type': 'Recovering',
      }
      output_list.append(recovering_state)

    pending_states = input_object.get('pendingStates')
    for state in pending_states:
      pending_state = {
          'name': state.get('state'),
          'type': 'Pending',
      }
      output_list.append(pending_state)

    return output_list

  def __format_factions(self, input_object):
    output_list = []
    factions = input_object.get('factions')

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
        controlling_faction_id = input_object.get(
            'controllingFaction', {}).get('id')

        if faction.get('id') == controlling_faction_id:
          output_dict['controlling'] = True

        output_list.append(output_dict)

    return output_list

  def _format_population(self):
    input_object = json.loads(self.json_string)
    output_object = {
        'id': input_object.get('id64'),
        'name': input_object.get('name'),
        'society': {
            'allegiance': input_object.get('allegiance'),
            'government': input_object.get('government'),
            'state': input_object.get('state'),
            'economy': input_object.get('economy'),
            'security': input_object.get('security'),
            'population': input_object.get('population'),
        },
        'factions': self.__format_factions(input_object),
        'updated': input_object.get('date'),
    }

    return json.dumps(output_object)

  def _format_powerplay(self):
    input_object = json.loads(self.json_string)
    output_object = {
        'id': input_object.get('id64'),
        'state': input_object.get('state'),
        'power': {
            'name': input_object.get('power'),
            'state': input_object.get('powerState'),
        },
        'allegiance': input_object.get('allegiance'),
        'government': input_object.get('government'),
        'updated': input_object.get('date'),
    }

    return json.dumps(output_object)

  def _format_systems(self):
    input_object = json.loads(self.json_string)
    output_object = {
        'id': input_object.get('id64'),
        'name': input_object.get('name'),
        'coordinates': {
            'x': input_object.get('coords', {}).get('x'),
            'y': input_object.get('coords', {}).get('y'),
            'z': input_object.get('coords', {}).get('z'),
            'coordinates': '%s, %s, %s' % (
                input_object.get('coords', {}).get('x'),
                input_object.get('coords', {}).get('y'),
                input_object.get('coords', {}).get('z'),
            ),
        },
        'updated': input_object.get('date'),
    }

    return json.dumps(output_object)

  def process(self, json_string):
    self.json_string = json_string
    if self.file_type == 'population':
      formatted_json = self._format_population()
    elif self.file_type == 'powerplay':
      formatted_json = self._format_powerplay()
    elif self.file_type == 'systems':
      formatted_json = self._format_systems()
    else:
      logging.error('Unsupported input file type.')
      formatted_json = None

    yield formatted_json

def format_edsm_json(raw_json: str, file_type: str):
  def format_faction_states(input_object):
    output_list = []

    current_state = {
        'name': input_object.get('state'),
        'type': 'Current',
    }
    if current_state.get('name') != 'None':
      output_list.append(current_state)

    recovering_states = input_object.get('recoveringStates')
    for state in recovering_states:
      recovering_state = {
          'name': state.get('state'),
          'type': 'Recovering',
      }
      output_list.append(recovering_state)

    pending_states = input_object.get('pendingStates')
    for state in pending_states:
      pending_state = {
          'name': state.get('state'),
          'type': 'Pending',
      }
      output_list.append(pending_state)

    return output_list

  def format_factions(input_object):
    output_list = []
    factions = input_object.get('factions')

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
            'states': format_faction_states(faction),
        }

        # Toggle controlling faction bool
        controlling_faction_id = input_object.get(
            'controllingFaction', {}).get('id')

        if faction.get('id') == controlling_faction_id:
          output_dict['controlling'] = True

        output_list.append(output_dict)

    return output_list

  def format_population(raw_json):
    input_object = json.loads(raw_json)
    output_object = {
        'id': input_object.get('id64'),
        'name': input_object.get('name'),
        'society': {
            'allegiance': input_object.get('allegiance'),
            'government': input_object.get('government'),
            'state': input_object.get('state'),
            'economy': input_object.get('economy'),
            'security': input_object.get('security'),
            'population': input_object.get('population'),
        },
        'factions': format_factions(input_object),
        'updated': input_object.get('date'),
    }

    return json.dumps(output_object)

  def format_powerplay(raw_json):
    input_object = json.loads(raw_json)
    output_object = {
        'id': input_object.get('id64'),
        'state': input_object.get('state'),
        'power': {
            'name': input_object.get('power'),
            'state': input_object.get('powerState'),
        },
        'allegiance': input_object.get('allegiance'),
        'government': input_object.get('government'),
        'updated': input_object.get('date'),
    }

    return json.dumps(output_object)

  def format_systems(raw_json):
    input_object = json.loads(raw_json)
    output_object = {
        'id': input_object.get('id64'),
        'name': input_object.get('name'),
        'coordinates': {
            'x': input_object.get('coords', {}).get('x'),
            'y': input_object.get('coords', {}).get('y'),
            'z': input_object.get('coords', {}).get('z'),
            'coordinates': '%s, %s, %s' % (
                input_object.get('coords', {}).get('x'),
                input_object.get('coords', {}).get('y'),
                input_object.get('coords', {}).get('z'),
            ),
        },
        'updated': input_object.get('date'),
    }

    return json.dumps(output_object)

  if file_type == 'population':
    formatted_json = format_population(raw_json)
  elif file_type == 'powerplay':
    formatted_json = format_powerplay(raw_json)
  elif file_type == 'systems':
    formatted_json = format_systems(raw_json)
  else:
    logging.error('Unsupported input file type.')
    formatted_json = None

  return formatted_json


def extract_json(raw_input):
  try:
    json_re_match = re.search(r'(\{.*\})', raw_input)
    if json_re_match:
      json_string = json_re_match.group(1)
      return json_string
  except ValueError:
    pass
  except AttributeError:
    pass


class RemoveNull(apache_beam.DoFn):
  def process(self, element):
    if element is not None:
      yield element
    else:
      return
