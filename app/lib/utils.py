import json
import re

from pprint import pprint

from absl import logging

JSON_RE_PATTERN = re.compile(r'(\{.*\})')
JSON_RE_SEARCH = JSON_RE_PATTERN.search


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

  def format_parents(input_object: list):
    output_list = []

    if input_object:
      for parent in input_object:
        for key, value in parent.items():
          output_list.append({
              'type': key,
              'relative_id': value,
          })

    return output_list

  def format_percentage(input_object):
    output_list = []

    if input_object:
      for key, value in input_object.items():
        output_list.append({
            'type': key,
            'percentage': value,
        })

    return output_list
  
  def format_ringlike(input_object):
    output_list = []

    if input_object:
      for ringlike in input_object:
        output_list.append({
          'name': ringlike.get('name'),
          'type': ringlike.get('type'),
          'mass': float(ringlike.get('mass', 0)),
          'inner_radius': float(ringlike.get('innerRadius', 0)),
          'outer_radius': float(ringlike.get('outerRadius', 0)),
        })

    return output_list

  def format_bodies(raw_json: str):
    input_object = json.loads(raw_json)
    output_object = {
        'id': input_object.get('id64'),
        'system_id': input_object.get('systemId64'),
        'relative_id': input_object.get('bodyId'),
        'name': input_object.get('name'),
        'properties': {
            'type': input_object.get('type'),
            'subtype': input_object.get('subType'),
            'distance': input_object.get('distanceToArrival'),
            'mass': input_object.get('earthMasses'),
            'gravity': input_object.get('gravity'),
            'landable': input_object.get('isLandable'),
            'radius': input_object.get('radius'),
            'temperature': input_object.get('surfaceTemperature'),
            'pressure': input_object.get('surfacePressure'),
            'volcanism': input_object.get('volcanismType'),
            'terraforming': input_object.get('terraformingState'),
            'luminosity': input_object.get('luminosity'),
            'solar_masses': input_object.get('solarMasses'),
            'solar_radius': input_object.get('solarRadius'),
            'spectral_class': input_object.get('spectralClass'),
            'reserve_level': input_object.get('reserveLevel'),
        },
        'atmosphere': {
            'type': input_object.get('atmosphereType'),
            'composition': format_percentage(input_object.get('atmosphereComposition')),
        },
        'belts': format_ringlike(input_object.get('belts')),
        'rings': format_ringlike(input_object.get('rings')),
        'composition': format_percentage(input_object.get('solidComposition')),
        'materials': format_percentage(input_object.get('materials')),
        'parents': format_parents(input_object.get('parents')),
        'orbit': {
            'period': input_object.get('orbitalPeriod'),
            'rotational_period': input_object.get('rotationalPeriod'),
            'tidally_locked': input_object.get('rotationalPeriodTidallyLocked'),
            'periapsis': input_object.get('argOfPeriapsis'),
            'eccentricity': input_object.get('orbitalEccentricity'),
            'inclination': input_object.get('orbitalInclination'),
            'semimajor_axis': input_object.get('semiMajorAxis'),
            'axial_tilt': input_object.get('axialTilt'),
        },
        'updated': input_object.get('updateTime'),
    }

    return json.dumps(output_object)

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

  def format_stations(raw_json):
    input_object = json.loads(raw_json)
    output_object = {
        'id': input_object.get('id'),
        'system_id': input_object.get('systemId64'),
        'name': input_object.get('name'),
        'type': input_object.get('type'),
        'distance': input_object.get('distanceToArrival'),
        'economy': {
            'id': input_object.get('marketId'),
            'type': input_object.get('economy'),
            'sub_type': input_object.get('secondEconomy'),
        },
        'services': {
            'market': input_object.get('haveMarket'),
            'shipyard': input_object.get('haveShipyard'),
            'outfitting': input_object.get('haveOutfitting'),
            'other': input_object.get('otherServices'),
        },
        'allegiance': input_object.get('allegiance'),
        'faction': {
            'id': input_object.get('controllingFaction', {}).get('id'),
            'name': input_object.get('controllingFaction', {}).get('name'),
        },
        'government': input_object.get('government'),
        'updated': input_object.get('updateTime', {}).get('information'),
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

  if not raw_json:
    return

  if file_type == 'bodies':
    formatted_json = format_bodies(raw_json)
  elif file_type == 'population':
    formatted_json = format_population(raw_json)
  elif file_type == 'powerplay':
    formatted_json = format_powerplay(raw_json)
  elif file_type == 'stations':
    formatted_json = format_stations(raw_json)
  elif file_type == 'systems':
    formatted_json = format_systems(raw_json)
  else:
    logging.error('Unsupported input file type.')
    formatted_json = None

  return formatted_json


def extract_json(raw_input):
  json_re_match = JSON_RE_SEARCH(raw_input)
  if json_re_match:
    json_string = json_re_match.group(1)
    return json_string
