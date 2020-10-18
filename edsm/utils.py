import json
import re
from datetime import datetime

from proto import society_pb2
from proto import system_pb2

from absl import logging

# Global vars
JSON_RE_PATTERN = re.compile(r'(\{.*\})')
JSON_RE_SEARCH = JSON_RE_PATTERN.search


def edsm_json_to_proto(file_type: str, edsm_json: str):
  edsm_dict = json.loads(edsm_json)

  if file_type == 'population':
    population_num = edsm_dict.get('population')
    if not population_num:
      population_num = 0

    population = society_pb2.Population()
    population.planet_id = int(edsm_dict.get('id64'))
    population.allegiance = str(edsm_dict.get('allegiance'))
    population.government = str(edsm_dict.get('government'))
    population.state = str(edsm_dict.get('state'))
    population.economy = str(edsm_dict.get('economy'))
    population.security = str(edsm_dict.get('security'))
    population.population = population_num
    population.updated = int(datetime.strptime(
        edsm_dict.get('date'), 
        '%Y-%m-%d %H:%M:%S').timestamp()
    )

    factions = edsm_dict.get('factions')
    if factions:
      for faction_dict in factions:
        faction = population.factions.add()
        faction.id = int(faction_dict.get('id'))
        faction.name = str(faction_dict.get('name'))
        faction.allegiance = str(faction_dict.get('allegiance'))
        faction.government = str(faction_dict.get('government'))
        faction.influence = float(faction_dict.get('influence'))
        faction.happiness = str(faction_dict.get('happiness'))
        faction.player_faction = faction_dict.get('isPlayer')

        controlling_faction_id = faction_dict.get(
            'controllingFaction', {}).get('id')
        if controlling_faction_id == faction_dict.get('id'):
          faction.controlling = True
        else:
          faction.controlling = False

        current_state = faction.states.add()
        current_state.name = str(faction_dict.get('state'))
        current_state.type = 'Current'

        for pending_state_dict in faction_dict.get('pendingStates'):
          pending_state = faction.states.add()
          pending_state.name = pending_state_dict.get('state')
          pending_state.type = 'Pending'
        
        for recovering_state_dict in faction_dict.get('recoveringStates'):
          recovering_state = faction.states.add()
          recovering_state.name = recovering_state_dict.get('state')
          recovering_state.type = 'Recovering'

    return population

  elif file_type == 'powerplay':
    powerplay = society_pb2.Powerplay()
    powerplay.system_id = int(edsm_dict.get('id64'))
    powerplay.power.name = str(edsm_dict.get('power'))
    powerplay.power.state = str(edsm_dict.get('powerState'))
    powerplay.allegiance = str(edsm_dict.get('allegiance'))
    powerplay.state = str(edsm_dict.get('state'))
    powerplay.government = str(edsm_dict.get('government'))
    powerplay.updated = int(datetime.strptime(
        edsm_dict.get('date'), 
        '%Y-%m-%d %H:%M:%S').timestamp()
    )

    return powerplay
  
  elif file_type == 'systems':
    system = system_pb2.System()
    system.id = int(edsm_dict.get('id64'))
    system.name = str(edsm_dict.get('name'))
    system.coordinates.x = float(edsm_dict.get('coords', {}).get('x'))
    system.coordinates.y = float(edsm_dict.get('coords', {}).get('y'))
    system.coordinates.z = float(edsm_dict.get('coords', {}).get('z'))
    system.coordinates.coordinates = '%s, %s, %s' % (
        system.coordinates.x, system.coordinates.y, system.coordinates.z)
    system.timestamp = int(datetime.strptime(
        edsm_dict.get('date'), 
        '%Y-%m-%d %H:%M:%S').timestamp()
    )

    return system
  # if file_type == 'bodies':
  #   formatted_json = format_bodies(edsm_json)
  # elif file_type == 'population':
  #   formatted_json = format_population(edsm_json)
  # elif file_type == 'powerplay':
  #   formatted_json = format_powerplay(edsm_json)
  # elif file_type == 'stations':
  #   formatted_json = format_stations(edsm_json)
  # elif file_type == 'systems':
  #   formatted_json = format_systems(edsm_json)
  # else:
  #   logging.error('Unsupported input file type.')
  #   formatted_json = None


def extract_json(raw_input: str):
  json_re_match = JSON_RE_SEARCH(raw_input)
  if json_re_match:
    json_string = json_re_match.group(1)
    return json_string
