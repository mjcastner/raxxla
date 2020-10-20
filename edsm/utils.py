import json
import re
import time
from datetime import datetime
from pprint import pprint

from proto import bodies_pb2
from proto import society_pb2
from proto import system_pb2

from absl import logging

# Global vars
JSON_RE_PATTERN = re.compile(r'(\{.*\})')
JSON_RE_SEARCH = JSON_RE_PATTERN.search


def edsm_json_to_proto(file_type: str, edsm_json: str):
  edsm_dict = json.loads(edsm_json)

  if file_type == 'bodies':
    if edsm_dict.get('type') == 'Star':
      star = bodies_pb2.Star()
      star.id = edsm_dict.get('id64') if edsm_dict.get('id64') else edsm_dict.get('id')
      star.system_id = edsm_dict.get('systemId64')
      star.name = edsm_dict.get('name')
      star.metadata.type = edsm_dict.get('subType')
      star.metadata.distance = edsm_dict.get('distanceToArrival')
      star.metadata.solar_masses = edsm_dict.get('solarMasses')
      star.metadata.solar_radius = edsm_dict.get('solarRadius')
      star.metadata.temperature = edsm_dict.get('surfaceTemperature')
      star.orbit.period = edsm_dict.get('rotationalPeriod')
      star.orbit.tidally_locked = edsm_dict.get(
          'rotationalPeriodTidallyLocked')
      star.updated = int(datetime.strptime(
        edsm_dict.get('updateTime'), 
        '%Y-%m-%d %H:%M:%S').timestamp()
      )

      # Optional fields
      if edsm_dict.get('axialTilt'):
        star.orbit.axial_tilt = edsm_dict.get('axialTilt')
      if edsm_dict.get('semiMajorAxis'):
        star.orbit.semimajor_axis = edsm_dict.get('semiMajorAxis')
      if edsm_dict.get('orbitalInclination'):
        star.orbit.inclination = edsm_dict.get('orbitalInclination')
      if edsm_dict.get('orbitalEccentricity'):
        star.orbit.eccentricity = edsm_dict.get('orbitalEccentricity')
      if edsm_dict.get('orbitalPeriod'):
        star.orbit.period = edsm_dict.get('orbitalPeriod')
      if edsm_dict.get('argOfPeriapsis'):
        star.orbit.periapsis = edsm_dict.get('argOfPeriapsis')
      if edsm_dict.get('bodyId'):
        star.relative_id = edsm_dict.get('bodyId')
      if edsm_dict.get('luminosity'):
        star.metadata.luminosity = edsm_dict.get('luminosity')
      if edsm_dict.get('spectralClass'):
        star.metadata.spectral_class = edsm_dict.get('spectralClass')
      if edsm_dict.get('reserveLevel'):
        star.metadata.reserve_level = edsm_dict.get('reserveLevel')

      rings = edsm_dict.get('rings')
      if rings:
        for ring_dict in rings:
          ring = star.rings.add()
          ring.name = ring_dict.get('name')
          ring.type = ring_dict.get('type')
          ring.mass = ring_dict.get('mass')
          ring.inner_radius = ring_dict.get('innerRadius')
          ring.outer_radius = ring_dict.get('outerRadius')
        
      belts = edsm_dict.get('belts')
      if belts:
        for belt_dict in belts:
          belt = star.belts.add()
          belt.name = belt_dict.get('name')
          belt.type = belt_dict.get('type')
          belt.mass = belt_dict.get('mass')
          belt.inner_radius = belt_dict.get('innerRadius')
          belt.outer_radius = belt_dict.get('outerRadius')
          
      parents = edsm_dict.get('parents')
      if parents:
        for parent_dict in parents:
          for parent_type, parent_relative_id in parent_dict.items():
            if parent_type and parent_relative_id:
              parent = star.parents.add()
              parent.type = parent_type if parent_type != "Null" else "Default"
              parent.relative_id = parent_relative_id

      return star
    elif edsm_dict.get('type') == 'Planet':
      # print('Planet')
      return
    else:
      # print('Unsupported type')
      return

  elif file_type == 'population':
    population = society_pb2.Population()
    population.planet_id = edsm_dict.get('id64')
    population.security = edsm_dict.get('security')
    updated_seconds = int(datetime.strptime(
        edsm_dict.get('date'), 
        '%Y-%m-%d %H:%M:%S').timestamp()
    )
    population.updated.FromSeconds(updated_seconds)

    # Optional fields
    if edsm_dict.get('allegiance'):
      population.allegiance = edsm_dict.get('allegiance')
    if edsm_dict.get('economy'):
      population.economy = edsm_dict.get('economy')
    if edsm_dict.get('government'):
      population.government = edsm_dict.get('government')
    if edsm_dict.get('population'):
      population.population = edsm_dict.get('population')
    if edsm_dict.get('state'):
      population.state = edsm_dict.get('state')

    # Factions
    factions = edsm_dict.get('factions')
    if factions:
      for faction_dict in factions:
        faction = population.factions.add()
        faction.id = faction_dict.get('id')
        faction.name = faction_dict.get('name')
        faction.influence = faction_dict.get('influence')
        faction.happiness = faction_dict.get('happiness')
        faction.player_faction = faction_dict.get('isPlayer')

        # Optional fields
        if faction_dict.get('allegiance'):
          faction.allegiance = faction_dict.get('allegiance')
        if faction_dict.get('government'):
          faction.government = faction_dict.get('government')

        # Controlling faction
        controlling_faction_id = faction_dict.get(
            'controllingFaction', {}).get('id')
        if controlling_faction_id == faction_dict.get('id'):
          faction.controlling = True
        else:
          faction.controlling = False

        # States
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
    powerplay.system_id = edsm_dict.get('id64')
    powerplay.power.name = edsm_dict.get('power')
    powerplay.power.state = edsm_dict.get('powerState')
    updated_seconds = int(datetime.strptime(
        edsm_dict.get('date'), 
        '%Y-%m-%d %H:%M:%S').timestamp()
    )
    powerplay.updated.FromSeconds(updated_seconds)

    # Optional fields
    if edsm_dict.get('allegiance'):
      powerplay.allegiance = edsm_dict.get('allegiance')
    if edsm_dict.get('government'):
      powerplay.government = edsm_dict.get('government')
    if edsm_dict.get('state'):
      powerplay.state = edsm_dict.get('state')

    return powerplay

  elif file_type == 'systems':
    system = system_pb2.System()
    system.id = edsm_dict.get('id64')
    system.name = edsm_dict.get('name')
    system.coordinates.x = edsm_dict.get('coords', {}).get('x')
    system.coordinates.y = edsm_dict.get('coords', {}).get('y')
    system.coordinates.z = edsm_dict.get('coords', {}).get('z')
    system.coordinates.coordinates = '%s, %s, %s' % (
        system.coordinates.x, system.coordinates.y, system.coordinates.z)
    discovered_seconds = int(datetime.strptime(
        edsm_dict.get('date'), 
        '%Y-%m-%d %H:%M:%S').timestamp()
    )
    system.timestamp.FromSeconds(discovered_seconds)

    return system

  else:
    logging.error('Unsupported input file type.')
    return
  # elif file_type == 'stations':
  #   formatted_json = format_stations(edsm_json)


def extract_json(raw_input: str):
  json_re_match = JSON_RE_SEARCH(raw_input)
  if json_re_match:
    json_string = json_re_match.group(1)
    return json_string