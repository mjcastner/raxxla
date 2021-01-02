import json
import re
from datetime import datetime

from protos import bodies_pb2
from protos import settlement_pb2
from protos import society_pb2
from protos import system_pb2

from absl import logging
from google.protobuf.json_format import MessageToJson

# Global vars
JSON_RE_PATTERN = re.compile(r'(\{.*\})')
JSON_RE_SEARCH = JSON_RE_PATTERN.search


def edsm_json_to_proto(file_type: str, edsm_json: str):
    edsm_dict = json.loads(edsm_json)

    def _format_composition(input_composition: dict):
        output_composition = []
        for element, percentage in input_composition.items():
            composition = bodies_pb2.Composition()
            composition.type = element
            composition.percentage = percentage
            output_composition.append(composition)

        return output_composition

    def _format_parents(input_parents: list):
        output_parents = []
        for input_parent in input_parents:
            for parent_type, parent_relative_id in input_parent.items():
                if parent_type and parent_relative_id:
                    parent = bodies_pb2.Parent()
                    parent.type = parent_type
                    parent.relative_id = parent_relative_id
                    output_parents.append(parent)

        return output_parents

    def _format_ringlike(input_rings: list):
        output_rings = []
        for input_ring in input_rings:
            ring = bodies_pb2.Ringlike()
            ring.name = input_ring.get('name')
            ring.type = input_ring.get('type')
            ring.mass = input_ring.get('mass')
            ring.inner_radius = input_ring.get('innerRadius')
            ring.outer_radius = input_ring.get('outerRadius')
            output_rings.append(ring)

        return output_rings

    if file_type == 'bodies':
        if edsm_dict.get('type') == 'Star':
            star = bodies_pb2.Star()
            star.id = edsm_dict.get('id64') if edsm_dict.get(
                'id64') else edsm_dict.get('id')
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
            star.updated = int(
                datetime.strptime(edsm_dict.get('updateTime'),
                                  '%Y-%m-%d %H:%M:%S').timestamp())

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
            if edsm_dict.get('rings'):
                rings = _format_ringlike(edsm_dict.get('rings'))
                star.rings.extend(rings)
            if edsm_dict.get('belts'):
                belts = _format_ringlike(edsm_dict.get('belts'))
                star.belts.extend(belts)
            if edsm_dict.get('parents'):
                parents = _format_parents(edsm_dict.get('parents'))
                star.parents.extend(parents)

            return MessageToJson(star, indent=0)
        elif edsm_dict.get('type') == 'Planet':
            planet = bodies_pb2.Planet()
            planet.id = edsm_dict.get('id64') if edsm_dict.get(
                'id64') else edsm_dict.get('id')
            planet.system_id = edsm_dict.get('systemId64')
            planet.name = edsm_dict.get('name')
            planet.metadata.type = edsm_dict.get('subType')
            planet.metadata.distance = edsm_dict.get('distanceToArrival')
            planet.metadata.mass = edsm_dict.get('earthMasses')
            planet.metadata.landable = edsm_dict.get('isLandable')
            planet.metadata.radius = edsm_dict.get('radius')
            planet.metadata.temperature = edsm_dict.get('surfaceTemperature')
            planet.orbit.period = edsm_dict.get('rotationalPeriod')
            planet.orbit.tidally_locked = edsm_dict.get(
                'rotationalPeriodTidallyLocked')
            planet.updated = int(
                datetime.strptime(edsm_dict.get('updateTime'),
                                  '%Y-%m-%d %H:%M:%S').timestamp())

            # Optional fields
            if edsm_dict.get('bodyId'):
                planet.relative_id = edsm_dict.get('bodyId')
            if edsm_dict.get('gravity'):
                planet.metadata.gravity = edsm_dict.get('gravity')
            if edsm_dict.get('surfacePressure'):
                planet.metadata.pressure = edsm_dict.get('surfacePressure')
            if edsm_dict.get('volcanismType'):
                planet.metadata.volcanism = edsm_dict.get('volcanismType')
            if edsm_dict.get('terraformingState'):
                planet.metadata.terraforming = edsm_dict.get(
                    'terraformingState')
            if edsm_dict.get('atmosphereType') and edsm_dict.get(
                    'atmosphereComposition'):
                planet.atmosphere.type = edsm_dict.get('atmosphereType')
                atmosphere_composition = _format_composition(
                    edsm_dict.get('atmosphereComposition'))
                planet.atmosphere.composition.extend(atmosphere_composition)
            if edsm_dict.get('solidComposition'):
                composition = _format_composition(
                    edsm_dict.get('solidComposition'))
                planet.composition.extend(composition)
            if edsm_dict.get('materials'):
                materials = _format_composition(edsm_dict.get('materials'))
                planet.materials.extend(materials)
            if edsm_dict.get('rings'):
                rings = _format_ringlike(edsm_dict.get('rings'))
                planet.rings.extend(rings)
            if edsm_dict.get('belts'):
                belts = _format_ringlike(edsm_dict.get('belts'))
                planet.belts.extend(belts)
            if edsm_dict.get('parents'):
                parents = _format_parents(edsm_dict.get('parents'))
                planet.parents.extend(parents)
            if edsm_dict.get('axialTilt'):
                planet.orbit.axial_tilt = edsm_dict.get('axialTilt')
            if edsm_dict.get('semiMajorAxis'):
                planet.orbit.semimajor_axis = edsm_dict.get('semiMajorAxis')
            if edsm_dict.get('orbitalInclination'):
                planet.orbit.inclination = edsm_dict.get('orbitalInclination')
            if edsm_dict.get('orbitalEccentricity'):
                planet.orbit.eccentricity = edsm_dict.get(
                    'orbitalEccentricity')
            if edsm_dict.get('orbitalPeriod'):
                planet.orbit.period = edsm_dict.get('orbitalPeriod')
            if edsm_dict.get('argOfPeriapsis'):
                planet.orbit.periapsis = edsm_dict.get('argOfPeriapsis')

            return MessageToJson(planet, indent=0)
        else:
            logging.error('Unsupported body type: %s', edsm_dict.get('type'))
            return

    elif file_type == 'population':
        population = society_pb2.Population()
        population.planet_id = edsm_dict.get('id64')
        population.security = edsm_dict.get('security')
        population.updated = int(
            datetime.strptime(edsm_dict.get('date'),
                              '%Y-%m-%d %H:%M:%S').timestamp())

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

                for recovering_state_dict in faction_dict.get(
                        'recoveringStates'):
                    recovering_state = faction.states.add()
                    recovering_state.name = recovering_state_dict.get('state')
                    recovering_state.type = 'Recovering'

        return MessageToJson(population, indent=0)

    elif file_type == 'powerplay':
        powerplay = society_pb2.Powerplay()
        powerplay.system_id = edsm_dict.get('id64')
        powerplay.power.name = edsm_dict.get('power')
        powerplay.power.state = edsm_dict.get('powerState')
        powerplay.updated = int(
            datetime.strptime(edsm_dict.get('date'),
                              '%Y-%m-%d %H:%M:%S').timestamp())

        # Optional fields
        if edsm_dict.get('allegiance'):
            powerplay.allegiance = edsm_dict.get('allegiance')
        if edsm_dict.get('government'):
            powerplay.government = edsm_dict.get('government')
        if edsm_dict.get('state'):
            powerplay.state = edsm_dict.get('state')

        return MessageToJson(powerplay, indent=0)

    elif file_type == 'stations':
        settlement = settlement_pb2.Settlement()
        settlement.id = edsm_dict.get('id')
        settlement.system_id = edsm_dict.get('systemId64')
        settlement.name = edsm_dict.get('name')
        settlement.metadata.type = edsm_dict.get('type')
        settlement.services.market = edsm_dict.get('haveMarket')
        settlement.services.shipyard = edsm_dict.get('haveShipyard')
        settlement.services.outfitting = edsm_dict.get('haveOutfitting')
        settlement.services.other.extend(edsm_dict.get('otherServices'))
        settlement.updated = int(
            datetime.strptime(
                edsm_dict.get('updateTime', {}).get('information'),
                '%Y-%m-%d %H:%M:%S').timestamp())

        # Optional fields
        if edsm_dict.get('distanceToArrival'):
            settlement.metadata.distance = edsm_dict.get('distanceToArrival')
        if edsm_dict.get('allegiance'):
            settlement.metadata.allegiance = edsm_dict.get('allegiance')
        if edsm_dict.get('controllingFaction', {}).get('id'):
            settlement.metadata.controlling_faction = edsm_dict.get(
                'controllingFaction', {}).get('id')
        if edsm_dict.get('government'):
            settlement.metadata.government = edsm_dict.get('government')
        if edsm_dict.get('marketId'):
            settlement.economy.id = edsm_dict.get('marketId')
        if edsm_dict.get('economy'):
            settlement.economy.type = edsm_dict.get('economy')
        if edsm_dict.get('secondEconomy'):
            settlement.economy.sub_type = edsm_dict.get('secondEconomy')
        if edsm_dict.get('body', {}).get('id'):
            settlement.parent.id = edsm_dict.get('body', {}).get('id')
        if edsm_dict.get('body', {}).get('name'):
            settlement.parent.name = edsm_dict.get('body', {}).get('name')
        if edsm_dict.get('body', {}).get('latitude'):
            settlement.parent.latitude = edsm_dict.get('body',
                                                       {}).get('latitude')
        if edsm_dict.get('body', {}).get('longitude'):
            settlement.parent.longitude = edsm_dict.get('body',
                                                        {}).get('longitude')

        return MessageToJson(settlement, indent=0)

    elif file_type == 'systems':
        system = system_pb2.System()
        system.id = edsm_dict.get('id64')
        system.name = edsm_dict.get('name')
        system.coordinates.x = edsm_dict.get('coords', {}).get('x')
        system.coordinates.y = edsm_dict.get('coords', {}).get('y')
        system.coordinates.z = edsm_dict.get('coords', {}).get('z')
        system.coordinates.coordinates = '%s, %s, %s' % (
            system.coordinates.x, system.coordinates.y, system.coordinates.z)
        system.timestamp = int(
            datetime.strptime(edsm_dict.get('date'),
                              '%Y-%m-%d %H:%M:%S').timestamp())

        return MessageToJson(system, indent=0)

    else:
        logging.error('Unsupported input file type.')
        return


def extract_json(raw_input: str):
    json_re_match = JSON_RE_SEARCH(raw_input)
    if json_re_match:
        json_string = json_re_match.group(1)
        return json_string
