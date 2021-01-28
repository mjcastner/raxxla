from concurrent import futures
from datetime import datetime
from pprint import pprint
import json
import logging
import utils

import grpc

import api_raxxla_pb2
import api_raxxla_pb2_grpc
import bodies_pb2
import settlement_pb2
import society_pb2
import system_pb2


class Raxxla(api_raxxla_pb2_grpc.RaxxlaServicer):
    def ConvertPlanetJson(self, request, context):
        logging.info('Received request: %s', request)
        schema_mapping = {
            'id': 'id64',
            'system_id': 'systemId64',
            'relative_id': 'bodyId',
            'name': 'name',
            'atmosphere.type': 'atmosphereType',
            'metadata.type': 'subType',
            'metadata.distance': 'distanceToArrival',
            'metadata.mass': 'earthMasses',
            'metadata.gravity': 'gravity',
            'metadata.landable': 'isLandable',
            'metadata.radius': 'radius',
            'metadata.temperature': 'surfaceTemperature',
            'metadata.pressure': 'surfacePressure',
            'metadata.volcanism': 'volcanismType',
            'metadata.terraforming': 'terraformingState',
            'orbit.period': 'orbitalPeriod',
            'orbit.rotational_period': 'rotationalPeriod',
            'orbit.tidally_locked': 'rotationalPeriodTidallyLocked',
            'orbit.periapsis': 'argOfPeriapsis',
            'orbit.eccentricity': 'orbitalEccentricity',
            'orbit.inclination': 'orbitalInclination',
            'orbit.semimajor_axis': 'semiMajorAxis',
            'orbit.axial_tilt': 'axialTilt',
            'repeated_fields': {
                'atmosphere.composition': {
                    'key': 'atmosphereComposition',
                    'type': 'composition',
                },
                'belts': {
                    'key': 'belts',
                    'type': 'ringlike',
                },
                'composition': {
                    'key': 'solidComposition',
                    'type': 'composition',
                },
                'materials': {
                    'key': 'materials',
                    'type': 'composition',
                },
                'parents': {
                    'key': 'parents',
                    'type': 'parents',
                },
                'rings': {
                    'key': 'rings',
                    'type': 'ringlike',
                },
            },
            'timestamp_fields': {
                'updated': 'updateTime',
            },
        }
        edsm_dict = json.loads(request.json)
        planet = bodies_pb2.Planet()
        utils.map_proto_fields(planet, schema_mapping, edsm_dict)

        return planet

    def ConvertPopulationJson(self, request, context):
        logging.info('Received request: %s', request)
        schema_mapping = {
            'system_id': 'id64',
            'allegiance': 'allegiance',
            'government': 'government',
            'state': 'state',
            'economy': 'economy',
            'security': 'security',
            'population': 'population',
            'timestamp_fields': {
                'updated': 'date',
            },
        }
        edsm_dict = json.loads(request.json)
        population = society_pb2.Population()
        utils.map_proto_fields(population, schema_mapping, edsm_dict)

        # Grab faction data
        faction_mapping = {
            'id': 'id',
            'name': 'name',
            'allegiance': 'allegiance',
            'government': 'government',
            'influence': 'influence',
            'happiness': 'happiness',
            'player_faction': 'isPlayer',
        }
        state_mapping = {
            'active': 'activeStates',
            'pending': 'pendingStates',
            'recovering': 'recoveringStates',
        }
        factions_edsm = utils.recursive_dict_get(edsm_dict, 'factions')
        if factions_edsm:
            controlling_faction = utils.recursive_dict_get(
                edsm_dict, 'controllingFaction.id')
            for faction_dict in factions_edsm:
                faction = population.factions.add()
                utils.map_proto_fields(faction, faction_mapping, faction_dict)

                if faction.id == controlling_faction:
                    faction.controlling = True

                states_dict = {
                    k: faction_dict.get(v)
                    for k, v in state_mapping.items()
                }
                states = [{
                    'type': k,
                    'name': x.get('state')
                } for k, v in states_dict.items() for x in v]
                [
                    faction.states.add(type=x.get('type'), name=x.get('name'))
                    for x in states
                ]
        return population

    def ConvertPowerplayJson(self, request, context):
        logging.info('Received request: %s', request)
        schema_mapping = {
            'system_id': 'id64',
            'power.name': 'power',
            'power.state': 'powerState',
            'allegiance': 'allegiance',
            'government': 'government',
            'state': 'state',
            'timestamp_fields': {
                'updated': 'date',
            },
        }
        edsm_dict = json.loads(request.json)
        powerplay = society_pb2.Powerplay()
        utils.map_proto_fields(powerplay, schema_mapping, edsm_dict)

        return powerplay

    def ConvertStationJson(self, request, context):
        logging.info('Received request: %s', request)
        schema_mapping = {
            'id': 'id',
            'system_id': 'systemId64',
            'name': 'name',
            'metadata.type': 'type',
            'metadata.distance': 'distanceToArrival',
            'metadata.allegiance': 'allegiance',
            'metadata.controlling_faction': 'controllingFaction.id',
            'metadata.government': 'government',
            'economy.id': 'marketId',
            'economy.type': 'economy',
            'economy.sub_type': 'secondEconomy',
            'services.market': 'haveMarket',
            'services.shipyard': 'haveShipyard',
            'services.outfitting': 'haveOutfitting',
            'timestamp_fields': {
                'updated': 'updateTime.information',
            },
        }
        edsm_dict = json.loads(request.json)
        station = settlement_pb2.Settlement()
        utils.map_proto_fields(station, schema_mapping, edsm_dict)

        # Grab additional station data
        station.services.other.extend(
            utils.recursive_dict_get(edsm_dict, 'otherServices'))
        commodities_edsm = utils.recursive_dict_get(edsm_dict, 'commodities')
        if commodities_edsm:
            [
                station.services.commodities.add(
                    name=x.get('name'),
                    buy_price=x.get('buyPrice'),
                    sell_price=x.get('sellPrice'),
                    stock=x.get('stock'),
                    demand=x.get('demand'),
                ) for x in commodities_edsm
            ]

        parts_edsm = utils.recursive_dict_get(edsm_dict, 'outfitting')
        if parts_edsm:
            parts_dict = [
                utils.extract_quality(x.get('name')) for x in parts_edsm
            ]
            [
                station.services.ship_parts.add(name=x.get('name'),
                                                quality=x.get('quality'))
                for x in parts_dict
            ]

        ships_edsm = utils.recursive_dict_get(edsm_dict, 'ships')
        if ships_edsm:
            [station.services.ships.append(x.get('name')) for x in ships_edsm]

        return station

    def ConvertStarJson(self, request, context):
        logging.info('Received request: %s', request)
        schema_mapping = {
            'id': 'id64',
            'system_id': 'systemId64',
            'relative_id': 'bodyId',
            'name': 'name',
            'metadata.type': 'subType',
            'metadata.distance': 'distanceToArrival',
            'metadata.reserve_level': 'reserveLevel',
            'metadata.spectral_class': 'spectralClass',
            'metadata.solar_masses': 'solarMasses',
            'metadata.solar_radius': 'solarRadius',
            'metadata.luminosity': 'luminosity',
            'metadata.temperature': 'surfaceTemperature',
            'orbit.period': 'orbitalPeriod',
            'orbit.rotational_period': 'rotationalPeriod',
            'orbit.tidally_locked': 'rotationalPeriodTidallyLocked',
            'orbit.periapsis': 'argOfPeriapsis',
            'orbit.eccentricity': 'orbitalEccentricity',
            'orbit.inclination': 'orbitalInclination',
            'orbit.semimajor_axis': 'semiMajorAxis',
            'orbit.axial_tilt': 'axialTilt',
            'repeated_fields': {
                'belts': {
                    'key': 'belts',
                    'type': 'ringlike',
                },
                'parents': {
                    'key': 'parents',
                    'type': 'parents',
                },
                'rings': {
                    'key': 'rings',
                    'type': 'ringlike',
                },
            },
            'timestamp_fields': {
                'updated': 'updateTime',
            },
        }
        edsm_dict = json.loads(request.json)
        star = bodies_pb2.Star()
        utils.map_proto_fields(star, schema_mapping, edsm_dict)

        return star

    def ConvertSystemJson(self, request, context):
        logging.info('Received request: %s', request)
        schema_mapping = {
            'id': 'id64',
            'name': 'name',
            'coordinates.x': 'coords.x',
            'coordinates.y': 'coords.y',
            'coordinates.z': 'coords.z',
            'timestamp_fields': {
                'timestamp': 'date',
            },
        }
        edsm_dict = json.loads(request.json)
        system = system_pb2.System()
        utils.map_proto_fields(system, schema_mapping, edsm_dict)

        return system


def serve():
    logging.info('Starting Raxxla debug gRPC server...')
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    api_raxxla_pb2_grpc.add_RaxxlaServicer_to_server(Raxxla(), server)
    server.add_insecure_port('0.0.0.0:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()