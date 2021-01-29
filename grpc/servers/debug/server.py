import json
from concurrent import futures
from datetime import datetime
from pprint import pprint

import api_raxxla_pb2
import api_raxxla_pb2_grpc
import bodies_pb2
import grpc
import settlement_pb2
import society_pb2
import system_pb2
from absl import app, flags, logging
from commonlib.google.datastore import datastore

import utils

FLAGS = flags.FLAGS
flags.DEFINE_string('server_host', '0.0.0.0',
                    'Address to host Raxxla debug gRPC server.')
flags.DEFINE_string('server_port', '50051',
                    'Port for Raxxla debug gRPC server.')


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

    def GetPopulation(self, request, context):
        logging.info('Received request: %s', request)
        ds_client = datastore.create_client()
        population = society_pb2.Population()
        proto_bytes = datastore.get_proto_bytes(ds_client, 'population', request.id)
        population.ParseFromString(proto_bytes)
        return population

    def SetPopulation(self, request, context):
        logging.info('Received request: %s', request)
        ds_client = datastore.create_client()
        response = api_raxxla_pb2.SetResponse()
        try:
            datastore.set_proto_bytes(ds_client, 'population', request.id,
                                      request.population.SerializeToString())
            response.code = 1
            return response
        except Exception as e:
            logging.error(e)
            response.code = 0
            return response

    def GetPowerplay(self, request, context):
        logging.info('Received request: %s', request)
        ds_client = datastore.create_client()
        powerplay = society_pb2.Powerplay()
        proto_bytes = datastore.get_proto_bytes(ds_client, 'powerplay', request.id)
        powerplay.ParseFromString(proto_bytes)
        return powerplay

    def SetPowerplay(self, request, context):
        logging.info('Received request: %s', request)
        ds_client = datastore.create_client()
        response = api_raxxla_pb2.SetResponse()
        try:
            datastore.set_proto_bytes(ds_client, 'powerplay', request.id,
                                      request.powerplay.SerializeToString())
            response.code = 1
            return response
        except Exception as e:
            logging.error(e)
            response.code = 0
            return response

    def GetStar(self, request, context):
        logging.info('Received request: %s', request)
        ds_client = datastore.create_client()
        star = bodies_pb2.Star()
        proto_bytes = datastore.get_proto_bytes(ds_client, 'star', request.id)
        star.ParseFromString(proto_bytes)
        return star

    def SetStar(self, request, context):
        logging.info('Received request: %s', request)
        ds_client = datastore.create_client()
        response = api_raxxla_pb2.SetResponse()
        try:
            datastore.set_proto_bytes(ds_client, 'star', request.id,
                                      request.star.SerializeToString())
            response.code = 1
            return response
        except Exception as e:
            logging.error(e)
            response.code = 0
            return response

    def GetSettlement(self, request, context):
        logging.info('Received request: %s', request)
        ds_client = datastore.create_client()
        settlement = settlement_pb2.Settlement()
        proto_bytes = datastore.get_proto_bytes(ds_client, 'settlement', request.id)
        settlement.ParseFromString(proto_bytes)
        return settlement

    def SetSettlement(self, request, context):
        logging.info('Received request: %s', request)
        ds_client = datastore.create_client()
        response = api_raxxla_pb2.SetResponse()
        try:
            datastore.set_proto_bytes(ds_client, 'settlement', request.id,
                                      request.settlement.SerializeToString())
            response.code = 1
            return response
        except Exception as e:
            logging.error(e)
            response.code = 0
            return response
    
    def GetSystem(self, request, context):
        logging.info('Received request: %s', request)
        ds_client = datastore.create_client()
        system = system_pb2.System()
        proto_bytes = datastore.get_proto_bytes(ds_client, 'system', request.id)
        system.ParseFromString(proto_bytes)
        return system

    def SetSystem(self, request, context):
        logging.info('Received request: %s', request)
        ds_client = datastore.create_client()
        response = api_raxxla_pb2.SetResponse()
        try:
            datastore.set_proto_bytes(ds_client, 'system', request.id,
                                      request.system.SerializeToString())
            response.code = 1
            return response
        except Exception as e:
            logging.error(e)
            response.code = 0
            return response


def main(argv):
    del argv

    logging.info('Starting Raxxla debug gRPC server at %s:%s...',
                 FLAGS.server_host, FLAGS.server_port)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    api_raxxla_pb2_grpc.add_RaxxlaServicer_to_server(Raxxla(), server)
    server.add_insecure_port('%s:%s' % (FLAGS.server_host, FLAGS.server_port))
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    app.run(main)
