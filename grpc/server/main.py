import json
import os
import re
from concurrent import futures
from datetime import datetime
from pprint import pprint

import api_raxxla_pb2
import api_raxxla_pb2_grpc
import bodies_pb2
import grpc
import schema
import settlement_pb2
import society_pb2
import system_pb2
from absl import app, flags, logging
from commonlib.google.datastore import datastore

import utils

# Global vars
JSON_RE_PATTERN = re.compile(r'(\{.*\})')
JSON_RE_SEARCH = JSON_RE_PATTERN.search


def _extract_json(raw_input: str):
    json_re_match = JSON_RE_SEARCH(raw_input)
    if json_re_match:
        json_string = json_re_match.group(1)
        return json_string


class Converter:
    # def __init__(self, type, edsm_dict):
    def planet(self, edsm_dict):
        planet = bodies_pb2.Planet()
        utils.map_proto_fields(planet, schema.EDSM.get('planet'), edsm_dict)

        return planet

    def population(self, edsm_dict):
        population = society_pb2.Population()
        schema_mapping = utils.recursive_dict_get(schema.EDSM,
                                                  'population.base')
        utils.map_proto_fields(population, schema_mapping, edsm_dict)

        # Add faction data
        factions_edsm = edsm_dict.get('factions')
        if factions_edsm:
            controlling_faction = utils.recursive_dict_get(
                edsm_dict, 'controllingFaction.id')
            for faction_dict in factions_edsm:
                faction = population.factions.add()
                faction_schema_mapping = utils.recursive_dict_get(
                    schema.EDSM, 'population.faction')
                utils.map_proto_fields(faction, faction_schema_mapping,
                                       faction_dict)

                if faction.id == controlling_faction:
                    faction.controlling = True

                state_schema_mapping = utils.recursive_dict_get(
                    schema.EDSM, 'population.state')
                states_dict = {
                    k: faction_dict.get(v)
                    for k, v in state_schema_mapping.items()
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

    def powerplay(self, edsm_dict):
        powerplay = society_pb2.Powerplay()
        utils.map_proto_fields(powerplay, schema.EDSM.get('powerplay'),
                               edsm_dict)

        return powerplay

    def star(self, edsm_dict):
        star = bodies_pb2.Star()
        utils.map_proto_fields(star, schema.EDSM.get('star'), edsm_dict)

        return star

    def station(self, edsm_dict):
        station = settlement_pb2.Settlement()
        utils.map_proto_fields(station, schema.EDSM.get('station'), edsm_dict)

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
    
    def system(self, edsm_dict):
        system = system_pb2.System()
        utils.map_proto_fields(system, scschema.EDSM.get('system')hema_mapping, edsm_dict)

        return system


class Raxxla(api_raxxla_pb2_grpc.RaxxlaServicer):
    def ConvertEdsm(self, request, context):
        response = api_raxxla_pb2.ConverterResponse()
        response.type = request.type

        try:
            edsm_dict = json.loads(request.json)
            schema_converter = Converter()
            print(dir(schema_converter))
            # edsm_proto = schema_converter.getattr(request.type)
            response.code = 0
            return response
        except json.decoder.JSONDecodeError:
            logging.error('Failed to parse JSON string: %s', request.json)
            response.code = 1
            return response


def main(argv):
    del argv

    default_port = os.environ.get('PORT')
    if not default_port:
        default_port = 50051
    logging.info('Starting Raxxla debug gRPC server at [::]:%s...',
                 default_port)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    api_raxxla_pb2_grpc.add_RaxxlaServicer_to_server(Raxxla(), server)
    server.add_insecure_port('[::]:%s' % default_port)
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    app.run(main)
