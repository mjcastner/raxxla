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
import society_pb2


class Raxxla(api_raxxla_pb2_grpc.RaxxlaServicer):
    def ConvertPlanetJson(self, request, context):
        print('Server received:\n')
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
        pprint(edsm_dict)
        planet = bodies_pb2.Planet()
        utils.map_proto_fields(planet, schema_mapping, edsm_dict)

        return api_raxxla_pb2.ConvertPlanetReply(message=planet)

    def ConvertPowerplayJson(self, request, context):
        print('Server received:\n')
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
        pprint(edsm_dict)
        powerplay = society_pb2.Powerplay()
        utils.map_proto_fields(powerplay, schema_mapping, edsm_dict)

        return api_raxxla_pb2.ConvertPowerplayReply(message=powerplay)

    def ConvertStarJson(self, request, context):
        print('Server received:\n')
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
        pprint(edsm_dict)
        star = bodies_pb2.Star()
        utils.map_proto_fields(star, schema_mapping, edsm_dict)

        return api_raxxla_pb2.ConvertStarReply(message=star)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    api_raxxla_pb2_grpc.add_RaxxlaServicer_to_server(Raxxla(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()