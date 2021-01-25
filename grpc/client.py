from __future__ import print_function
import logging

import grpc

import api_raxxla_pb2
import api_raxxla_pb2_grpc

POWERPLAY_JSON = '''{"power":"Edmund Mahon","powerState":"Exploited","id":15453,"id64":6406111564522,"name":"Kitchosat","coords":{"x":21.90625,"y":64.8125,"z":107.625},"allegiance":"Independent","government":"Corporate","state":"None","date":"2020-12-24 04:17:51"}'''

PLANET_JSON = '''{"id":262289809,"id64":1405123461663602546,"bodyId":39,"name":"Byeia Auwsy JV-D c13-1 3","type":"Planet","subType":"Gas giant with ammonia-based life","parents":[{"Null":38},{"Null":37},{"Star":0}],"distanceToArrival":4183,"isLandable":false,"gravity":3.933318274373266,"earthMasses":545.528687,"radius":75112.768,"surfaceTemperature":143,"surfacePressure":0,"volcanismType":"No volcanism","atmosphereType":"No atmosphere","atmosphereComposition":{"Hydrogen":72.97,"Helium":26.99},"solidComposition":null,"terraformingState":"Not terraformable","orbitalPeriod":321.5753852769213,"semiMajorAxis":0.016215843348876623,"orbitalEccentricity":0.038608,"orbitalInclination":0.716835,"argOfPeriapsis":314.529009,"rotationalPeriod":1.622185318761574,"rotationalPeriodTidallyLocked":false,"axialTilt":0.249962,"rings":[{"name":"Byeia Auwsy JV-D c13-1 3 A Ring","type":"Rocky","mass":183190000000,"innerRadius":123940,"outerRadius":149260},{"name":"Byeia Auwsy JV-D c13-1 3 B Ring","type":"Icy","mass":1961600000000,"innerRadius":149360,"outerRadius":303120}],"reserveLevel":"Pristine","updateTime":"2020-12-08 12:18:01","systemId":59547254,"systemId64":377924007794,"systemName":"Byeia Auwsy JV-D c13-1"}'''

def run():
    responses = []
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = api_raxxla_pb2_grpc.RaxxlaStub(channel)
        planet_response = stub.ConvertPlanetJson(api_raxxla_pb2.ConvertEdsmRequest(json=PLANET_JSON))
        responses.append(planet_response)

        powerplay_response = stub.ConvertPowerplayJson(api_raxxla_pb2.ConvertEdsmRequest(json=POWERPLAY_JSON))
        responses.append(powerplay_response)

    print('Client received:\n')
    [print(x.message) for x in responses]


if __name__ == '__main__':
    logging.basicConfig()
    run()