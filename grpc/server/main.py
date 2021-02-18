import json
import os
import re
from concurrent import futures
from datetime import datetime
from pprint import pprint
from threading import Thread

import api_raxxla_pb2
import api_raxxla_pb2_grpc
import bodies_pb2
import grpc
import settlement_pb2
import society_pb2
import system_pb2
from absl import app, flags, logging
from commonlib import utils
from commonlib.google.datastore import datastore

import schema

WORKER_POOL = futures.ThreadPoolExecutor(max_workers=10)


class Raxxla(api_raxxla_pb2_grpc.RaxxlaServicer):
    def ConvertPlanet(self, request, context):
        try:
            edsm_object = schema.EdsmObject('planet', request.json)
            return edsm_object.generate_proto()
        except Exception as e:
            logging.error(e)
            return

    def ConvertPowerplay(self, request, context):
        try:
            edsm_object = schema.EdsmObject('powerplay', request.json)
            return edsm_object.generate_proto()
        except Exception as e:
            logging.error(e)
            return

    def ConvertStar(self, request, context):
        try:
            edsm_object = schema.EdsmObject('star', request.json)
            return edsm_object.generate_proto()
        except Exception as e:
            logging.error(e)
            return

def main(argv):
    del argv

    default_port = os.environ.get('PORT')
    if not default_port:
        default_port = 50051
    logging.info('Starting Raxxla debug gRPC server at [::]:%s...',
                 default_port)
    server = grpc.server(WORKER_POOL)
    api_raxxla_pb2_grpc.add_RaxxlaServicer_to_server(Raxxla(), server)
    server.add_insecure_port('[::]:%s' % default_port)
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    app.run(main)
