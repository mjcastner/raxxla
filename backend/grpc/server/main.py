import json
import os
import re
from concurrent import futures
from datetime import datetime

import grpc
from absl import app, flags, logging
from commonlib import utils
from commonlib.google.datastore import datastore
from protos import (api_raxxla_pb2, api_raxxla_pb2_grpc, bodies_pb2,
                    settlement_pb2, society_pb2, system_pb2)

WORKER_POOL = futures.ThreadPoolExecutor(max_workers=10)


class Raxxla(api_raxxla_pb2_grpc.RaxxlaServicer):
    def GetPlanet(self, request, context):
        edsm_object = bodies_pb2.Planet()
        return edsm_object

    def GetPowerplay(self, request, context):
        edsm_object = society_pb2.Powerplay()
        return edsm_object


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
