import 'dart:async';
import 'package:fixnum/fixnum.dart';
import 'package:grpc/grpc_web.dart';
import 'package:raxxla/protos/api_raxxla.pbgrpc.dart';
import 'package:raxxla/protos/system.pb.dart';

final channel = GrpcWebClientChannel.xhr(
  Uri.https("raxxla-grpc-endpoint-664m6lgixa-uw.a.run.app", "/"),
);
final raxxlaStub = RaxxlaClient(
  channel,
  options: CallOptions(
    timeout: Duration(seconds: 30),
  ),
);

Future<System> getSystemData(Int64 systemId) async {
  return raxxlaStub.getSystem(GetRequest(id: systemId));
}
