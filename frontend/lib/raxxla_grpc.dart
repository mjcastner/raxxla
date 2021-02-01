import 'package:grpc/grpc_web.dart';
import 'package:grpc/grpc.dart';
import 'package:raxxla/protos/api_raxxla.pb.dart';
import 'package:raxxla/protos/api_raxxla.pbgrpc.dart';
import 'package:raxxla/protos/system.pb.dart';
import 'package:fixnum/fixnum.dart';

Int64 systemId = Int64(111197315617635);

Future<System> getSystemData() async {
  final channel = GrpcWebClientChannel.xhr(
      Uri.parse('http://grpcserver-664m6lgixa-uw.a.run.app:443'));
  final service = RaxxlaClient(channel);
  var system = service.getSystem(GetRequest(id: systemId));
  return system;
}

// final channel = ClientChannel(
//   'grpcserver-664m6lgixa-uw.a.run.app',
//   port: 443,
//   options: ChannelOptions(
//     credentials: ChannelCredentials.secure(),
//     idleTimeout: Duration(seconds: 10),
//   ),
// );
// final stub = RaxxlaClient(channel);

// Future<System> getSystemData() async {
//   return await stub.getSystem(GetRequest(id: systemId));
// }
