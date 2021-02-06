import 'package:grpc/grpc_web.dart';
import 'package:raxxla/protos/api_raxxla.pbgrpc.dart';

final String endpoint = 'raxxla-grpc-endpoint-664m6lgixa-uw.a.run.app';

RaxxlaClient initRaxxlaClient() {
  final channel = GrpcWebClientChannel.xhr(
    Uri.https(endpoint, "/"),
  );
  final raxxlaStub = RaxxlaClient(
    channel,
    options: CallOptions(
      timeout: Duration(seconds: 30),
    ),
  );
  return raxxlaStub;
}
