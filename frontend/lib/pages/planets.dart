import 'package:flutter/material.dart';
import 'package:raxxla/pages/base.dart';
import 'package:raxxla/raxxla_grpc.dart';
import 'package:raxxla/protos/bodies.pb.dart';
import 'package:fixnum/fixnum.dart';
import 'package:raxxla/protos/api_raxxla.pbgrpc.dart';

RaxxlaClient raxxlaStub = initRaxxlaClient();

class PlanetWidget extends StatelessWidget {
  var planetData;

  PlanetWidget(Planet this.planetData);

  @override
  Widget build(BuildContext context) {
    return Column(
      children: [
        Tooltip(
          message: planetData.name,
          child: Icon(Icons.stop_circle),
        )
      ],
    );
  }
}

class PlanetsPage extends StatelessWidget {
  var systemId;
  var planets;

  PlanetsPage(Int64 this.systemId) {
    this.planets = raxxlaStub.getPlanetsInSystem(GetRequest(
      id: systemId,
    ));
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(),
      drawer: NavDrawer(),
      body: Center(
        child: FutureBuilder(
          future: planets,
          builder: (context, AsyncSnapshot<PlanetResponse> snapshot) {
            if (snapshot.hasData) {
              var planetData = snapshot.data;
              var planetWidgets =
                  planetData.planet.map((x) => PlanetWidget(x)).toList();
              return Container(
                child: Row(
                  mainAxisAlignment: MainAxisAlignment.spaceEvenly,
                  children: planetWidgets,
                ),
              );
            }
            if (snapshot.hasError) {
              return Text('Failed!');
            } else {
              return Container(
                decoration: BoxDecoration(
                  image: DecorationImage(
                    image: AssetImage('images/loading.gif'),
                    fit: BoxFit.cover,
                  ),
                ),
              );
            }
          },
        ),
      ),
    );
  }
}
