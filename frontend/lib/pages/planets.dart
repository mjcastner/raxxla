import 'package:flutter/material.dart';
import 'package:raxxla/pages/base.dart';
import 'package:raxxla/raxxla_grpc.dart';
import 'package:raxxla/protos/bodies.pb.dart';
import 'package:fixnum/fixnum.dart';
import 'package:raxxla/protos/api_raxxla.pbgrpc.dart';
import 'package:raxxla/sol_example.dart';

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

class SystemMap extends StatelessWidget {
  var systemChildren;
  var systemMapWidgets;

  SystemMap(SystemChildrenResponse this.systemChildren) {
    this.systemMapWidgets = this.buildSystemMap(this.systemChildren);
  }

  List findDirectChildren(childrenPb, parentPb, String parentType) {
    List directChildren = [];
    for (var child in childrenPb) {
      for (var parent in child.parents) {
        if (child.parents.length == 1) {
          if (parent.type == parentType &&
              parent.relativeId == parentPb.relativeId) {
            directChildren.add(child);
          }
        }
      }
    }
    return directChildren;
  }

  Tooltip buildBodyContainer(bodyPb, double height, double width, Color color) {
    var bodyContainer = Tooltip(
      message: bodyPb.name,
      child: Container(
        height: height,
        width: width,
        color: color,
      ),
    );
    return bodyContainer;
  }

  List<Row> buildSystemMap(systemChildren) {
    List<Row> starRows = [];
    for (var star in systemChildren.star) {
      List<Tooltip> starRowContainers = [];
      Tooltip starContainer = buildBodyContainer(star, 100, 100, Colors.red);
      starRowContainers.add(starContainer);

      List childPlanets =
          findDirectChildren(systemChildren.planet, star, 'Star');
      childPlanets.forEach((planet) => starRowContainers
          .add(buildBodyContainer(planet, 25, 25, Colors.blue)));

      starRows.add(
        Row(
          mainAxisAlignment: MainAxisAlignment.spaceEvenly,
          children: starRowContainers,
        ),
      );
    }

    return starRows;
  }

  @override
  Widget build(BuildContext context) {
    return Column(
      mainAxisAlignment: MainAxisAlignment.spaceEvenly,
      children: this.systemMapWidgets,
    );
  }
}

class PlanetsPage extends StatelessWidget {
  var systemId;
  var planets;

  PlanetsPage(Int64 this.systemId) {
    // this.planets = raxxlaStub.getSystemChildren(GetRequest(
    //   id: systemId,
    // ));

    this.planets = solChildrenFuture();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(),
      drawer: NavDrawer(),
      body: Center(
        child: FutureBuilder(
          future: planets,
          builder: (context, AsyncSnapshot<SystemChildrenResponse> snapshot) {
            if (snapshot.hasData) {
              var systemChildrenData = snapshot.data;
              var systemMapWidget = SystemMap(systemChildrenData);
              return Center(
                child: systemMapWidget,
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
