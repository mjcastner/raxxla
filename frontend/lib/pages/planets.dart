import 'package:flutter/material.dart';
import 'package:raxxla/pages/base.dart';
import 'package:raxxla/protos/bodies.pb.dart';
import 'package:fixnum/fixnum.dart';

class PlanetsPage extends StatelessWidget {
  var systemId;
  var stars = [
    Star(
      name: 'Star 1',
      relativeId: 0,
    ),
    Star(
      name: 'Star 2',
      relativeId: 1,
    ),
    Star(
      name: 'Star 3',
      relativeId: 2,
    ),
  ];
  var planets = [
    Planet(
      name: 'Planet 1A',
      parents: [
        Parent(
          relativeId: 0,
          type: 'Star',
        ),
      ],
    ),
    Planet(
      name: 'Planet 1A 1',
      parents: [
        Parent(
          relativeId: 0,
          type: 'Star',
        ),
        Parent(
          relativeId: 0,
          type: 'Planet',
        ),
      ],
    ),
    Planet(
      name: 'Planet 1B',
      parents: [
        Parent(
          relativeId: 0,
          type: 'Star',
        ),
      ],
    ),
  ];

  PlanetsPage(Int64 this.systemId);

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(),
      drawer: NavDrawer(),
      body: Center(child: Text(systemId.toString())),
    );
  }
}
