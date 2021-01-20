import 'package:flutter/material.dart';
import 'package:raxxla/pages/planets.dart';
import 'package:raxxla/pages/settlements.dart';
import 'package:raxxla/pages/ships.dart';
import 'package:raxxla/pages/stations.dart';
import 'package:raxxla/pages/systems.dart';

void main() {
  runApp(RaxxlaApp());
}

class RaxxlaApp extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      initialRoute: '/',
      routes: {
        '/': (context) => SystemsPage(),
        '/planets': (context) => PlanetsPage(),
        '/settlements': (context) => SettlementsPage(),
        '/ships': (context) => ShipsPage(),
        '/stations': (context) => StationsPage(),
        '/systems': (context) => SystemsPage(),
      },
      title: 'Universal Cartographics Database',
      theme: ThemeData.dark(),
    );
  }
}
