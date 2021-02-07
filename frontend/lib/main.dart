import 'package:flutter/material.dart';
import 'package:raxxla/pages/factions.dart';
import 'package:raxxla/pages/guardians.dart';
import 'package:raxxla/pages/planets.dart';
import 'package:raxxla/pages/population.dart';
import 'package:raxxla/pages/powers.dart';
import 'package:raxxla/pages/settlements.dart';
import 'package:raxxla/pages/ships.dart';
import 'package:raxxla/pages/stations.dart';
import 'package:raxxla/pages/systems.dart';
import 'package:raxxla/pages/thargoids.dart';
import 'package:fixnum/fixnum.dart';

// Add a live map page using gRPC streams
// ZeroMQ source: https://github.com/EDCD/EDDN/wiki
Int64 systemId = Int64(1732851569378);

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
        '/factions': (context) => FactionsPage(),
        '/guardians': (context) => GuardiansPage(),
        '/planets': (context) => PlanetsPage(systemId),
        '/population': (context) => PopulationPage(),
        '/powers': (context) => PowersPage(),
        '/settlements': (context) => SettlementsPage(),
        '/ships': (context) => ShipsPage(),
        '/stations': (context) => StationsPage(),
        '/systems': (context) => SystemsPage(),
        '/thargoids': (context) => ThargoidsPage(),
      },
      title: 'Universal Cartographics Database',
      theme: ThemeData.dark(),
    );
  }
}
