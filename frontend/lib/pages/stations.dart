import 'package:flutter/material.dart';
import 'package:raxxla/pages/base.dart';

class StationsPage extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(),
      drawer: NavDrawer(),
      body: Center(child: Text('Stations')),
    );
  }
}
