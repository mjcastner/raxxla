import 'package:flutter/material.dart';
import 'package:raxxla/pages/base.dart';
import 'package:raxxla/bigquery.dart';

class SystemsPage extends StatelessWidget {
  final test = getBigQueryTables();

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(),
      drawer: NavDrawer(),
      body: Center(child: Text('Systems')),
    );
  }
}
