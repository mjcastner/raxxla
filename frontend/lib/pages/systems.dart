import 'package:flutter/material.dart';
import 'package:google_fonts/google_fonts.dart';
import 'package:raxxla/pages/base.dart';
import 'package:raxxla/pages/planets.dart';
import 'package:raxxla/protos/api_raxxla.pbgrpc.dart';
import 'package:raxxla/protos/system.pb.dart';
import 'package:raxxla/sol_example.dart';
import 'package:raxxla/raxxla_grpc.dart';
import 'package:fixnum/fixnum.dart';
import 'package:fab_circular_menu/fab_circular_menu.dart';

Int64 systemId = Int64(1732851569378);
// Int64 systemId = Int64(1);
RaxxlaClient raxxlaStub = initRaxxlaClient();

class SystemsPage extends StatelessWidget {
  // final system = raxxlaStub.getSystem(GetRequest(id: systemId));
  final system = solFuture();

  @override
  Widget build(BuildContext context) {
    double screenHeight = MediaQuery.of(context).size.height;
    double screenWidth = MediaQuery.of(context).size.width;

    return Scaffold(
      appBar: AppBar(),
      drawer: NavDrawer(),
      floatingActionButton: FabCircularMenu(
        fabOpenIcon: Icon(Icons.add),
        ringColor: Colors.orange,
        children: [
          FlatButton.icon(
            onPressed: () {},
            icon: Icon(Icons.home),
            label: Text('Overview'),
          ),
          FlatButton.icon(
            onPressed: () {
              Navigator.push(
                context,
                MaterialPageRoute(builder: (context) => PlanetsPage(systemId)),
              );
            },
            icon: Icon(Icons.home),
            label: Text('System Map'),
          ),
          FlatButton.icon(
            onPressed: () {},
            icon: Icon(Icons.home),
            label: Text('Traffic'),
          ),
        ],
      ),
      body: Center(
        child: FutureBuilder(
          future: system,
          builder: (context, AsyncSnapshot<System> snapshot) {
            if (snapshot.hasData) {
              var systemData = snapshot.data;
              var coords =
                  '${systemData.coordinates.x}, ${systemData.coordinates.y}, ${systemData.coordinates.z}';
              DateTime systemUpdated = new DateTime.fromMillisecondsSinceEpoch(
                  (systemData.timestamp.toInt() * 1000));
              return Container(
                child: Column(
                  children: [
                    Row(
                      children: [
                        Container(
                          padding: EdgeInsets.fromLTRB(10, 10, 0, 0),
                          child: Text(
                            systemData.name,
                            style: GoogleFonts.roboto(fontSize: 24),
                          ),
                        ),
                      ],
                    ),
                    Row(
                      children: [
                        Container(
                          padding: EdgeInsets.fromLTRB(12, 5, 0, 0),
                          child: Text(
                            'Galactic coordinates: $coords',
                            style: GoogleFonts.roboto(fontSize: 12),
                          ),
                        ),
                      ],
                    ),
                    Row(
                      children: [
                        Container(
                          padding: EdgeInsets.fromLTRB(12, 5, 0, 0),
                          child: Text(
                            'Last updated: $systemUpdated UTC',
                            style: GoogleFonts.roboto(fontSize: 12),
                          ),
                        ),
                      ],
                    ),
                  ],
                ),
                decoration: BoxDecoration(
                  image: DecorationImage(
                    image: AssetImage('images/system/neutron.gif'),
                    fit: BoxFit.cover,
                  ),
                ),
              );
            }
            if (snapshot.hasError) {
              print(snapshot.error);
              return Container(
                child: Center(
                  child: Column(
                    mainAxisAlignment: MainAxisAlignment.center,
                    children: [
                      Text(
                        'System data unavailable',
                        style: GoogleFonts.roboto(
                          color: Colors.deepOrange,
                          fontSize: 48,
                        ),
                      )
                    ],
                  ),
                ),
                decoration: BoxDecoration(
                  image: DecorationImage(
                    image: AssetImage('images/system/error.gif'),
                    fit: BoxFit.cover,
                  ),
                ),
              );
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
