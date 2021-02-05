import 'package:flutter/material.dart';
import 'package:raxxla/pages/base.dart';
import 'package:raxxla/protos/system.pb.dart';
import 'package:raxxla/raxxla_grpc.dart';
import 'package:fixnum/fixnum.dart';

Int64 systemId = Int64(1732851569378);

class SystemsPage extends StatelessWidget {
  final system = getSystemData(systemId);

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(),
      drawer: NavDrawer(),
      floatingActionButton: FloatingActionButton(
        onPressed: () {},
        backgroundColor: Colors.orange,
        foregroundColor: Colors.white,
        child: Icon(Icons.add),
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
                        Card(
                          margin: EdgeInsets.fromLTRB(10, 0, 0, 10),
                          elevation: 2,
                          child: Container(
                            height: 200,
                            width: 250,
                          ),
                        ),
                      ],
                    ),
                  ],
                  mainAxisAlignment: MainAxisAlignment.end,
                ),
                decoration: BoxDecoration(
                  image: DecorationImage(
                    image: AssetImage('images/system/galaxy2.jpg'),
                    fit: BoxFit.cover,
                  ),
                ),
              );
              // return Column(
              //   children: [
              //     Row(
              //       children: [
              //         Card(
              //           child: Column(
              //             children: [
              //               Text(systemData.id.toString()),
              //               Text(systemData.name),
              //               Text(systemUpdated.toString()),
              //               Text(coords),
              //             ],
              //           ),
              //         ),
              //       ],
              //     ),
              //   ],
              // );
            }
            if (snapshot.hasError) {
              print(snapshot.error);
              return Text('Crapple');
            } else {
              return CircularProgressIndicator();
            }
          },
        ),
      ),
    );
  }
}
