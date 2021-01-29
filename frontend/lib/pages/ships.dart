import 'package:flutter/material.dart';
import 'package:raxxla/pages/base.dart';
import 'package:animated_text_kit/animated_text_kit.dart';
import 'package:google_fonts/google_fonts.dart';

final ships = <String, Map>{
  'Adder': {
    'name': 'adder',
    'description':
        'A compact, adaptable ship, the Adder has a larger cargo hold than other ships of similar size, and its base jump range of over 30 light years makes it a viable choice for explorers. The ship can also hold its own in a dogfight, when properly outfitted.',
  },
  'Cobra Mk.III': {
    'name': 'cobra_mk_3',
  },
  'Cobra Mk.IV': {
    'name': 'cobra_mk_4',
  },
};

class Ship {
  String name;
  String manufacturer;
  String description;
  String image;

  Ship(String name) {
    var shipData = ships[name];
    this.name = shipData['name'];
    this.image = 'images/ships/${shipData['name']}.png';

    switch (this.name) {
      case "adder":
        {
          this.manufacturer = 'zorgon_peterson';
          this.description = shipData['description'];
        }
        break;

      case "cobra_mk_3":
        {
          this.manufacturer = 'faulcon_delacy';
        }
        break;

      case "cobra_mk_4":
        {
          this.manufacturer = 'faulcon_delacy';
        }
        break;

      default:
        {
          this.manufacturer = 'unknown';
        }
    }
  }
}

class ShipsPage extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(),
      drawer: NavDrawer(),
      body: Center(child: ShipDetails()),
    );
  }
}

class ShipDetails extends StatefulWidget {
  @override
  _ShipDetailState createState() => _ShipDetailState();
}

class _ShipDetailState extends State<ShipDetails> {
  String dropdownValue = 'Adder';
  var selectedShip = Ship('Adder');

  void getShipDetails(String shipName) {
    setState(() {
      dropdownValue = shipName;
      selectedShip = Ship(shipName);
    });
  }

  @override
  Widget build(BuildContext context) {
    return Expanded(
      child: Container(
        child: Column(
          mainAxisAlignment: MainAxisAlignment.spaceBetween,
          children: [
            Row(
              mainAxisAlignment: MainAxisAlignment.end,
              children: [
                Column(
                  children: [
                    Card(
                      child: Container(
                        margin: EdgeInsets.all(5),
                        width: 200,
                        height: 200,
                        decoration: BoxDecoration(
                          image: DecorationImage(
                            fit: BoxFit.cover,
                            image: AssetImage(
                                'images/ships/manufacturers/${selectedShip.manufacturer}.png'),
                          ),
                        ),
                      ),
                    ),
                    DropdownButton(
                      value: dropdownValue,
                      icon: Icon(Icons.arrow_downward),
                      iconSize: 24,
                      elevation: 2,
                      underline: Container(
                        height: 2,
                        color: Colors.white,
                      ),
                      onChanged: (String selectedShip) {
                        getShipDetails(selectedShip);
                      },
                      items: ships.keys
                          .map<DropdownMenuItem<String>>((String value) {
                        return DropdownMenuItem<String>(
                          value: value,
                          child: Text(value),
                        );
                      }).toList(),
                    ),
                  ],
                ),
              ],
            ),
            Container(
              margin: EdgeInsets.zero,
              child: Row(
                mainAxisAlignment: MainAxisAlignment.spaceEvenly,
                children: [
                  Column(
                    mainAxisAlignment: MainAxisAlignment.center,
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [],
                  ),
                ],
              ),
              color: Colors.black54,
              height: 250,
            ),
          ],
        ),
        margin: EdgeInsets.all(10),
        decoration: BoxDecoration(
          image: DecorationImage(
            fit: BoxFit.fitWidth,
            image: AssetImage(selectedShip.image),
          ),
        ),
      ),
    );
  }
}
