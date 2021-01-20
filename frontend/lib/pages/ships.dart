import 'package:flutter/material.dart';
import 'package:raxxla/pages/base.dart';
import 'package:animated_text_kit/animated_text_kit.dart';

final ships = <String, String>{
  'Adder': 'adder',
  'Cobra Mk.III': 'cobra_mk_3',
  'Cobra Mk.IV': 'cobra_mk_4',
};

class Ship {
  String name;
  String manufacturer;
  String description;
  String image;

  Ship(String name) {
    this.name = ships[name];
    this.image = 'images/ships/${ships[name]}.png';

    switch (this.name) {
      case "adder":
        {
          this.manufacturer = 'zorgon_peterson';
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
              child: Row(
                children: [
                  TypewriterAnimatedTextKit(
                    speed: Duration(milliseconds: 100),
                    isRepeatingAnimation: false,
                    text: ["ROCK ME AMADEUS\nROCK ME ALL NIGHT LONG"],
                    textStyle: TextStyle(fontSize: 24.0),
                  ),
                ],
              ),
              color: Colors.black54,
              height: 250,
            ),
          ],
        ),
        decoration: BoxDecoration(
          image: DecorationImage(
            fit: BoxFit.fitHeight,
            image: AssetImage(selectedShip.image),
          ),
        ),
      ),
    );
  }
}
