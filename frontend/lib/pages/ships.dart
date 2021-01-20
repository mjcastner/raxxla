import 'package:flutter/material.dart';
import 'package:raxxla/pages/base.dart';

class Ship {
  String name;
  String manufacturer;
  String description;
  String image;

  Ship(String name) {
    this.name = name;
    this.image = 'images/ships/$name.png';
  }
}

class ShipsPage extends StatelessWidget {
  var selected_ship = Ship('adder');

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(),
      drawer: NavDrawer(),
      body: Column(
        children: [
          Row(
            children: [
              Text('Text'),
            ],
          ),
          Expanded(
            child: Container(
              margin: EdgeInsets.all(20),
              decoration: BoxDecoration(
                image: DecorationImage(
                  image: AssetImage(selected_ship.image),
                ),
              ),
            ),
            flex: 2,
          ),
          Expanded(
            child: Card(
              margin: EdgeInsets.all(20),
              child: Container(
                margin: EdgeInsets.all(20),
                child: Row(
                  children: [
                    Text('Item'),
                    Text('Item'),
                    Text('Item'),
                  ],
                ),
              ),
            ),
            flex: 1,
          ),
        ],
      ),
    );
  }
}
