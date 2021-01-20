import 'package:flutter/material.dart';
import 'package:google_fonts/google_fonts.dart';

class NavItem extends StatelessWidget {
  String title;
  String destination;
  String image;

  NavItem(String title, String destination, String image) {
    this.title = title;
    this.destination = destination;
    this.image = image;
  }

  @override
  Widget build(BuildContext context) {
    return Card(
        child: ListTile(
      onTap: () {
        Navigator.pushNamed(context, this.destination);
      },
      leading: ConstrainedBox(
        constraints: BoxConstraints(
          maxWidth: 30,
          maxHeight: 30,
        ),
        child: Image.asset(this.image, fit: BoxFit.cover),
      ),
      title: Text(this.title),
    ));
  }
}

class NavDrawer extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return Drawer(
      child: ListView(children: [
        DrawerHeader(
          child: Row(children: [
            Image.asset(
              'images/universal_cartographics_logo.png',
              height: 50,
              width: 50,
            ),
            Text('Univeral Cartographics\nStellar Cartography Database',
                style: GoogleFonts.roboto(fontWeight: FontWeight.bold))
          ]),
          decoration: BoxDecoration(
            color: Colors.black87,
          ),
        ),
        ExpansionTile(
          title: Text('Cartographic Database'),
          children: [
            NavItem('Systems', '/systems', 'images/system.png'),
            NavItem('Planets', '/planets', 'images/planet.png'),
            NavItem('Settlements', '/settlements', 'images/settlements.png'),
            NavItem('Stations', '/stations', 'images/station.png'),
          ],
        ),
        ExpansionTile(
          title: Text('Galactic Census'),
          children: [
            NavItem('Population', '/systems', 'images/population.png'),
            NavItem('Factions', '/systems', 'images/factions.png'),
            NavItem('Powers', '/systems', 'images/powers.png'),
          ],
        ),
        ExpansionTile(
          title: Text('Technology'),
          children: [
            NavItem('Ships', '/ships', 'images/ship.png'),
          ],
        ),
        ExpansionTile(
          title: Text('Xenobiology'),
          children: [
            NavItem('Guardians', '/systems', 'images/guardian.png'),
            NavItem('Thargoids', '/systems', 'images/thargoid.png'),
          ],
        ),
      ]),
    );
  }
}
