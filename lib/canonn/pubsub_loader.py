from commonlib.google import sheets
from proto import poi_pb2

from absl import app, flags, logging

# Global vars
DATASET = 'canonn'
SCIENCE_DB_URL = 'https://docs.google.com/spreadsheets/d/1qd38O5farcCHCRodQnJntIIZA7VN99bt_4HtiwI0yNg/'
NOTABLE_PHENOMENA_URL = 'https://docs.google.com/spreadsheets/d/1IY7R2Q74tBgAZUjGdFT5IWhD5c4RRh2Ad_y90slNl7s/'
TOURIST_BEACON_URL = 'https://docs.google.com/spreadsheets/d/1eu30UyjpQrWexAglwD1Ax_GaDz4d7l8KD76kSzX4DEk/'
CANONN_DATA = [
  {'name': 'bark_mounds', 'sheet_url': SCIENCE_DB_URL, 'sheet_name': 'BM'},
  # {'name': 'brain_trees', 'sheet_url': SCIENCE_DB_URL, 'sheet_name': 'BT'},
  {'name': 'crystalline_shards', 'sheet_url': SCIENCE_DB_URL,
   'sheet_name': 'CS'},
  {'name': 'generation_ships', 'sheet_url': SCIENCE_DB_URL,
   'sheet_name': 'GEN'},
  {'name': 'guardian_beacons', 'sheet_url': SCIENCE_DB_URL, 'sheet_name': 'GB'},
  {'name': 'guardian_ruins', 'sheet_url': SCIENCE_DB_URL, 'sheet_name': 'GR'},
  {'name': 'hyperdictions', 'sheet_url': SCIENCE_DB_URL, 'sheet_name': 'HD'},
  {'name': 'thargoid_barnacles', 'sheet_url': SCIENCE_DB_URL,
   'sheet_name': 'TB'},
  {'name': 'tube_worms', 'sheet_url': SCIENCE_DB_URL, 'sheet_name': 'TW'},
  {'name': 'notable_phenomena', 'sheet_url': NOTABLE_PHENOMENA_URL,
   'sheet_name': 'NP Repository'},
  {'name': 'tourist_beacons', 'sheet_url': TOURIST_BEACON_URL,
   'sheet_name': 'Beacons'},
]


def canonn_dict_to_proto(sheet_type: str, canonn_dict: dict):
  poi = poi_pb2.PointOfInterest()

  if sheet_type in ('bark_mounds', 'crystalline_shards'):
    poi.id = canonn_dict.get('ID #')
    poi.type = 'Bark mound'
    poi.system_name = canonn_dict.get('System')
    poi.planet_name = canonn_dict.get('Planet')
    poi.discovered_by = canonn_dict.get('Discovered By')
    poi.coordinates.x = float(canonn_dict.get('Galactic X'))
    poi.coordinates.y = float(canonn_dict.get('Galactic Y'))
    poi.coordinates.z = float(canonn_dict.get('Galactic Z'))
    poi.coordinates.coordinates = '%s, %s, %s' % (
        canonn_dict.get('Galactic X'),
        canonn_dict.get('Galactic Y'),
        canonn_dict.get('Galactic Z'),
    )

    return poi

  elif sheet_type == 'generation_ships':
    poi.id = canonn_dict.get('ID #')
    poi.type = 'Generation ship'
    poi.system_name = canonn_dict.get('System')
    poi.planet_name = canonn_dict.get('Orbit Body')
    poi.discovered_by = canonn_dict.get('Discovered By')
    poi.coordinates.x = float(canonn_dict.get('Galactic X'))
    poi.coordinates.y = float(canonn_dict.get('Galactic Y'))
    poi.coordinates.z = float(canonn_dict.get('Galactic Z'))
    poi.coordinates.coordinates = '%s, %s, %s' % (
        canonn_dict.get('Galactic X'),
        canonn_dict.get('Galactic Y'),
        canonn_dict.get('Galactic Z'),
    )

    gen_ship_name = poi.metadata.add()
    gen_ship_name.type = 'name'
    gen_ship_name.value = canonn_dict.get('Name')
    
    return poi

  elif sheet_type == 'guardian_beacons':
    poi.id = canonn_dict.get('ID #')
    poi.type = 'Guardian beacon'
    poi.system_name = canonn_dict.get('System')
    poi.planet_name = canonn_dict.get('Planet')
    poi.discovered_by = canonn_dict.get('Discovered By')
    poi.coordinates.x = float(canonn_dict.get('Galactic X'))
    poi.coordinates.y = float(canonn_dict.get('Galactic Y'))
    poi.coordinates.z = float(canonn_dict.get('Galactic Z'))
    poi.coordinates.coordinates = '%s, %s, %s' % (
        canonn_dict.get('Galactic X'),
        canonn_dict.get('Galactic Y'),
        canonn_dict.get('Galactic Z'),
    )

    message_body = poi.metadata.add()
    message_body.type = 'Message Body'
    message_body.value = canonn_dict.get('Message Body')

    message_system = poi.metadata.add()
    message_system.type = 'Message System'
    message_system.value = canonn_dict.get('Message System')

    message_lat = poi.metadata.add()
    message_lat.type = 'Message Latitude'
    message_lat.value = canonn_dict.get('Message Lat')

    message_lon = poi.metadata.add()
    message_lon.type = 'Message Longitude'
    message_lon.value = canonn_dict.get('Message Lon')
    
    return poi

  elif sheet_type == 'guardian_ruins':
    print(canonn_dict)
    print(poi)
    print('\n\n')
  else:
    logging.error('Unsupported sheet type: %s', sheet_type)
    return False
  # for row in worksheet_df.to_dict('records'):
  #   print(row)


def main(argv):
  for sheet_dict in CANONN_DATA:
    logging.info('Processing Canonn sheet: %s', sheet_dict.get('name'))
    sheet = sheets.get_spreadsheet(sheet_dict.get('sheet_url'))
    worksheet = sheet.worksheet(sheet_dict.get('sheet_name'))
    worksheet_df = sheets.worksheet_to_dataframe(worksheet)
    canonn_protos = [canonn_dict_to_proto(sheet_dict.get('name'), x) for x in worksheet_df.to_dict('records')]


if __name__ == '__main__':
  app.run(main)
