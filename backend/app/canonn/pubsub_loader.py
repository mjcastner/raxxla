from commonlib.google import gcs
from commonlib.google import sheets
from protos import poi_pb2

from absl import app, flags, logging

# Global vars
DATASET = 'canonn'
SCIENCE_DB_URL = 'https://docs.google.com/spreadsheets/d/1qd38O5farcCHCRodQnJntIIZA7VN99bt_4HtiwI0yNg/'
NOTABLE_PHENOMENA_URL = 'https://docs.google.com/spreadsheets/d/1IY7R2Q74tBgAZUjGdFT5IWhD5c4RRh2Ad_y90slNl7s/'
CANONN_DATA = [
  {'name': 'bark_mounds', 'sheet_url': SCIENCE_DB_URL, 'sheet_name': 'BM'},
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
]
FILE_TYPES = [x.get('name') for x in CANONN_DATA]
FILE_TYPES_META = FILE_TYPES.copy()
FILE_TYPES_META.append('all')

# Define flags
FLAGS = flags.FLAGS
flags.DEFINE_boolean('cleanup_files', False, 'Cleanup GCS files.')
flags.DEFINE_enum('file_type', None, FILE_TYPES_META, 'File(s) to process.')
flags.mark_flag_as_required('file_type')


def canonn_dict_to_proto(sheet_type: str, canonn_dict: dict):
  poi = poi_pb2.PointOfInterest()
  poi.type = sheet_type

  if sheet_type in ('bark_mounds', 'crystalline_shards'):
    poi.id = canonn_dict.get('ID #')
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
    message_body.type = 'Beacon Body'
    message_body.value = canonn_dict.get('Message Body')

    message_system = poi.metadata.add()
    message_system.type = 'Beacon System'
    message_system.value = canonn_dict.get('Message System')

    message_lat = poi.metadata.add()
    message_lat.type = 'Beacon Latitude'
    message_lat.value = canonn_dict.get('Message Lat')

    message_lon = poi.metadata.add()
    message_lon.type = 'Beacon Longitude'
    message_lon.value = canonn_dict.get('Message Lon')

    return poi

  elif sheet_type == 'guardian_ruins':
    poi.id = canonn_dict.get('ID #')
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

    ruin_type = poi.metadata.add()
    ruin_type.type = 'Ruin type'
    ruin_type.value = canonn_dict.get('Type')

    latitude = poi.metadata.add()
    latitude.type = 'Latitude'
    latitude.value = canonn_dict.get('Latitude')

    longitude = poi.metadata.add()
    longitude.type = 'Longitude'
    longitude.value = canonn_dict.get('Longitude')

    return poi

  elif sheet_type == 'hyperdictions':
    hyperdiction = poi_pb2.Hyperdiction()
    hyperdiction.origin_system = canonn_dict.get('From System')
    hyperdiction.destination_system = canonn_dict.get('To System')
    hyperdiction.commander = canonn_dict.get('CMDR')
    
    return hyperdiction

  elif sheet_type == 'thargoid_barnacles':
    poi.id = canonn_dict.get('ID #')
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

    latitude = poi.metadata.add()
    latitude.type = 'Latitude'
    latitude.value = canonn_dict.get('Latitude')

    longitude = poi.metadata.add()
    longitude.type = 'Longitude'
    longitude.value = canonn_dict.get('Longitude')

    if canonn_dict.get('SubType'):
      barnacle_subtype = poi.metadata.add()
      barnacle_subtype.type = 'Barnacle sub-type'
      barnacle_subtype.value = canonn_dict.get('SubType')

    if canonn_dict.get('Type'):
      barnacle_type = poi.metadata.add()
      barnacle_type.type = 'Barnacle type'
      barnacle_type.value = canonn_dict.get('Type')

    if canonn_dict.get('Meta Alloy'):
      meta_alloy = poi.metadata.add()
      meta_alloy.type = 'Meta-alloy'
      meta_alloy.value = canonn_dict.get('Meta Alloy')
    
    if canonn_dict.get('Ripe Status'):
      ripeness = poi.metadata.add()
      ripeness.type = 'Ripeness'
      ripeness.value = canonn_dict.get('Ripe Status')

    return poi

  elif sheet_type == 'tube_worms':
    poi.id = canonn_dict.get('ID #')
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

    tube_worm_variant = poi.metadata.add()
    tube_worm_variant.type = 'Variant'
    tube_worm_variant.value = canonn_dict.get('Type')

    return poi

  elif sheet_type == 'notable_phenomena':
    poi.id = canonn_dict.get('NP ID')
    poi.system_name = canonn_dict.get('System')
    poi.discovered_by = canonn_dict.get('Reported by')

    galactic_region = poi.metadata.add()
    galactic_region.type = 'Galactic region'
    galactic_region.value = canonn_dict.get('Galactic Region')

    if canonn_dict.get('Notes'):
      np_notes = poi.metadata.add()
      np_notes.type = 'Notes'
      np_notes.value = canonn_dict.get('Notes')

    if canonn_dict.get('Storm?') == 'Yes':
      storm = poi.metadata.add()
      storm.type = 'Storm'
      storm.value = 'True'

    if canonn_dict.get('Type of Notable Phenomenon'):
      phenomenon_type = poi.metadata.add()
      phenomenon_type.type = 'Phenomenon type'
      phenomenon_type.value = canonn_dict.get('Type of Notable Phenomenon')

    if canonn_dict.get('Variety'):
      phenomenon_variant = poi.metadata.add()
      phenomenon_variant.type = 'Variant'
      phenomenon_variant.value = canonn_dict.get('Variety')

    return poi

  else:
    logging.error('Unsupported sheet type: %s', sheet_type)
    return False


def process_sheet(sheet_dict: dict):
    logging.info('Processing Canonn sheet: %s', sheet_dict.get('name'))
    sheet = sheets.get_spreadsheet(sheet_dict.get('sheet_url'))
    worksheet = sheet.worksheet(sheet_dict.get('sheet_name'))
    worksheet_df = sheets.worksheet_to_dataframe(worksheet)

    proto_batch = []
    for row_dict in worksheet_df.to_dict('records'):
      canonn_proto = canonn_dict_to_proto(sheet_dict.get('name'), row_dict)
      if canonn_proto:
        pubsub_message = pubsub.send_message(
            FLAGS.pubsub_topic,
            canonn_proto.SerializeToString(),
            dataset=DATASET,
            table=sheet_dict.get('name'),
        )
        proto_batch.append(pubsub_message)
    
    return proto_batch


def main(argv):
  if FLAGS.file_type == 'all':
    proto_batch = [process_sheet(x) for x in CANONN_DATA]

  else:
    sheet_dict = next(x for x in CANONN_DATA if x.get('name') == FLAGS.file_type)
    proto_batch = process_sheet(sheet_dict)


if __name__ == '__main__':
  app.run(main)
