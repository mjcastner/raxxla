from commonlib.google import sheets

# Global vars
DATASET = 'canonn'
SCIENCE_DB_URL = 'https://docs.google.com/spreadsheets/d/1qd38O5farcCHCRodQnJntIIZA7VN99bt_4HtiwI0yNg/'
NOTABLE_PHENOMENA_URL = 'https://docs.google.com/spreadsheets/d/1IY7R2Q74tBgAZUjGdFT5IWhD5c4RRh2Ad_y90slNl7s/'
TOURIST_BEACON_URL = 'https://docs.google.com/spreadsheets/d/1eu30UyjpQrWexAglwD1Ax_GaDz4d7l8KD76kSzX4DEk/'
CANONN_DATA = [
  {'name': 'bark_mounds', 'sheet_url': SCIENCE_DB_URL, 'sheet_name': 'BM'},
  {'name': 'brain_trees', 'sheet_url': SCIENCE_DB_URL, 'sheet_name': 'BT'},
  {'name': 'crystalline_shards', 'sheet_url': SCIENCE_DB_URL,
   'sheet_name': 'CS'},
  {'name': 'generation_ships', 'sheet_url': SCIENCE_DB_URL,
   'sheet_name': 'GEN'},
  {'name': 'guardian_beacons', 'sheet_url': SCIENCE_DB_URL, 'sheet_name': 'GB'},
  {'name': 'guardian_ruins', 'sheet_url': SCIENCE_DB_URL, 'sheet_name': 'GR'},
  {'name': 'hyperdictions', 'sheet_url': SCIENCE_DB_URL, 'sheet_name': 'HD'},
  {'name': 'lagrange_cloud', 'sheet_url': SCIENCE_DB_URL, 'sheet_name': 'LC'},
  {'name': 'thargoid_barnacles', 'sheet_url': SCIENCE_DB_URL,
   'sheet_name': 'TB'},
  {'name': 'tube_worms', 'sheet_url': SCIENCE_DB_URL, 'sheet_name': 'TW'},
  {'name': 'notable_phenomena', 'sheet_url': NOTABLE_PHENOMENA_URL,
   'sheet_name': 'NP Repository'},
  {'name': 'tourist_beacons', 'sheet_url': TOURIST_BEACON_URL,
   'sheet_name': 'Beacons'},
]

for sheet in CANONN_DATA:
  print(sheet)

