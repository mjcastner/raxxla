import base64

def bigquery_row_streamer(event, context):
  table = event['attributes'].get('table')
  row_json = base64.b64decode(event['data']).decode('utf-8')

  print(row_json)
  print(table)
