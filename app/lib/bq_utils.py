from google.cloud import bigquery

class bq_client(bigquery.client.Client):
  def __init__(self):
    self.banana = 'test'

