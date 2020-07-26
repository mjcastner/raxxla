import json

from lib import pubsub
from pprint import pprint

from absl import app, flags, logging
from google.cloud import bigquery

import pandas

# Global variables
STREAMING_BATCH_SIZE = 500
BQ_CLIENT = bigquery.Client()

# Define flags
FLAGS = flags.FLAGS
flags.DEFINE_integer(
    'batch_size',
    STREAMING_BATCH_SIZE,
    'Rows to process in a given streaming batch.',
    lower_bound=1,
)


def main(argv):
  del argv

  messages = pubsub.fetch_bigquery_batch(FLAGS.batch_size)
  bq_tables = {x.message.attributes.get('table') for x in messages}
  bq_rows = {x:[] for x in bq_tables}

  for msg in messages:
    table = msg.message.attributes.get('table')
    row = json.loads(msg.message.data)
    row_batch = bq_rows.get(table)
    row_batch.append(row)
  
  for table, rows in bq_rows.items():
    print(table, len(rows))
  
  #test_df = pandas.DataFrame(row_batch)
  #print(test_df)
  #BQ_CLIENT.insert_rows_from_dataframe('', test_df)
  #pubsub.ack_message_batch(messages)


if __name__ == '__main__':
  app.run(main)
