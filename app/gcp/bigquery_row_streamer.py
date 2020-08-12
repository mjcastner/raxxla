import json
from lib import bigquery, pubsub
from pprint import pprint

from absl import app, flags, logging

# Define flags
FLAGS = flags.FLAGS
flags.DEFINE_integer(
    'batch_size',
    bigquery.STREAMING_BATCH_SIZE,
    'Rows to process in a given streaming batch.',
    lower_bound=1,
)

def format_messages(messages):
  bq_tables = {x.message.attributes.get('table') for x in messages}
  bq_rows = {x:[] for x in bq_tables}

  for msg in messages:
    table = msg.message.attributes.get('table')
    row = json.loads(msg.message.data)
    row_batch = bq_rows.get(table)
    row_batch.append(row)

  return bq_rows


def main(argv):
  del argv

  messages = pubsub.fetch_bigquery_batch(FLAGS.batch_size)
  rows = format_messages(messages)

  for table, rows in rows.items():
    bigquery.stream_records(table, rows)


if __name__ == '__main__':
  app.run(main)
