import json

from lib import pubsub

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
  for msg in messages:
    print(dir(msg))
    print(msg)
  #row_batch = [json.loads(x.message.data) for x in messages]
  #test_df = pandas.DataFrame(row_batch)
  #print(test_df)
  #BQ_CLIENT.insert_rows_from_dataframe('', test_df)
  #pubsub.ack_message_batch(messages)


if __name__ == '__main__':
  app.run(main)
