from lib import bigquery

from absl import app, flags, logging

# Define flags
FLAGS = flags.FLAGS
flags.DEFINE_integer(
    'batch_size',
    bigquery.STREAMING_BATCH_SIZE,
    'Your age in years.',
    lower_bound=1,
)


def main(argv):
  del argv

  print(FLAGS.batch_size)


if __name__ == '__main__':
  app.run(main)
