from lib import pubsub

from absl import app, flags, logging


def main(argv):
  del argv
  response = pubsub.send_bigquery_row('Test body.', 'raxxla.edsm.test')

  while not response.done():
    time.sleep(5)

  print(response.done())
  print(response.exception())

if __name__ == '__main__':
  app.run(main)
