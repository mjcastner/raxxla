import io
import gzip
from urllib import request

from absl import app
from absl import flags
from absl import logging
import boto3


# Define flags
FLAGS = flags.FLAGS
flags.DEFINE_string('bucket', None, 'AWS S3 Bucket for raw file storage.')
flags.DEFINE_string('prefix', 'edsm', 'AWS S3 path prefix.')
flags.DEFINE_enum(
    'type', None,
    ['systems', 'population', 'bodies', 'powerplay', 'stations', 'all'],
    'Input file type.')
flags.mark_flag_as_required('bucket')
flags.mark_flag_as_required('type')

# Global vars
s3 = boto3.client('s3')
edsm_files = {
  'bodies': 'https://www.edsm.net/dump/bodies7days.json.gz',
  'population': 'https://www.edsm.net/dump/systemsPopulated.json.gz',
  'powerplay': 'https://www.edsm.net/dump/powerPlay.json.gz',
  'stations': 'https://www.edsm.net/dump/stations.json.gz',
  'systems': 'https://www.edsm.net/dump/systemsWithCoordinates.json.gz',
}


def fetch_edsm_file(filetype: str):
  edsm_file_url = edsm_files.get(filetype)
  logging.info('Fetching %s...', edsm_file_url)

  with request.urlopen(edsm_file_url) as response:
    logging.info('Processing gzipped file...')
    gz_data = response.read()
    file_content = gzip.decompress(gz_data)
    file_object = io.BytesIO(file_content)
    s3_path = '%s/%s.json' % (FLAGS.prefix, filetype)

    logging.info('Uploading file to s3:/%s/%s...', FLAGS.bucket, s3_path)
    s3_response = s3.upload_fileobj(file_object, FLAGS.bucket, s3_path)


def main(argv):
  del argv

  if FLAGS.type == 'all':
    for k, v in edsm_files.items():
      fetch_edsm_file(k)
  else:
    fetch_edsm_file(FLAGS.type)


if __name__ == '__main__':
  app.run(main)
