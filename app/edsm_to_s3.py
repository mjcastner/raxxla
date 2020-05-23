import gzip
import io
import os
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

  # Fetch GZ from EDSM
  logging.info('Fetching %s...', edsm_file_url)
  with request.urlopen(edsm_file_url) as response:
    gz_filepath = '/tmp/%s.gz' % (filetype)
    json_filepath = '/tmp/%s.json' % (filetype)
    gz_data = response.read()

    with open(gz_filepath, 'wb') as gz_file:
      gz_file.write(gz_data)
      logging.info('Saved to %s...', gz_filepath)

  with open(gz_filepath, 'rb') as gz_file:
    logging.info('Decompressing %s...', gz_filepath)
    gz_content = gz_file.read()
    stream_str = io.BytesIO(gz_content)

    with open(json_filepath, 'wb') as json_file:
      logging.info('Writing output to %s...', json_filepath)
      raw_content = gzip.decompress(stream_str.read())
      json_file.write(raw_content)

  # Upload to S3
  s3_path = '%s/%s.json' % (FLAGS.prefix, filetype)
  logging.info('Uploading file to s3:/%s/%s...', FLAGS.bucket, s3_path)
  s3.upload_file(json_filepath, FLAGS.bucket, s3_path)

  # Clean up tmp files
  logging.info('Cleaning temp files...')
  tmp_files = [gz_filepath, json_filepath]
  for file in tmp_files:
    os.remove(file)


def main(argv):
  del argv

  if FLAGS.type == 'all':
    for k, v in edsm_files.items():
      fetch_edsm_file(k)
  else:
    fetch_edsm_file(FLAGS.type)


if __name__ == '__main__':
  app.run(main)
