import os
import sys
from multiprocessing import Pool
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


def fetch_edsm_file(filetype: str) -> tuple:
  edsm_file_url = edsm_files.get(filetype)

  # Fetch GZ from EDSM
  logging.info('Fetching %s...', edsm_file_url)
  with request.urlopen(edsm_file_url) as response:
    gz_filepath = '/tmp/%s.gz' % (filetype)
    gz_data = response.read()

    with open(gz_filepath, 'wb') as gz_file:
      gz_file.write(gz_data)
      logging.info('Saved to %s...', gz_filepath)

  return (filetype, gz_filepath)


def upload_to_s3(type_path: tuple) -> bool:
  filetype = type_path[0]
  filepath = type_path[1]
  s3_path = '%s/%s.gz' % (FLAGS.prefix, filetype)

  try:
    logging.info('Uploading file to s3:/%s/%s...', FLAGS.bucket, s3_path)
    s3.upload_file(filepath, FLAGS.bucket, s3_path)
    os.remove(filepath)
    return True
  except Exception as e:
    logging.error(e)
    return False


def main(argv):
  del argv

  if FLAGS.type == 'all':
    with Pool(5) as fetch_pool:
      download_list = list(edsm_files.keys())
      type_paths = fetch_pool.map(fetch_edsm_file, download_list)
      if all(type_paths):
        fetch_pool.map(upload_to_s3, type_paths)
      else:
        logging.error('1 or more files failed to download, exiting...')
        sys.exit()
  else:
    type_path = fetch_edsm_file(FLAGS.type)
    upload_to_s3(type_path)


if __name__ == '__main__':
  app.run(main)
