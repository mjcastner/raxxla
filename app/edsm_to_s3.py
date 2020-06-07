import os
import sys
from multiprocessing import Pool
from urllib import error as urllib_error
from urllib import request

import boto3
from absl import app, flags, logging

from lib import schema

# Global vars
file_types = list(schema.edsm_files.keys())
file_types.append('all')
s3 = boto3.client('s3')

# Define flags
FLAGS = flags.FLAGS
flags.DEFINE_string('bucket', None, 'AWS S3 Bucket for raw file storage.')
flags.DEFINE_string('prefix', 'edsm', 'AWS S3 path prefix.')
flags.DEFINE_enum('type', None, file_types, 'Input file type.')
flags.mark_flag_as_required('bucket')
flags.mark_flag_as_required('type')


def fetch_edsm_file(filetype: str) -> tuple:
  fetch_complete = False
  edsm_file_url = schema.edsm_files.get(filetype)
  gz_filepath = '/tmp/%s.gz' % (filetype)

  # Fetch GZ from EDSM
  logging.info('Fetching %s...', edsm_file_url)
  try:
    with request.urlopen(edsm_file_url) as response:
      gz_data = response.read()

      with open(gz_filepath, 'wb') as gz_file:
        gz_file.write(gz_data)
        logging.info('Saved to %s...', gz_filepath)
        fetch_complete = True
  except (urllib_error.URLError, urllib_error.HTTPError) as e:
    logging.error('Error fetching %s file: %s', filetype, e)

  return (fetch_complete, filetype, gz_filepath)


# TODO(mjcastner): Add S3 lifecycle policy to uploaded objects (5 day exp.)
def upload_to_s3(filetype_obj: tuple) -> bool:
  filetype = filetype_obj[1]
  filepath = filetype_obj[2]
  s3_path = '%s/%s.gz' % (FLAGS.prefix, filetype)

  try:
    logging.info('Uploading file to s3:/%s/%s...', FLAGS.bucket, s3_path)
    s3.upload_file(filepath, FLAGS.bucket, s3_path)

    logging.info('Uploaded successfully!  Cleaning up temp file %s', filepath)
    os.remove(filepath)
    return True
  except Exception as e:
    logging.error(e)
    return False


def main(argv):
  del argv

  if FLAGS.type == 'all':
    with Pool(5) as fetch_pool:
      filetype_obj = fetch_pool.map(fetch_edsm_file, file_types)
      fetch_status = map(lambda x: x[0], filetype_obj)
      if all(fetch_status):
        fetch_pool.map(upload_to_s3, filetype_obj)
      else:
        logging.error('1 or more files failed to download, exiting...')
        sys.exit()
  else:
    filetype_obj = fetch_edsm_file(FLAGS.type)
    fetch_status = filetype_obj[0]
    if fetch_status:
      upload_to_s3(filetype_obj)
    else:
      logging.error('File failed to download, exiting...')
      sys.exit()


if __name__ == '__main__':
  app.run(main)
