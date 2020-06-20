import gzip
import json
import os
import re

from multiprocessing import Pool
from urllib import error as urllib_error
from urllib import request

from absl import app, flags, logging
from edsm import schema
from lib import sqs

# Global vars
file_types_meta = schema.file_types.copy()
file_types_meta.append('all')

# Define flags
FLAGS = flags.FLAGS
flags.DEFINE_integer('batch_size',
                     500,
                     'Default job batch size for processing and loading',
                     lower_bound=1)
flags.DEFINE_string(
    'download_dir',
    '/tmp',
    'Local directory in which to temporarily store EDSM files.')
flags.DEFINE_enum('type', None, file_types_meta, 'Input file type.')
flags.DEFINE_string('sqs_queue', None, 'SQS Queue name.')
flags.mark_flag_as_required('type')
flags.mark_flag_as_required('sqs_queue')


def fetch_edsm_file(filetype: str) -> tuple:
  edsm_file_url = schema.urls.get(filetype)
  gz_filepath = '%s/%s.gz' % (FLAGS.download_dir, filetype)

  try:
    logging.info('Fetching %s...', edsm_file_url)
    with request.urlopen(edsm_file_url) as response:
      gz_data = response.read()

      with open(gz_filepath, 'wb') as gz_file:
        gz_file.write(gz_data)
        logging.info('Saved to %s', gz_filepath)
  except (urllib_error.URLError, urllib_error.HTTPError) as e:
    logging.error('Error fetching %s file: %s', filetype, e)

  return (filetype, gz_filepath)


def process_edsm_file(filetype: str, gz_filepath: str) -> bool:
  gz_file = open(gz_filepath, 'rb')
  sqs_batch = []

  try:
    with gzip.open(gz_file, mode='rt') as uncompressed_file:
      for line in uncompressed_file:
        if len(sqs_batch) < FLAGS.batch_size:
          json_object_match = re.search(r'(\{.*\})', line)
          if json_object_match:
            json_object = json_object_match.group(1)
            edsm_object = schema.edsmObject(filetype)
            parsed_json = edsm_object.format_json(json_object)
            sqs_batch.append(json.loads(parsed_json))
        else:
          sqs_body = json.dumps(sqs_batch)
          sqs_response = sqs.send_message(
              queue_name=FLAGS.sqs_queue,
              message_content=sqs_body,
              message_attributes=edsm_object.attributes)
          if sqs_response:
            logging.info(
                '[EDSM/%s] batch of %s sent to "%s" SQS queue',
                filetype,
                len(sqs_batch),
                FLAGS.sqs_queue)
            sqs_batch.clear()

      sqs_body = json.dumps(sqs_batch)
      sqs_response = sqs.send_message(
          queue_name=FLAGS.sqs_queue,
          message_content=sqs_body,
          message_attributes=edsm_object.attributes)
      if sqs_response:
        logging.info(
            '[EDSM/%s] batch of %s sent to "%s" SQS queue',
            filetype,
            len(sqs_batch),
            FLAGS.sqs_queue)
        sqs_batch.clear()
      return True
  except Exception as e:
    logging.error(e)
    return False


def main(argv):
  del argv

  if FLAGS.type == 'all':
    with Pool(5) as fetch_pool:
      fetch_responses = fetch_pool.map(fetch_edsm_file, schema.file_types)
      for fetch_response in fetch_responses:
        filetype = fetch_response[0]
        gz_filepath = fetch_response[1]
        process_response = process_edsm_file(filetype, gz_filepath)
        if process_response:
          logging.info('Successfully processed %s file!', FLAGS.type)
          os.remove(gz_filepath)
          logging.info('Temporary file %s removed', gz_filepath)
  else:
    fetch_response = fetch_edsm_file(FLAGS.type)
    gz_filepath = fetch_response[1]
    process_response = process_edsm_file(FLAGS.type, gz_filepath)
    if process_response:
      logging.info('Successfully processed %s file!', FLAGS.type)
      os.remove(gz_filepath)
      logging.info('Temporary file %s removed', gz_filepath)



if __name__ == '__main__':
  app.run(main)
