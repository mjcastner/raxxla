import gzip
import io
import json
import re
from multiprocessing import Pool

import boto3
from absl import app, flags, logging
from google.cloud import bigquery
from lib import schema

# Define args
FLAGS = flags.FLAGS
flags.DEFINE_integer('batch_size',
                     500,
                     'Default job batch size for processing and loading',
                     lower_bound=1)
flags.DEFINE_string('bucket', None, 'AWS S3 Bucket for raw file storage.')
flags.DEFINE_string('prefix', 'edsm', 'AWS S3 path prefix.')
flags.DEFINE_string('queue', None, 'SQS queue to send messages.')
flags.DEFINE_enum('type',
                  None,
                  [
                      'systems', 'population', 'bodies',
                      'powerplay', 'stations', 'all'
                  ],
                  'Input file type.')
flags.mark_flag_as_required('bucket')
flags.mark_flag_as_required('queue')
flags.mark_flag_as_required('type')

# Global vars
sqs = boto3.resource('sqs')
s3 = boto3.resource('s3')


def process_batch(input_batch: list, sqs_queue: sqs.Queue):
  # Assemble output from valid JSON lines in input
  output_batch = []

  for line in input_batch:
    try:
      if FLAGS.type == 'systems':
        edsm_object = schema.system()
      elif FLAGS.type == 'population':
        edsm_object = schema.population()
      elif FLAGS.type == 'bodies':
        edsm_object = schema.body()
      elif FLAGS.type == 'powerplay':
        edsm_object = schema.powerplay()
      elif FLAGS.type == 'stations':
        edsm_object = schema.station()

      raw_data = re.search(r'(\{.*\})', line)
      edsm_object.from_json(raw_data.group(1))
      system_data = edsm_object.to_json()
      output_batch.append(system_data)
    except AttributeError as e:
      logging.warning(e)
      logging.warning('Malformed JSON string: %s', line)

  json_data = json.dumps(output_batch)

  # Send batch to SQS queue
  response = sqs_queue.send_message(MessageBody=json_data,
                                    MessageAttributes={
                                        'dataset': {
                                            'StringValue': 'edsm',
                                            'DataType': 'String'
                                        },
                                        'table': {
                                            'StringValue': FLAGS.type,
                                            'DataType': 'String'
                                        },
                                    })
  sqs_id = response.get('MessageId')
  logging.info('SQS MessageId: %s', sqs_id)


def file_parser(filetype: str) -> bool:
  output_batch = []
  file_path = '%s/%s.gz' % (FLAGS.prefix, filetype)
  file_object = s3.Object(FLAGS.bucket, file_path)
  file_body = file_object.get()['Body'].read()
  file_stream = io.BytesIO(file_body)

  logging.info('Parsing s3://%s/%s', FLAGS.bucket, file_path)
  with gzip.GzipFile(fileobj=file_stream, mode='rb') as file:
    for line in file:
      if len(output_batch) < FLAGS.batch_size:
        try:
          if filetype == 'systems':
            edsm_object = schema.system()
          elif filetype == 'population':
            edsm_object = schema.population()
          elif filetype == 'bodies':
            edsm_object = schema.body()
          elif filetype == 'powerplay':
            edsm_object = schema.powerplay()
          elif filetype == 'stations':
            edsm_object = schema.station()

          raw_data = re.search(r'(\{.*\})', line.decode())
          edsm_object.from_json(raw_data.group(1))
          system_data = edsm_object.to_json()
          output_batch.append(system_data)
        except AttributeError as e:
          logging.warning(e)
          logging.warning('Malformed JSON string: %s', line)
      else:
        queue_status = sqs_batch_send(filetype, output_batch)
        if queue_status:
          output_batch.clear()

    queue_status = sqs_batch_send(filetype, output_batch)

  return True


def sqs_batch_send(filetype: str, input_batch: list) -> bool:
  # Send batch to SQS queue
  sqs_queue = sqs.get_queue_by_name(QueueName=FLAGS.queue)
  json_data = json.dumps(input_batch)
  response = sqs_queue.send_message(MessageBody=json_data,
                                    MessageAttributes={
                                        'dataset': {
                                            'StringValue': 'edsm',
                                            'DataType': 'String'
                                        },
                                        'table': {
                                            'StringValue': filetype,
                                            'DataType': 'String'
                                        },
                                    })
  sqs_id = response.get('MessageId')
  logging.info('SQS MessageId: %s', sqs_id)

  return True


def main(argv):
  client = bigquery.Client()
  job_config = bigquery.LoadJobConfig()
  
  # del argv

  # if FLAGS.type == 'all':
  #   filetype_list = ['systems', 'population', 'bodies',
  #                    'powerplay', 'stations']
  #   for filetype in filetype_list:
  #     sqs_results = file_parser(filetype)
  # else:
  #   sqs_results = file_parser(FLAGS.type)


if __name__ == '__main__':
  app.run(main)
