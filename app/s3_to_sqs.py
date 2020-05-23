import io
import json
import re

from lib import schema
from absl import app
from absl import flags
from absl import logging
import boto3

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


def file_parser(filetype: str) -> list:
  output_batch = []

  file_path = '%s/%s.gz' % (FLAGS.prefix, filetype)
  print(file_path)
  logging.info('Parsing s3://%s/%s', FLAGS.bucket, file_path)
  file_object = s3.Object(FLAGS.bucket, file_path)
  file_body = file_object.get()['Body'].read()
  
  gzipfile = BytesIO(file_body)
  gzipfile = gzip.GzipFile(fileobj=gzipfile)
  content = gzipfile.read()
  print(content)
  return output_batch


def sqs_batch_send(input_list: list) -> bool:
  print(input_list)
  return True


def main(argv):
  del argv
  sqs_queue = sqs.get_queue_by_name(QueueName=FLAGS.queue)

  test = file_parser(FLAGS.type)
  print(test)

  # Process S3 input file
  # logging.info('Processing s3://%s/%s...', FLAGS.bucket, FLAGS.filepath)
  # file_object = s3.Object(FLAGS.bucket, FLAGS.filepath)
  # file_body = file_object.get()['Body'].iter_lines()

  # json_batch = []
  # for line in file_body:
  #   if len(json_batch) < FLAGS.batch_size:
  #     json_batch.append(line.decode())
  #   else:
  #     print(json_batch)
  #     process_batch(json_batch, sqs_queue)
  #     json_batch.clear()

  # process_batch(json_batch, sqs_queue)


if __name__ == '__main__':
  app.run(main)
