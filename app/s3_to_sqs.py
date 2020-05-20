import codecs
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
flags.DEFINE_string('filepath', None, 'Path to JSON input file.')
flags.DEFINE_string('queue', None, 'SQS queue to send messages.')
flags.DEFINE_enum('type',
                  None,
                  ['systems', 'population', 'bodies', 'powerplay', 'stations'],
                  'Input file type.')
flags.mark_flag_as_required('bucket')
flags.mark_flag_as_required('filepath')
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


def main(argv):
  del argv

  # Instantiate SQS queue
  sqs_queue = sqs.get_queue_by_name(QueueName=FLAGS.queue)
  
  # Process S3 input file
  logging.info('Processing s3://%s/%s...', FLAGS.bucket, FLAGS.filepath)
  file_object = s3.Object(FLAGS.bucket, FLAGS.filepath)
  file_body = file_object.get()['Body']

  json_batch = []
  for line in codecs.getreader('utf-8')(file_body):
    print(line)
    print()
  #   if len(json_batch) < FLAGS.batch_size:
  #     json_batch.append(line)
  #   else:
  #     process_batch(json_batch, sqs_queue)
  #     json_batch.clear()

  # process_batch(json_batch, sqs_queue)


if __name__ == '__main__':
  app.run(main)
