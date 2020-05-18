import json
import re

from absl import app
from absl import flags
from absl import logging
import boto3
import schema

# Define args
FLAGS = flags.FLAGS
flags.DEFINE_integer('batch_size',
                     500,
                     'Default job batch size for processing and loading',
                     lower_bound=1)
flags.DEFINE_string('queue_name', None, 'Message queue to send output.')
flags.DEFINE_string('input_json', None, 'Path to JSON input file.')
flags.DEFINE_enum('input_type',
                  None,
                  ['systems', 'population', 'bodies', 'powerplay', 'stations'],
                  'Input file type.')
flags.mark_flag_as_required('queue_name')
flags.mark_flag_as_required('input_json')
flags.mark_flag_as_required('input_type')

# Global vars
sqs = boto3.resource('sqs')


def process_batch(input_batch: list, sqs_queue: sqs.Queue):
  # Assemble output from valid JSON lines in input
  output_batch = []

  for line in input_batch:
    try:
      if FLAGS.input_type == 'systems':
        edsm_object = schema.system()
      elif FLAGS.input_type == 'population':
        edsm_object = schema.population()
      elif FLAGS.input_type == 'bodies':
        edsm_object = schema.body()
      elif FLAGS.input_type == 'powerplay':
        edsm_object = schema.powerplay()
      elif FLAGS.input_type == 'stations':
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
                                            'StringValue': FLAGS.input_type,
                                            'DataType': 'String'
                                        },
                                    })
  sqs_id = response.get('MessageId')
  logging.info('SQS MessageId: %s', sqs_id)


def main(argv):
  del argv

  # Instantiate SQS queue
  sqs_queue = sqs.get_queue_by_name(QueueName=FLAGS.queue_name)

  with open(FLAGS.input_json, 'r') as systems_file:
    # Process lines in multithreaded batches
    json_batch = []
    for line in systems_file:
      if len(json_batch) < FLAGS.batch_size:
        json_batch.append(line)
      else:
        process_batch(json_batch, sqs_queue)
        json_batch.clear()

    # Catch any leftover items in final batch
    process_batch(json_batch, sqs_queue)


if __name__ == '__main__':
  app.run(main)
