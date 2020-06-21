import json
import pprint

from absl import app, flags, logging

from edsm import schema
from lib import bigquery, sqs

# Define flags
FLAGS = flags.FLAGS
flags.DEFINE_string(
    'service_account_secret',
    None,
    'Name of GCP Service Account information stored in AWS Secrets Manager.',
)
flags.DEFINE_string('sqs_queue', None, 'SQS message queue to read from.')
flags.mark_flag_as_required('sqs_queue')
flags.mark_flag_as_required('service_account_secret')


def load_rows(
    bigquery_client: bigquery.client,
    row_batch: list,
    attributes: dict,
):
    # Configure BigQuery based on message metadata
    dataset_name = attributes.get('dataset', {}).get('StringValue')
    table_name = attributes.get('table', {}).get('StringValue')

    # Get table schema
    # TODO(mjcastner): Break out to a locally scoped function when multiple
    # sources are added (e.g. Canonn)
    edsm_object = schema.edsmObject(table_name)
    bigquery_client.safe_get_dataset('{project}.{dataset}'.format(
        project=bigquery_client.project,
        dataset=dataset_name,
      ),
    )
    table = bigquery_client.safe_get_table(
        '{project}.{dataset}.{table}'.format(
            project=bigquery_client.project,
            dataset=dataset_name,
            table=table_name,
        ),
        edsm_object.schema,
    )

    # Load rows
    streaming_errors = bigquery_client.insert_rows(table, row_batch)

    if streaming_errors:
        return False
    elif not streaming_errors:
        return True


def main(argv):
    bigquery_client = bigquery.client(FLAGS.service_account_secret)
    queue_size = sqs.get_queue_size(queue_name=FLAGS.sqs_queue)
    logging.info(
        'Streaming to BigQuery from "{queue}" SQS queue...'.format(
            queue=FLAGS.sqs_queue,
        ),
    )

    while queue_size > 0:
        message_batch = sqs.receive_message(queue_name=FLAGS.sqs_queue)

        for message in message_batch:
            row_batch = json.loads(message.body)
            logging.info(
                'Loading batch of {batch_size} rows...'.format(
                  batch_size=len(row_batch),
                ),
            )

            streaming_status = load_rows(
                bigquery_client,
                row_batch,
                message.message_attributes,
            )

            if streaming_status:
                logging.info('Rows loaded successfully!')
                message.delete()

        queue_size = sqs.get_queue_size(queue_name=FLAGS.sqs_queue)

    logging.info(
        'SQS queue {queue_name} contains no additional messages.'.format(
            queue_name=FLAGS.sqs_queue,
        ),
    )


if __name__ == '__main__':
    app.run(main)
