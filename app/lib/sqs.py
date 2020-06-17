import boto3

sqs = boto3.client('sqs')


class sqs_client(sqs):
  def __init__(self, queue_name: str, queue_url: str):
    # Initialize SQS client
    super().__init__()
    self.queue = sqs.get_queue_by_name(QueueName=queue_name)
    self.available_messages = int(
        sqs_queue.attributes.get('ApproximateNumberOfMessages'))
