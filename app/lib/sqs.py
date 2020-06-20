from botocore import exceptions as boto_exceptions
import boto3


sqs_client = boto3.resource('sqs')


def _get_queue(queue_name: str):
  return sqs_client.get_queue_by_name(QueueName=queue_name)


def get_queue_size(queue_name: str) -> int:
  message_queue = _get_queue(queue_name)
  return int(message_queue.attributes.get('ApproximateNumberOfMessages'))


def receive_message(queue_name: str,
                    batch_size: int = 10):
  message_queue = _get_queue(queue_name)
  messages = message_queue.receive_messages(MaxNumberOfMessages=batch_size,
                                            MessageAttributeNames=['All'])

  return messages


def send_message(queue_name: str,
                 message_content: str,
                 message_attributes: dict = {}):
  response = None
  message_queue = _get_queue(queue_name)
  response = message_queue.send_message(MessageBody=message_content,
                                        MessageAttributes=message_attributes)

  return response
