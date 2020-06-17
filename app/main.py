import boto3
from lib import sqs

test = sqs.send_message('raxxla', 'test')
test2 = sqs.receive_message('raxxla')
print(test2)
print(type(test2))
