import boto3
from lib import sqs

#test = sqs.send_message('raxxla', 'test')
test2 = sqs.receive_message('raxxla')

for message in test2:
  print(message)
  print(message.body)
  print(type(message))
  print(dir(message))
  print('\n\n')
  message.delete()
