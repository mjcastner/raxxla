from lib import sqs

client = sqs.sqs_client(
    queue_name='raxxla',
    queue_url='https://sqs.us-west-2.amazonaws.com/209604952396/raxxla')

print(dir(client))
print(client.available_messages)
