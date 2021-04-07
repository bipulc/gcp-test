#!/usr/bin/env python3
'''
script to receive messages from a pubsub subscription
subscription name - projects/data-analytics-bk/subscriptions/rt-mt-data-ingest-subs
'''

from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

project_id = 'data-analytics-bk'
subscription_id = 'rt-mt-data-ingest-subs'
# number of seconds the subscriber should listen for messages
timeout = 30.0

subscriber = pubsub_v1.SubscriberClient()
subscriber_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message):
    print(f"Received {message}.")
    message.ack()
    # Raise TimeoutError after one message has been recieved. However given that it is async, multiple call back may happen simultaneously.
    streaming_pull_future.set_exception(TimeoutError)

streaming_pull_future = subscriber.subscribe(subscriber_path, callback=callback)
print(f"Listening for messages on {subscriber_path}.. \n")

# Wrap subscriber in a 'with' block to automatically call close() when done.

with subscriber:
    try:
        # When 'timeout is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result(timeout=timeout)
        # The following line will never be executed
        print("Done with pulling ...\n")
    except TimeoutError:
        streaming_pull_future.cancel()
        print("Done with pulling in TimeoutError Exception...\n")