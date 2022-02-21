# Beam_Template
Examples for the Beam_Docker repository

# Start Testing

## Start PubSub Emulator

New Terminal:

    $ source env/bin/activate
    $ gcloud beta emulators pubsub start --project=myproject

## Setup PubSub

New Terminal

    $ source /env/bin/activate
    $ $(gcloud beta emulators pubsub env-init)

### Create Raw Topic, Subsrcription

    $ python /opt/python-pubsub/samples/snippets/publisher.py myproject create retail_topic
    $ python /opt/python-pubsub/samples/snippets/subscriber.py myproject create retail_topic retail_subscription

### Create Output Topic, Subsciption

    $ python /opt/python-pubsub/samples/snippets/publisher.py myproject create retail_output_topic
    $ python /opt/python-pubsub/samples/snippets/subscriber.py myproject create retail_output_topic retail_output_subscription

### Read Message

    $ python subscriber.py myproject receive retail_subscription

### Start Pipeline

    $ python pipeline.py --allow_unsafe_triggers

### Publish Message

#### Test

    $ python /opt/python-pubsub/samples/snippets/publisher.py myproject publish retail_topic

#### Retail Example

    $ python publish.py
