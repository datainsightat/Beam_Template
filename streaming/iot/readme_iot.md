### Start Server

    $ source /env/bin/activate
    $ gcloud beta emulators pubsub start --project=myproject
    $ $(gcloud beta emulators pubsub env-init)

### Set Environment Variables

    $ gcloud beta emulators pubsub env-init
    $ export PUBSUB_EMULATOR_HOST=localhost:8085
    $ export PUBSUB_PROJECT_ID=myproject
    $ export PUBSUB_PATH=/opt/python-pubsub/samples/snippets/

### Create Topic, Subsrciption
 
    $ python $PUBSUB_PATH/publisher.py myproject create iot_topic
    $ python $PUBSUB_PATH/subscriber.py myproject create iot_topic iot_subscription
    $ python $PUBSUB_PATH/publisher.py myproject create iot_output_topic
    $ python $PUBSUB_PATH/subscriber.py myproject create iot_output_topic iot_output_subscription

### Publish Message

    $ python $PUBSUB_PATH/publisher.py myproject publish iot_topic

### Read Message

    $ python $PUBSUB_PATH/subscriber.py myproject receive iot_subscription