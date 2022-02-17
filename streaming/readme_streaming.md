### Start Server

    $ source env/bin/activate
    $ gcloud beta emulators pubsub start --project=myproject
    $ $(gcloud beta emulators pubsub env-init)

### Set Environment Variables

    $ gcloud beta emulators pubsub env-init
    $ export PUBSUB_EMULATOR_HOST=localhost:8085
    $ export PUBSUB_PROJECT_ID=myproject
    $ export PUBSUB_PATH=/opt/python-pubsub/samples/snippets/

### Create Topic
 
    $ python $PUBSUB_PATH/publisher.py myproject create mytopic

### Create Subsrciption

    $ python $PUBSUB_PATH/subscriber.py myproject create mytopic mysubscription

### Publish Message

    $ python $PUBSUB_PATH/publisher.py myproject publish mytopic

### Read Message

    $ python $PUBSUB_PATH/subscriber.py myproject receive mysubscription