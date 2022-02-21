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
    $ gcloud beta emulators pubsub env-init
    $ export PUBSUB_EMULATOR_HOST=localhost:8085
    $ export PUBSUB_PROJECT_ID=myproject
    $ export PUBSUB_PATH=/opt/python-pubsub/samples/snippets/

### Create Raw Topic, Subsrcription

    $ python $PUBSUB_PATH/publisher.py myproject create game_topic
    $ python $PUBSUB_PATH/subscriber.py myproject create game_topic game_subscription

### Create Output Topic, Subsciption

    $ python $PUBSUB_PATH/publisher.py myproject create output_player_score_topic
    $ python $PUBSUB_PATH/subscriber.py myproject create output_player_score_topic output_player_score_subscription
    $ python $PUBSUB_PATH/publisher.py myproject create output_team_score_topic
    $ python $PUBSUB_PATH/subscriber.py myproject create output_team_score_topic output_team_score_subscription
    $ python $PUBSUB_PATH/publisher.py myproject create output_weapon_score_topic
    $ python $PUBSUB_PATH/subscriber.py myproject create output_weapon_score_topic output_weapom_score_subscription

### Read Message

    $ python $PUBSUB_PATH/subscriber.py myproject receive output_player_score_subscription

### Start Pipeline

    $ python score_pipeline.py --allow_unsafe_triggers

### Publish Message

#### Test

    $ python $PUBSUB_PATH/publisher.py myproject publish retail_topic

#### Game Example

    $ python publish.py
