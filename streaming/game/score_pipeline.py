import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os
from apache_beam import window
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount, Repeatedly
from datetime import datetime
import logging

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(name)s : %(message)s", level=logging.INFO
)
logging = logging.getLogger(__name__)

def run():
        
    # Replace with your service account path
    # service_account_path = ''

    # print("Service account file : ", service_account_path)
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path

    input_subscription = 'projects/myproject/subscriptions/game_subscription'

    output_player_score_topic = 'projects/myproject/topics/output_player_score_topic'
    output_team_score_topic = 'projects/myproject/topics/output_team_score_topic'

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    p = beam.Pipeline(options=options)

    def custom_timestamp(elements):
        unix_timestamp = elements[16].rstrip().lstrip()
        return beam.window.TimestampedValue(elements, int(unix_timestamp))

    def encode_byte_string(element):
        element = str(element)
        print(element)
        return element.encode('utf-8')

    def player_pair(element_list):
        return element_list[1], 1

    def score_pair(element_list):
        return element_list[3], 1

    logging.info("-----------------------------------------------------------------")
    logging.info("          Dataflow Streaming Game Score with Pub/Sub             ")
    logging.info("-----------------------------------------------------------------")

    pubsub_data = (
        p
        | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription = input_subscription)
        | 'Parse data' >> beam.Map(lambda element: element.decode('ascii').split(','))
        | 'Apply custom timestamp' >> beam.Map(custom_timestamp)
    )

    player_score = (
        pubsub_data
        | 'Form k,v pair of player (player_id, 1)' >> beam.Map(player_pair)
        | 'Window for player' >> beam.WindowInto(window.GlobalWindows(), trigger=Repeatedly(AfterCount(10)), accumulation_mode=AccumulationMode.ACCUMULATING)
        | 'Group payers and their score' >> beam.CombinePerKey(sum)
        | 'Encode player info to byte string' >> beam.Map(encode_byte_string)
        | 'Write player score to pub sub' >> beam.io.WriteToPubSub(output_player_score_topic)
    )

    team_score = (
        pubsub_data
        | 'From k,v pair ot (team_score, 1)' >> beam.Map(score_pair)
        | 'Window for team' >> beam.WindowInto(window.GlobalWindows(), trigger=Repeatedly(AfterCount(10)), accumulation_mode=AccumulationMode.ACCUMULATING)
        | 'Group teams and their score' >> beam.CombinePerKey(sum)
        | 'Encode teams info to byte string' >> beam.Map(encode_byte_string)
        | 'Write team score to pub sub' >> beam.io.WriteToPubSub(output_team_score_topic)
    )

    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":

    # ./env.sh
    # python $PUBSUB_PATH/publisher.py myproject create retail_topic
    # python $PUBSUB_PATH/subscriber.py myproject create retail_topic retail_subscription
    # python $PUBSUB_PATH/publisher.py myproject create retail_output_topic
    # python $PUBSUB_PATH/subscriber.py myproject create retail_topic retail_output_subscription

    os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8085"
    os.environ["PUBSUB_PROJECT_ID"] = "myproject"
    os.environ["PUBSUB_PATH"] = "/opt/python-pubsub/samples/snippets/"

    run()