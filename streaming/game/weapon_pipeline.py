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

    output_topic = 'projects/myproject/topics/output_weapon_score_topic'

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    p = beam.Pipeline(options=options)

    def custom_timestamp(elements):
        unix_timestamp = elements[16].rstrip().lstrip()
        return beam.window.TimestampedValue(elements,int(unix_timestamp))

    def encode_byte_string(element):
        element = str(element)
        print(element)
        return element.encode('utf-8')

    def calculate_battle_points(element_list):
        
        total_points = 0
        game_id = element_list[0]
        player_id = element_list[1]
        weapon = element_list[5]
        
        my_weapon_ranking = element_list[6].rstrip().lstrip()
        my_weapon_ranking = int(my_weapon_ranking)
        opp_weapon_ranking = element_list[13].rstrip().lstrip()
        opp_weapon_ranking = int(opp_weapon_ranking)

        my_map_location = element_list[7].rstrip().lstrip()
        opp_map_location = element_list[14].rstrip().lstrip()

        battle_time = element_list[15]
        battle_time = int(battle_time.rstrip().lstrip())

        if battle_time >= 10 and battle_time <= 20:
            total_points += 4
        elif battle_time >=21 and battle_time <= 30:
            total_points += 3
        elif battle_time >=31 and battle_time <= 40:
            total_points += 2
        elif battle_time > 40:
            total_points += 1
        
        diff = my_weapon_ranking - opp_weapon_ranking

        if diff >= 6:
            total_points += 3
        elif diff >= 3:
            total_points += 2
        else:
            total_points += 1
        
        if my_map_location != opp_map_location:
            total_points += 3
        
        return game_id + ':' + player_id + ':' + weapon, total_points

    class PointFn(beam.CombineFn):

        def create_accumulator(self):
            return(0.0, 0)
        
        def add_input(self, sum_count, input):
            (sum, count) = sum_count
            return sum + input, count + 1
        
        def merge_accumulators(self, accumulators):
            sums, counts = zip(*accumulators)
            return sum(sums), sum(counts)
        
        def extract_output(self, sum_count):
            (sum, count) = sum_count
            return sum / count if count else float('NaN')
        
    def format_result(key_value_pair):

        name, points = key_value_pair
        name_list = name.split(':')
        game_id = name_list[0]
        player_id = name_list[1]
        weapon = ' '.join(name_list[2:])
        return game_id + ',' + player_id + ', ' + weapon + ', ' + str(points) + ' average battle points '

    logging.info("------------------------------------------------------------------")
    logging.info("          Dataflow Streaming Game Weapon with Pub/Sub             ")
    logging.info("------------------------------------------------------------------")

    pubsub_data = (
        p 
        | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription= input_subscription)
        # GM_1,PL_1,Allison,TM_01,Blasters,BN60,6,MP_100,PL_16,Odette,TM_03,Masters,Bomb,1,MP_113,20,1553578221/r/n

        | 'Parse data' >> beam.Map(lambda element: element.decode('ascii').split(','))
        # | 'Apply custom timestamp' >> beam.Map(custom_timestamp)
        | 'Calculate battle points' >> beam.Map(calculate_battle_points)        #  Key = GM_1:PL_1:BN60 value = 9 
        | 'Window for player' >> beam.WindowInto(window.Sessions(30))
        | 'Group by key' >> beam.CombinePerKey(PointFn())                    # output --> GM_1:PL_1:BN60, average points
        | 'Format results' >> beam.Map(format_result)    
        | 'Encode data to byte string' >> beam.Map(encode_byte_string)
        | 'Write player score to pub sub' >> beam.io.WriteToPubSub(output_topic)
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