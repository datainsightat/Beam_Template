import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os
from apache_beam import window
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount, Repeatedly
import logging

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(name)s : %(message)s", level=logging.INFO
)
logging = logging.getLogger(__name__)

def run():

    # # Replace with your service account path
    # service_account_path = ''

    # print("Service account file : ", service_account_path)
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path

    input_subscription = 'projects/myproject/subscriptions/retail_subscription'

    output_topic = 'projects/myproject/topics/retail_output_topic'

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    p = beam.Pipeline(options=options)

    def encode_byte_string(element):
        element = str(element)
        return element.encode('utf-8')

    def custom_timestamp(elements):
        unix_timestamp = elements[7]
        return beam.window.TimestampedValue(elements, int(unix_timestamp))

    def calculate_profit(elements):
        buy_rate = elements[5]
        sell_price = elements[6]
        products_count = int(elements[4])
        profit = (int(sell_price) - int(buy_rate)) * products_count
        elements.append(str(profit))
        return elements

    logging.info("-------------------------------------------------------------")
    logging.info("          Dataflow Streaming Retail with Pub/Sub             ")
    logging.info("-------------------------------------------------------------")

    pubsub_data  =(
        p
        | "Read from pub sub" >> beam.io.ReadFromPubSub(subscription=input_subscription, timestamp_attribute = '1553578219')
        | "Remove extra chars" >> beam.Map(lambda data:(data.rstrip().lstrip()))
        | "Split row" >> beam.Map(lambda row:row.decode('ascii').split(','))
        | "Filter by country" >> beam.Filter(lambda elements:(elements[1]=="Mumbai" or elements[1]=="Bangalore"))
        | "Create profit column" >> beam.Map(calculate_profit)
        # | "Apply custom timestamp" >> beam.Map(custom_timestamp)
        | "Form key value pair" >> beam.Map(lambda elements:(elements[3], int(elements[8])))
        # | "Window Fixed" >> beam.WindowInto(window.FixedWindows(20))
        # | "Window Sliding" >> beam.WindowInto(window.SlidingWindows(30,10))
        # | "Window Session" >> beam.WindowInto(window.Sessions(25))
        | "Window Global" >> beam.WindowInto(window.GlobalWindows(), trigger=Repeatedly(AfterCount(5)), accumulation_mode=AccumulationMode.DISCARDING)
        | "Sum values" >> beam.CombinePerKey(sum)
        | "Encode to byte string" >> beam.Map(encode_byte_string)
        | "Write to pub sub" >> beam.io.WriteToPubSub(output_topic)
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