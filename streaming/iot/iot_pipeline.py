# python $PUBSUB_PATH/publisher.py myproject create iot_topic
# python $PUBSUB_PATH/subscriber.py myproject create iot_topic iot_subscription

############################
# Setup Python Environment #
############################

#######################################
# gcp > Activate Cloud Shell > Editor #
# Upload iot_pipeline.py              #
# Upload iot_pipeline.sh              #
#######################################

# ./iot_pipeline.sh

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os
from apache_beam import window
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount, Repeatedly
from google.cloud import bigquery

import logging
import json
import argparse

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(name)s : %(message)s", level=logging.INFO
)

logging = logging.getLogger(__name__)

def run():

    # # Replace with your service account path
    # service_account_path = ''

    # print("Service account file : ", service_account_path)
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path

    # input_subscription = 'projects/myproject/subscriptions/iot_subscription'

    # options = PipelineOptions()
    # options.view_as(StandardOptions).streaming = True

    parser = argparse.ArgumentParser()

    parser.add_argument('--input',dest='input',required=True,help='Input PubSub Subscription')
    parser.add_argument('--output',dest='output',required=True,help='Output BigQuery Table')

    path_args, pipeline_args = parser.parse_known_args()

    input_subscription = path_args.input
    output_table = path_args.output

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True
    p = beam.Pipeline(options=options)

    def clean_json(elements):

        data = json.loads(elements.decode('utf-8'))

        if (data['eventType'] == 'DEVICE_LOCATION_UPDATE'):

            recordtimestamp = data['recordTimestamp']

            data_device = data['deviceLocationUpdate']
            device = data_device['device']
            location = data_device['location']

            macaddress = device['macAddress'].replace(':','_')
            locationid = location['locationId']
            lastseen = data_device['lastSeen']

            xpos = data_device['xPos']
            ypos = data_device['yPos']
            confidence = data_device['confidenceFactor']

            data_json = {
                "locationid":locationid,\
                "macaddress":macaddress,\
                "recordtimestamp":recordtimestamp,\
                "lastseen":lastseen,\
                "xpos":xpos,\
                "ypos":ypos,\
                "confidence":confidence}

            logging.info(data_json)

            return(data_json)

        # else:

        #     return(NULL)

    logging.info("----------------------------------------------------------")
    logging.info("          Dataflow Streaming IoT with Pub/Sub             ")
    logging.info("----------------------------------------------------------")

    table_schema = 'locationid:STRING,macaddress:STRING,recordtimestamp:INTEGER,lastseen:INTEGER,xpos:FLOAT,ypos:FLOAT,confidence:FLOAT'

    # if (1 == 2):

    #     logging.info("Create BigQuery Table")

    #     client = bigquery.Client()

    #     dataset_id = "bigquery-demo-285417.food_orders_dataset"

    #     dataset = bigquery.Dataset(dataset_id)
    #     dataset.location = "US"
    #     dataset.description = "dataset for food orders"

    #     dataset_ref = client.create_dataset(dataset, timeout = 30)

    pubsub_data  = (
        p
        | "Read from pub sub" >> beam.io.ReadFromPubSub(subscription=input_subscription)
        | "Clean json file" >> beam.Map(clean_json)
        | 'Write to bigquery' >> beam.io.WriteToBigQuery(\
            output_table,\
            schema=table_schema,\
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,\
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,\
            additional_bq_parameters={'timePartitioning':{'type':'DAY'}})
    )

    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":

    os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8085"

    run()