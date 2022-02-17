import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions
import argparse
import os
import sys
from apache_beam import window
import logging

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(name)s : %(message)s", level=logging.INFO
)
logging = logging.getLogger(__name__)

class JobOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--input", required=True, help="pubsub input topic")
        parser.add_argument("--output", required=True, help="pubsub outut topic")

def run(argv=None, save_main_session=True):

    argv = sys.argv[1:]

    argv.append('--runner=Direct')
    argv.append('--input=projects/myproject/subscriptions/mysubscription')
    argv.append('--output=projects/myproject/topics/myoutputtopic')
    argv.append('--streaming')

    # Replace 'my-service-account-path' with your service account path
    #service_account_path = 'my-service-account-path'
    #print("Service account file : ", service_account_path)
    #os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path

    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session#.view_as(StandardOptions).streaming = True
    job_options = pipeline_options.view_as(JobOptions)

    logging.info("------------------------------------------------------")
    logging.info("          Dataflow Streaming with Pub/Sub             ")
    logging.info("------------------------------------------------------")

    p = beam.Pipeline(options=pipeline_options)

    pubsub_data = (
                    p 
                    | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription=str(job_options.input))
                    | 'Write to pub sub' >> beam.io.WriteToPubSub(str(job_options.output))
                    #| 'Write to file' >> beam.io.WriteToText("data/pubsub")
                  )

    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":

  os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8085"
  os.environ["PUBSUB_PROJECT_ID"] = "myproject"

  run()