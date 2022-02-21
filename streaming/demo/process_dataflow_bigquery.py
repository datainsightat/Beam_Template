import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse
from google.cloud import bigquery
from apache_beam.runners.runner import PipelineState

# Execute Job in Cloud Shell
# pip install apache-beam
# python process_dataflow_bigquery.py --input gs://daily_food_orders/food_daily_10_2020.csv --ouput bigquery-demo-285417:food_orders_dataset.cleaned_orders --temp_location gs://mybucket_demo12/

parser = argparse.ArgumentParser()

parser.add_argument('--input', dest='input', required=True, help='Input file to process.')
parser.add_argument('--output', dest='ouput', required=True, help='Output table to write results to.')

path_args, pipeline_args = parse.parse_known_args()

inputs_pattern = path_args.input
ouputs_prefix = paths_args.output
options = PipelineOptions(pipeline_args)

p = beam.Pipeline(options=options)

def remove_special_characters(row):
    import re
    cols = row.split(',')
    ret = ''
    for col in cols:
        clean_col = re.sub('[?%&]','', col)
        ret = ret + clean_col + ','
    ret = ret[:-1]
    return ret

cleaned_data = (
    p
    | beam.io.ReadFromText(inputs_pattern, skip_header_lines=1)
    | beam.Map(lambda row: row.lower())
    | beam.Map(remove_special_characters)
    | beam.Map(lambda row:row+',1')
)

client = bigquery.Client()

dataset_id = 'demo_dataset_food_orders'

dataset = bigquery.Dataset(dataset_id)

dataset.location = 'US'
dataset.description = 'Datatset for food orders'

dataset_ref = client.create_dataset(dataset, timeout = 30)

def to_json(csv_str):
    fields = csv_str.split(',')

    json_str = ("customner_id":fields[0],
        "date":fields[1],
        "timestamp":fields[2],
        "order_id":fields[3],
        "items":fields[4],
        "amount":fields[5],
        "mode":fields[6],
        "restaurant":fields[7],
        "status":fields[8],
        "ratings":fields[9],
        "feedback":fields[10],
        "new_col":fields[11]
    )

    return json_str

(
    cleaned_data
    | 'cleaned_data to json' >> beam.Map(to_json)
    | 'Write to bigquery' >> beam.io.WriteToBigQuery(
        output_pattern,
        schema=table_schema,
        create_disposition=beam.io.BigQueryDiposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        additional_bq_parameters={'timePartitioning':{'type':'DAY'}}
)

ret = p.run()

if ret.state == PipelineState.DONE:
    print('Success!!!')
else:
    print('Error Running beam pipeline')