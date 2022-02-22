#!/bin/sh

sudo apt-get install -y python3-venv
python3 -m venv df-env
source df-env/bin/activate

python3 -m pip install -q --upgrade pip setuptools wheel
python3 -m pip install apache-beam[gcp]

gcloud services enable dataflow.googleapis.com

PROJECT_ID=$(gcloud config get-value project)
export PROJECT_NUMBER=$(gcloud projects list --filter="$PROJECT_ID" --format="value(PROJECT_NUMBER)")
export serviceAccount=""$PROJECT_NUMBER"-compute@developer.gserviceaccount.com"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${serviceAccount}" --role="roles/dataflow.worker"

python3 iot_pipeline.py \
--input projects/myproject/subscriptions/iot_subscription \
--output myproject:iot_dataset.iot_table \
--runner DataflowRunner \
--project myproject \
--temp_location gs://mybucket/tmp \
--streaming \
--region europe-west6 \
--experiment use_unsupported_python_version