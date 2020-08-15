export GCP_PROJECT=my-project
export GCS_BUCKET=my-bucket
export GCP_REGION=us-central1

run-direct:
	rm -rf output && python pipeline.py

run-dataflow:
	python pipeline.py \
	--input=gs://dataflow-samples/shakespeare/kinglear.txt \
	--output=gs://$(GCS_BUCKET)/wordcount/output \
	--runner=DataFlowRunner \
	--project=$(GCP_PROJECT) \
	--staging_location=gs://$(GCS_BUCKET)/wordcount/_staging \
	--temp_location=gs://$(GCS_BUCKET)/wordcount/_temp \
	--job_name=word-count \
	--region=$(REGION)