# Word Count
This pipeline will count will count the number of occurrences of each word in a given input text.

**Install Apache Beam Python SDK:**
```
pip install apache-beam

# or use command below if you want to run it on GCP (Cloud Dataflow)
pip install apache-beam[gcp]
```

**Run using Direct Runner (local)**
```
python pipeline.py
```

**Run on Cloud Dataflow**
```
python pipeline.py \
--input=gs://dataflow-samples/shakespeare/kinglear.txt \
--output=gs://my-bucket/wordcount/output \
--runner=DataFlowRunner \
--project=my-project \
--staging_location=gs://my-bucket/wordcount/_staging \
--temp_location=gs://my-bucket/wordcount/_temp \
--job_name=word-count \
--region=us-central1
```

Or use the `Makefile` as the shortcut command.