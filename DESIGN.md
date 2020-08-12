# Design

## Google Cloud Storage (GCS)
GCS is used to store raw data from EDSM, Canonn and other community-managed
sources of Elite Dangerous information.  It serves as a
[data lake](https://en.wikipedia.org/wiki/Data_lake) and has an added bonus of
great [object lifecycle management](https://cloud.google.com/storage/docs/lifecycle).

By default, objects are only kept in GCS for 48 hours, as the EDSM files in
particular are quite large and expensive to store.

## Google Container Registry (GCR)
GCR is used to store Docker images that can be run in Google Cloud Run.

## Google Cloud Run
Cloud Run is used to manage, scale and run our ETL pipeline jobs.  These jobs
utilize the defined images in GCR to download and transform Elite Dangerous
data into formatted JSON and sent to Pub/Sub.

## Pub/Sub
Pub/Sub serves as a message broker for streaming formatted JSON messages into
BigQuery via [row streaming](https://cloud.google.com/bigquery/streaming-data-into-bigquery).
Streaming is used in place of batch loads due to the structure of the EDSM
files, which are large single JSON objects instead of
[NDJson](http://ndjson.org/), and thus require us to go line-by-line
anyway.

## Cloud Functions
Google Cloud Functions is used to host a BigQuery row streaming function,
which takes batches of formatted JSON data and streams directly into
date-partitioned BigQuery tables.

## (Optional) Google Cloud Scheduler
Cron files are included to enable regular updates from upstream data sources.
