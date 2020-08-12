# Raxxla: Elite Dangerous data analysis pipeline

## Getting started
To setup your initial working directory, you'll need Git and Docker installed.
```shell
git clone https://github.com/mjcastner/raxxla.git;
docker build -t raxxla:latest ./raxxla/;
```

## Google Cloud setup
To bootstrap the required GCP resources, you'll need to install the
[Google Cloud SDK](https://cloud.google.com/sdk/docs/downloads-docker).

```shell
gcloud auth;
docker run raxxla:latest gcp/initial_setup.py --help;

cd app/gcp/bigquery_row_streamer;
gcloud functions deploy bigquery_row_streamer --runtime python37 --trigger-topic raxxla;
```

Once the image is built, you'll be able to run any of the
[available commands](#commands).


## Commands
