FROM python:3

ADD ./app /raxxla
WORKDIR /raxxla
ENV PYTHONPATH="/raxxla"
ENV GOOGLE_APPLICATION_CREDENTIALS="/gcp/credentials.json"
RUN pip install absl-py google-cloud-bigquery google-cloud-pubsub
ENTRYPOINT ["python"]
