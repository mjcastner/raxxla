FROM python:3

ADD ./app /raxxla
WORKDIR /raxxla
ENV PYTHONPATH="/raxxla"
ENV GOOGLE_APPLICATION_CREDENTIALS="/gcp/credentials.json"
RUN pip install \
    'absl-py==0.9.0' \
    'pandas==1.0.5' \
    'google-cloud-bigquery==1.26.0' \
    'google-cloud-pubsub==1.7.0' \
    'google-cloud-storage==1.29.0'
ENTRYPOINT ["python"]
