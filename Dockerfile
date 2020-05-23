FROM python:3

ADD ./app /raxxla
WORKDIR /raxxla
RUN pip install boto3 absl-py google-cloud-bigquery
ENTRYPOINT ["python"]
CMD ["edsm_to_s3.py", "--help"]
