FROM python:3

ADD ./app /raxxla
WORKDIR /raxxla
RUN pip install boto3 absl-py
ENTRYPOINT ["python"]
CMD ["s3_to_sqs.py", "--help"]
