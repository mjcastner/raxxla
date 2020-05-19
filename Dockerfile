FROM python:3

ADD ./app /raxxla
WORKDIR /raxxla
RUN pip install boto3 absl-py
RUN ls && pwd
ENTRYPOINT ["python"]
RUN ls && pwd
CMD ["s3_to_sqs.py", "--help"]
