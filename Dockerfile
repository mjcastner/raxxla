FROM python:3

ADD ./app /raxxla
WORKDIR /raxxla
RUN pip install boto3 absl-py
ENTRYPOINT ["python"]
