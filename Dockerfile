FROM python:3

WORKDIR /raxxla
COPY ./app /raxxla
RUN pip install boto3 absl-py
RUN ls && pwd
ENTRYPOINT ["python"]
