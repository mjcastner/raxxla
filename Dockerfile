FROM python:3.7-buster
ADD ./app /app
WORKDIR /app
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir -r commonlib/google/requirements.txt \
    && apt-get update \
    && apt-get install -y protobuf-compiler \
    && protoc -I protos --python_out protos protos/*.proto
ENV PYTHONPATH="/app"
ENTRYPOINT ["python"]
