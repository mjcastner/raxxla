FROM python:3.7-buster
ADD backend/batch/edsm/ /app
ADD protos/ /app/protos
ADD commonlib/ /app/commonlib
WORKDIR /app
RUN pip install -r requirements.txt \
    && apt-get update \
    && apt-get install -y protobuf-compiler \
    && protoc -I protos --python_out protos protos/*.proto \
    && ls
ENV PYTHONPATH="/app"
ENTRYPOINT ["python"]
