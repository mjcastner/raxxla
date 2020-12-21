FROM python:3-buster
ADD ./app /app
WORKDIR /app
RUN pip install -r requirements.txt \
    && pip install -r commonlib/google/requirements.txt \
    && apt-get update \
    && apt-get install -y protobuf-compiler \
    && protoc -I protos --python_out protos protos/*.proto
ENV PYTHONPATH="/app"
# ENTRYPOINT ["kernprof.py", "-v", "-l"]
ENTRYPOINT ["python", "-m", "memory_profiler"]
# ENTRYPOINT ["python"]
