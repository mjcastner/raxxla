FROM python:3.9-buster
ADD ./grpc/server /app
ADD ./commonlib /app/commonlib
ADD ./protos /app
ADD ./requirements.txt /app
ADD ./commonlib/google/datastore/requirements.txt /app/datastore_requirements.txt
WORKDIR /app
RUN pip install -r requirements.txt \
    && pip install -r datastore_requirements.txt \
    && python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. *.proto
ENTRYPOINT ["python", "main.py"]