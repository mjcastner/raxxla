FROM python:3.9-buster
ADD ./grpc/clients/edsm /app
ADD ./commonlib /app/commonlib
ADD ./protos /app
ADD ./requirements.txt /app
ADD ./commonlib/google/storage/requirements.txt /app/gcs_requirements.txt
WORKDIR /app
RUN pip install -r requirements.txt \
    && pip install -r gcs_requirements.txt \
    && python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. *.proto
ENTRYPOINT ["python", "batch.py"]