FROM python:3.7-buster
ADD ./grpc/clients/debug /app
ADD ./protos /app
ADD ./requirements.txt /app
WORKDIR /app
RUN pip install -r requirements.txt \
    && python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. *.proto
ENTRYPOINT ["python"]
