FROM python:3.7-buster
ADD ./grpc/servers/debug /app
ADD ./protos /app
ADD ./requirements.txt /app
WORKDIR /app
RUN pip install -r requirements.txt \
    && python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. *.proto
EXPOSE 50051
CMD ["python", "server.py"]
