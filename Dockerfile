FROM python:3.7

ADD ./app /raxxla
ADD ./requirements.txt /raxxla/requirements.txt
WORKDIR /raxxla
RUN pip install -r requirements.txt
ENV PYTHONPATH="/raxxla"
ENV GOOGLE_APPLICATION_CREDENTIALS="/gcp/credentials.json"
ENTRYPOINT ["python"]
