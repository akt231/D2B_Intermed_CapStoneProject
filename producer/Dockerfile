# Using the Ubuntu image (our OS)
FROM python:3.10.13-alpine
#FROM python:3.10-slim-buster
RUN apk update && apk upgrade --no-cache
RUN pip install --upgrade pip
WORKDIR /usr/src/app/producer
COPY ./producer .
COPY .env .
RUN pip install --no-cache-dir -r requirements.txt
#CMD ["python","producer.py"]
ENV PYTHONUNBUFFERED 1
CMD tail -f /dev/null