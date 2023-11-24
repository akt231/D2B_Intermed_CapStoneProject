# Using the Ubuntu image (our OS)
FROM bitnami/spark:3.5
RUN apk update && apk upgrade --no-cache
RUN pip install --upgrade pip
#WORKDIR /opt/spark-apps
RUN pip install --no-cache-dir -r requirements.txt
ENV PYTHONUNBUFFERED 1
CMD tail -f /dev/null