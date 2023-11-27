# Using the Ubuntu image (our OS)
FROM bitnami/spark:3.5
USER root

# Add Dependencies for PySpark
RUN apt-get update && apt-get install -y \
        curl vim wget software-properties-common \
        bash ssh net-tools ca-certificates python3 python3-pip
RUN pip3 install --upgrade pip

WORKDIR /opt/spark-python-config
COPY ./spark/apps .
COPY .env .
RUN chmod +x /opt/spark-python-config/*

RUN pip3 install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED 1

USER 1001
RUN ./start-spark.sh
WORKDIR /opt/bitnami/spark
#RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.704/aws-java-sdk-bundle-1.11.704.jar --output /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.704.jar
CMD tail -f /tmp/java_opts.txt