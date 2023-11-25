# Using the Ubuntu image (our OS)
FROM bitnami/spark:3.5
USER root

# Add Dependencies for PySpark
RUN apt-get update && apt-get install -y \
        curl vim wget software-properties-common \
        bash ssh net-tools ca-certificates python3 python3-pip

RUN pip3 install --upgrade pip
WORKDIR /opt/spark-python-config
COPY ./requirements.txt .
COPY ./start-spark.sh .
RUN chmod +x /opt/spark-python-config/*

RUN pip3 install --no-cache-dir -r requirements.txt
ENV PYTHONUNBUFFERED 1

USER 1001
RUN ./start-spark.sh
WORKDIR /opt/bitnami/spark
CMD tail -f /tmp/java_opts.txt