# Using the Ubuntu image (our OS)
FROM bitnami/spark:3.5
RUN apk update && apk upgrade --no-cache
RUN pip install --upgrade pip
WORKDIR /opt/spark-python-config
COPY ./spark/requirements.txt .
COPY ./start-spark.sh .
# Add Dependencies for PySpark
RUN apt-get update && apt-get install -y curl vim wget software-properties-common ssh net-tools ca-certificates python3 python3-pip python3-numpy python3-matplotlib python3-scipy python3-pandas python3-simpy
RUN pip install --no-cache-dir -r requirements.txt
ENV PYTHONUNBUFFERED 1
#CMD tail -f /dev/null
CMD ["/bin/bash", "./start-spark.sh"]