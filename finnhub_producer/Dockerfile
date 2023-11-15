# Using the Ubuntu image (our OS)
FROM python:3.10.13-bookworm
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt


#WORKDIR /apps/d2b_cap/finnhub_producer
COPY . .

#
## Copy the application files to the container
#COPY . .
#CMD ["python","finnhub_producer/finnhub_producer.py"]


#https://stackoverflow.com/questions/66146088/docker-gets-error-failed-to-compute-cache-key-not-found-runs-fine-in-visual