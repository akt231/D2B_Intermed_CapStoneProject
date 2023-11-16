# Using the Ubuntu image (our OS)
FROM python:3.10.13-alpine
RUN apk update && apk upgrade --no-cache
WORKDIR /usr/src/app
COPY . .
#COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
#CMD ["python","producer.py"]
ENV PYTHONUNBUFFERED 1
CMD [ "sh", "-c", "service ssh start; bash"]