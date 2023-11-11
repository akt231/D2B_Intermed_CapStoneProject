FROM python:3.9

WORKDIR /apps/app_d2b/intermediate/cap_project

COPY . .

RUN pip install -r requirements.txt

CMD ["python","finnhub_producer/finnhub_producer.py"]