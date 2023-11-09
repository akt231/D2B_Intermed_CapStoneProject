FROM python:3.9

WORKDIR /apps/app_d2b/intermediate/cap_project/finnhub_producer

COPY . .

RUN pip install -r requirements.txt

CMD ["python","producer_finnhub/main.py"]