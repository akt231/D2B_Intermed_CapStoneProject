FROM python:3.9

WORKDIR /d2bcapp_app

COPY . .

RUN pip install -r requirements.txt

CMD ["python","producer_finnhub/main.py"]