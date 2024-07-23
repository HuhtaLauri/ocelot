FROM python:3.10.14-alpine3.20

COPY requirements.txt .

RUN pip install -r requirements.txt
