FROM python:3.9-slim-buster

WORKDIR /srv
COPY . .

RUN apt update && apt install -y make
RUN pip install poetry
RUN poetry install
