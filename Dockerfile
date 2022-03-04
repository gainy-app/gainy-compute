FROM python:3.9-slim

WORKDIR /srv
COPY . .

RUN apt update && apt install make
RUN pip install poetry
RUN make install
