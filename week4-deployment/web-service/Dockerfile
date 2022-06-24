FROM python:3.9.12-slim

RUN pip install -U pip
RUN pip install pipenv

WORKDIR /app

COPY [ "Pipfile", "Pipfile.lock", "./"]

RUN pipenv install --system 