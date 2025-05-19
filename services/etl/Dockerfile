FROM python:3.10

WORKDIR /opt/app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY requirements.txt requirements.txt

RUN  pip install --upgrade pip \
     && pip install -r requirements.txt --no-cache-dir \
     && apt-get update  \
     && apt-get install -y netcat-traditional

COPY . .

RUN addgroup --system etl && adduser --system --group etl && \
    chown -R etl:etl /opt/app

USER etl

EXPOSE 8000
