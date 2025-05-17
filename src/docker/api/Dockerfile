FROM --platform=linux/amd64 python:3.10.2-buster

WORKDIR /code

RUN groupadd -r web && useradd -r -g web web

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt update && apt install -y netcat
RUN pip install --upgrade pip

ENV TZ=Europe/Moscow
RUN apt install tzdata
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY ./requirements.txt ./

RUN pip install -r ./requirements.txt

COPY ./docker/api/entrypoint.sh /web/entrypoint.sh
COPY movies_admin/ .

RUN python manage.py collectstatic --no-input

USER web

ENTRYPOINT ["/web/entrypoint.sh"]