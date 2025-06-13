#!/usr/bin/env python3
import os

from backoff import backoff
from elasticsearch import Elasticsearch


@backoff()
def ping_es(es: Elasticsearch):
    es.info()


if __name__ == "__main__":
    es_url = os.getenv("ES_URL", "http://localhost:9200")
    es_client = Elasticsearch(es_url)
    ping_es(es_client)
