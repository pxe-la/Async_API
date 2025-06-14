#!/usr/bin/env python3
import os

import backoff
import elastic_transport
from elasticsearch import Elasticsearch


@backoff.on_exception(backoff.expo, elastic_transport.ConnectionError)
def ping_es(es: Elasticsearch):
    es.info()


if __name__ == "__main__":
    es_url = os.getenv("ES_URL", "http://localhost:9200")
    es_client = Elasticsearch(es_url)
    ping_es(es_client)
