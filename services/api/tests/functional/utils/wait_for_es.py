import os
import time

from elasticsearch import Elasticsearch

if __name__ == "__main__":
    es_url = os.getenv("ES_URL", "http://localhost:9200")
    es_client = Elasticsearch(hosts=es_url)
    while True:
        if es_client.ping():
            break
        print("Ping elastic attempt: ", es_url)
        time.sleep(1)
