import time
import json

from elasticsearch import Elasticsearch, exceptions as es_exceptions


config_path = '/config/config.json'
# config_path = 'kube/secrets/config.json'


if __name__ == "__main__":

    with open(config_path) as json_data:
        config = json.load(json_data,)

    es = Elasticsearch(
        hosts=[{'host': config['ES_HOST'], 'port':9200, 'scheme':'https'}],
        basic_auth=(config['ES_USER'], config['ES_PASS']),
        request_timeout=60)

    if es.ping():
        print('connected to ES.')
    else:
        print('no connection to ES.')
        sys.exit(1)

    while True:
        print("collecting")
        time.sleep(6000)
