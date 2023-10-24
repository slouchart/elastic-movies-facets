from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from csv import DictReader

import io

from .common import MovieSchema, INDEX_NAME


def load_movies(source_path: str, cluster_url: str) -> int:
    schema = MovieSchema()
    data = []
    with io.open(source_path, mode='r') as file:
        reader = DictReader(file, delimiter=';')
        for item in reader:
            data.append(schema.load(item).to_json())

    client = Elasticsearch(cluster_url)
    # clear index
    search = Search(using=client, index=INDEX_NAME).query('match_all')
    client.delete_by_query(index=INDEX_NAME, body=search.to_dict())

    # load data
    for item in data:
        client.index(index=INDEX_NAME, document=item)

    return len(data)
