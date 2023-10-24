import typing
from elasticsearch import Elasticsearch


class PointInTime:

    def __init__(self, body: dict[str, typing.Any] = None, using=None):
        self._client = using
        self._body = body or {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._client.close_point_in_time(body=self._body)

    def to_dict(self):
        return self._body.copy()


def open_point_in_time(client: Elasticsearch, index, keep_alive='', **params) -> PointInTime:
    pit: dict[str, typing.Any] = client.open_point_in_time(index=index, keep_alive=keep_alive, **params)
    return PointInTime(body=pit, using=client)
