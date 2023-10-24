from elasticsearch import Elasticsearch
from contextlib import nullcontext
import typing
import env
from .classes import MovieFacetedSearch, MovieFacetedResults
from .utils import open_point_in_time


def search_movies(
        terms: str = '',
        filters: dict[str, typing.Any] = None,
        sort: tuple[str] = (),
        keep_alive: int = 0,
        offset: int = 0,
        page_size: int = 10
):

    client = Elasticsearch(env.CLUSTER_URL)
    MovieFacetedSearch.using = client

    if keep_alive:
        pit = open_point_in_time(client, index=MovieFacetedSearch.index, keep_alive=str(keep_alive) + 'm')
    else:
        pit = nullcontext()

    with pit:
        search: MovieFacetedSearch = MovieFacetedSearch(
            terms,
            filters=dict(filters or {}),
            sort=tuple(sort),
            point_in_time=pit.to_dict() if keep_alive else None
        )[offset:offset+page_size]

        resp: MovieFacetedResults = search.execute()
        return resp
