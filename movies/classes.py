import dataclasses as dc
import typing
from elasticsearch_dsl import Search, FacetedSearch, TermsFacet, RangeFacet, FacetedResponse, Facet

from .common import Movie, INDEX_NAME


@dc.dataclass(frozen=True)
class FacetRepr:
    value: str
    count: int
    selected: bool


class MovieFacetedResponse(FacetedResponse):

    @property
    def faceted_search(self) -> 'MovieFacetedSearch':
        return self._faceted_search


class MovieFacetedSearch(FacetedSearch):
    index: str = INDEX_NAME
    query_params: dict[str, typing.Any] = {
        "type": "bool_prefix"
    }
    search_params: dict[str, typing.Any] = {
        'track_total_hits': True
    }
    fields: list[str] = ['title', 'genre.analyzed', 'director']
    facets: dict[str, Facet] = {
        'genres': TermsFacet(field='genre', min_doc_count=0),
        'languages': TermsFacet(field='language', min_doc_count=0),
        'years': RangeFacet(
            field='year',
            ranges=[
                ("1980-1990", (1980, 1990)),
                ("1990-2000", (1990, 2000)),
                ("2000-2010", (2000, 2010)),
                ("2010-2020", (2010, 2020)),
                ("2020-2030", (2020, 2030))
            ]
        ),
        'duration': RangeFacet(
            field='duration',
            ranges=[
                ("80-90", (80, 90)),
                ("90-120", (90, 120)),
                ("120+", (120, 500))
            ]
        )
    }

    @property
    def point_in_time(self):
        return self._pit

    def __init__(self, *args, point_in_time=None, **kwargs):
        self._pit = point_in_time
        super().__init__(*args, **kwargs)

    def build_search(self):
        s = super().build_search()
        if self._pit:
            s = s.extra(pit=self._pit)
        return s

    def query(self, search: Search, query: str) -> Search:
        params = {}
        if query:
            if self.fields:
                params.update(fields=self.fields)
            if self.query_params:
                params.update(**self.query_params)
            return search.query('multi_match', query=query, **params)
        return search

    def search(self) -> Search:
        if self._pit:
            s = Search(using=self.using)
        else:
            s = Search(index=self.index, using=self.using)

        if self.search_params:
            s = s.extra(**self.search_params)

        return s.response_class(MovieFacetedResponse)

    def to_dict(self) -> dict[str, typing.Any]:
        return self._s.to_dict()

    def get_search(self) -> Search:
        return self._s

    def execute(self):
        r = super().execute()
        return MovieFacetedResults(response=r)


class MovieFacetedResults:
    def __init__(self, response: MovieFacetedResponse = None):
        self._response = response

    @property
    def facets(self) -> dict[str, typing.Any]:
        return {
            key: [FacetRepr(value=f[0], count=f[1], selected=f[2]) for f in _facet]
            for key, _facet in
            self._response.facets.to_dict().items()
        }

    @property
    def aggregations(self) -> dict[str, typing.Any]:
        return self._response.aggregations.to_dict().copy()

    @property
    def results(self) -> list[typing.Dict]:
        return [
            dict(_id=hit['_id'], _doc=Movie(**hit['_source'].to_dict().copy()))
            for hit in self._response.hits.hits
        ]

    @property
    def total(self) -> int:
        return self._response.hits.total['value']

    @property
    def pages_count(self) -> int:
        _from, _size = self._get_search_page_attrs()
        return self.total // _size + 1 if self.total % _size else 0

    @property
    def current_page(self) -> int:
        _from, _size = self._get_search_page_attrs()
        return 1 + _from // _size

    def _get_search_page_attrs(self) -> tuple[int, int]:
        pagination = {'from': 0, 'size': 10}
        try:
            pagination = getattr(self._response.faceted_search.get_search(), '_extra')
        except AttributeError:
            pass
        return pagination.get('from', 0), pagination.get('size', 10)

    @property
    def faceted_search(self) -> MovieFacetedSearch:
        return self._response.faceted_search
