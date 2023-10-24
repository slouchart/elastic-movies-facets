import io
import json

from movies import search_movies
from movies.common import Movie, ListOrString


def render_response(response) -> str:
    buffer = io.StringIO()
    indent: str = ' '*2
    print('\nRésultats de la recherche', file=buffer)
    print('-------------------------', file=buffer)
    print(f"{indent}nombre total de films trouvés {response.total if response.total else 'inconnu'}", file=buffer)
    print(f"{indent}nombre total de films sur cette page {len(response.results)}\n", file=buffer)

    for doc in response.results:
        film: Movie = doc['_doc']
        print(f"{film.title} ({ListOrString.to_str(film.genre)}, {film.year})", file=buffer)

    print(f"\npage: {response.current_page}/{response.pages_count}", file=buffer)

    print('\nFacettes', file=buffer)
    print('--------', file=buffer)
    for name, facets in response.facets.items():
        print(f"{indent}{name}", file=buffer)
        for facet in facets:
            print(f"{'->' if facet.selected else indent}{indent}{facet.value} ({facet.count})", file=buffer)
    return buffer.getvalue()


if __name__ == '__main__':
    resp = search_movies(offset=0, keep_alive=5)
    print(json.dumps(resp.faceted_search.to_dict(), indent=4))
    print(render_response(resp))
