import json
import dataclasses as dc
from marshmallow import Schema, fields, post_load

INDEX_NAME = 'movies'


@dc.dataclass
class Movie:
    title: str
    genre: list[str] | str
    year: int
    duration: int
    director: str
    language: str

    def to_json(self):
        return json.dumps(dc.asdict(self))


class ListOrString(fields.Field):

    def _deserialize(self, value, attr, data, **kwargs):
        data = [value] if ',' not in value else [v.strip() for v in value.split(',')]
        return ListOrString.flatten(data)

    @staticmethod
    def flatten(value):
        # flattens empty or one-item lists
        if len(value) == 1:
            value = value[0]
        elif len(value) == 0:
            value = ''
        return value

    def _serialize(self, value, attr, obj, **kwargs):
        if isinstance(value, str):
            return [value]
        return value

    @staticmethod
    def to_str(value):
        if isinstance(value, str):
            return value
        return str(value).strip('[]').translate({ord('\''): None})


class MovieSchema(Schema):
    title = fields.String()
    genre = ListOrString()
    year = fields.Integer()
    duration = fields.Integer()
    director = fields.String()
    language = fields.String()

    @post_load
    def to_dataclass(self, data, **kw):
        return Movie(**data)
