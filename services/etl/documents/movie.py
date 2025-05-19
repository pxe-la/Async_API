from datetime import datetime
from typing import Generator

import psycopg
from elasticsearch_dsl import (
    Document,
    Float,
    InnerDoc,
    Keyword,
    MetaField,
    Nested,
    Text,
)
from psycopg import ServerCursor
from psycopg.conninfo import make_conninfo
from psycopg.rows import class_row


class Director(InnerDoc):
    id = Keyword()
    name = Text(analyzer='ru_en')

    class Meta:
        dynamic = MetaField('strict')


class Actor(InnerDoc):
    id = Keyword()
    name = Text(analyzer='ru_en')

    class Meta:
        dynamic = MetaField('strict')


class Writer(InnerDoc):
    id = Keyword()
    name = Text(analyzer='ru_en')

    class Meta:
        dynamic = MetaField('strict')


class Movie(Document):
    id = Keyword()
    imdb_rating = Float()
    genres = Keyword()
    title = Text(analyzer='ru_en', fields={'raw': Keyword()})
    description = Text(analyzer='ru_en')
    directors_names = Text(analyzer='ru_en')
    actors_names = Text(analyzer='ru_en')
    writers_names = Text(analyzer='ru_en')
    directors = Nested(Director)
    actors = Nested(Actor)
    writers = Nested(Writer)
    last_change_date = Keyword(index=False)

    class Index:
        name = 'movies'
        settings = {
            'refresh_interval': '1s',
            'analysis': {
                'filter': {
                    'english_stop': {'type': 'stop', 'stopwords': '_english_'},
                    'english_stemmer': {'type': 'stemmer', 'language': 'english'},
                    'english_possessive_stemmer': {
                        'type': 'stemmer',
                        'language': 'possessive_english',
                    },
                    'russian_stop': {'type': 'stop', 'stopwords': '_russian_'},
                    'russian_stemmer': {'type': 'stemmer', 'language': 'russian'},
                },
                'analyzer': {
                    'ru_en': {
                        'tokenizer': 'standard',
                        'filter': [
                            'lowercase',
                            'english_stop',
                            'english_stemmer',
                            'english_possessive_stemmer',
                            'russian_stop',
                            'russian_stemmer',
                        ],
                    }
                },
            },
        }

    class Meta:
        dynamic = MetaField('strict')


def get_movie_index_data(
    database_settings: dict, last_sync_state: datetime, batch_size: int = 100
) -> Generator[list[Movie], None, None]:

    dsn = make_conninfo(**database_settings)

    with psycopg.connect(dsn, row_factory=class_row(Movie)) as conn, ServerCursor(conn, 'fetcher') as cursor:
        raw_sql = """
        SELECT
            fw.id,
            fw.title,
            fw.description,
            fw.rating as imdb_rating,
            COALESCE (array_agg(DISTINCT g.name),'{}') as genres,
            COALESCE (array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role='director'),'{}') as directors_names,
            COALESCE (array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role='actor'),'{}') as actors_names, 
            COALESCE (array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role='writer'),'{}') as writers_names,  
            COALESCE (
                json_agg(
                    DISTINCT jsonb_build_object(
                        'id', p.id,
                        'name', p.full_name
                    )
                ) FILTER (WHERE p.id is not null and pfw.role='director'),
                '[]'
            ) as directors,

                COALESCE (
                json_agg(
                    DISTINCT jsonb_build_object(
                        'id', p.id,
                        'name', p.full_name
                    )
                ) FILTER (WHERE p.id is not null and pfw.role='actor'),
                '[]'
            ) as actors,

                COALESCE (
                json_agg(
                    DISTINCT jsonb_build_object(
                        'id', p.id,
                        'name', p.full_name
                    )
                ) FILTER (WHERE p.id is not null and pfw.role='writer'),
                '[]'
            ) as writers
            ,max(v.last_change_date) last_change_date

            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            cross join lateral (values (fw.modified), (pfw.created), (p.modified), (gfw.created), (g.modified)) v(last_change_date)
            GROUP BY fw.id
            having max(v.last_change_date) > %s
            ORDER BY fw.modified
        """

        cursor.execute(raw_sql, (last_sync_state,))
        while results := cursor.fetchmany(size=batch_size):
            yield results
