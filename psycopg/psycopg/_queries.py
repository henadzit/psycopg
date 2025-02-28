"""
Utility module to manipulate queries
"""

# Copyright (C) 2020 The Psycopg Team

from __future__ import annotations
from typing import Callable

from . import abc
from ._cmodule import _psycopg

PostgresQuery: type[abc.PostgresQuery]
PostgresClientQuery: type[abc.PostgresQuery]
# TODO: type properly
_split_query: Callable

if _psycopg:
    PostgresQuery = _psycopg.PostgresQuery
    PostgresClientQuery = _psycopg.PostgresClientQuery
    _split_query = _psycopg._split_query
else:
    from . import _py_queries

    PostgresQuery = _py_queries.PostgresQuery
    PostgresClientQuery = _py_queries.PostgresClientQuery
    _split_query = _py_queries._split_query