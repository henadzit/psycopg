"""
Stub representation of the public objects exposed by the _psycopg module.

TODO: this should be generated by mypy's stubgen but it crashes with no
information. Will submit a bug.
"""

# Copyright (C) 2020 The Psycopg Team

from __future__ import annotations

from typing import Any, Sequence
from collections import deque

from psycopg import BaseConnection, abc, pq
from psycopg.rows import Row, RowMaker
from psycopg.adapt import AdaptersMap, PyFormat
from psycopg.pq.abc import PGcancelConn, PGconn, PGresult

class Transformer(abc.AdaptContext):
    types: tuple[int, ...] | None
    formats: list[pq.Format] | None
    def __init__(self, context: abc.AdaptContext | None = None): ...
    @classmethod
    def from_context(cls, context: abc.AdaptContext | None) -> "Transformer": ...
    @property
    def connection(self) -> BaseConnection[Any] | None: ...
    @property
    def encoding(self) -> str: ...
    @property
    def adapters(self) -> AdaptersMap: ...
    @property
    def pgresult(self) -> PGresult | None: ...
    def set_pgresult(
        self,
        result: "PGresult" | None,
        *,
        set_loaders: bool = True,
        format: pq.Format | None = None,
    ) -> None: ...
    def set_dumper_types(self, types: Sequence[int], format: pq.Format) -> None: ...
    def set_loader_types(self, types: Sequence[int], format: pq.Format) -> None: ...
    def dump_sequence(
        self, params: Sequence[Any], formats: Sequence[PyFormat]
    ) -> Sequence[abc.Buffer | None]: ...
    def as_literal(self, obj: Any) -> bytes: ...
    def get_dumper(self, obj: Any, format: PyFormat) -> abc.Dumper: ...
    def load_rows(self, row0: int, row1: int, make_row: RowMaker[Row]) -> list[Row]: ...
    def load_row(self, row: int, make_row: RowMaker[Row]) -> Row | None: ...
    def load_sequence(self, record: Sequence[abc.Buffer | None]) -> tuple[Any, ...]: ...
    def get_loader(self, oid: int, format: pq.Format) -> abc.Loader: ...

# Generators
def connect(conninfo: str, *, timeout: float = 0.0) -> abc.PQGenConn[PGconn]: ...
def cancel(cancel_conn: PGcancelConn, *, timeout: float = 0.0) -> abc.PQGenConn[None]: ...
def execute(pgconn: PGconn) -> abc.PQGen[list[PGresult]]: ...
def send(pgconn: PGconn) -> abc.PQGen[None]: ...
def fetch_many(pgconn: PGconn) -> abc.PQGen[list[PGresult]]: ...
def fetch(pgconn: PGconn) -> abc.PQGen[PGresult | None]: ...
def pipeline_communicate(
    pgconn: PGconn, commands: deque[abc.PipelineCommand]
) -> abc.PQGen[list[list[PGresult]]]: ...
def wait_c(gen: abc.PQGen[abc.RV], fileno: int, interval: float | None = None) -> abc.RV: ...

# Copy support
def format_row_text(
    row: Sequence[Any], tx: abc.Transformer, out: bytearray | None = None
) -> bytearray: ...
def format_row_binary(
    row: Sequence[Any], tx: abc.Transformer, out: bytearray | None = None
) -> bytearray: ...
def parse_row_text(data: abc.Buffer, tx: abc.Transformer) -> tuple[Any, ...]: ...
def parse_row_binary(data: abc.Buffer, tx: abc.Transformer) -> tuple[Any, ...]: ...

# Arrays optimization
def array_load_text(data: abc.Buffer, loader: abc.Loader, delimiter: bytes = b",") -> list[Any]: ...
def array_load_binary(data: abc.Buffer, tx: abc.Transformer) -> list[Any]: ...

# Queries
class PostgresQuery:
    def __init__(self, transformer: abc.Transformer): ...
    def convert(self, query: abc.Query, vars: abc.Params | None) -> None: ...
    def dump(self, vars: abc.Params | None) -> None: ...

class PostgresClientQuery(PostgresQuery):
    def __init__(self, transformer: abc.Transformer): ...
    def convert(self, query: abc.Query, vars: abc.Params | None) -> None: ...
    def dump(self, vars: abc.Params | None) -> None: ...

def _split_query(query: bytes, encoding: str, collapse_double_percent: bool): ...
