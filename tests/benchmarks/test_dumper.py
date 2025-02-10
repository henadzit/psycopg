from uuid import UUID

import psycopg
from psycopg._enums import PyFormat


PSYCOPG_TEST_DSN = "host=localhost dbname=psycopg_test user=postgres password=123456"


def test_uuid_text_dumper_100(benchmark, conn):
    val = UUID("12345678123456781234567812345679")
    tx = psycopg.adapt.Transformer(conn)

    @benchmark
    def bench():
        d = tx.get_dumper(val, PyFormat.TEXT)
        for _ in range(100):
            d.dump(val)


def test_uuid_binary_dumper_100(benchmark, conn):
    val = UUID("12345678123456781234567812345679")
    tx = psycopg.adapt.Transformer(conn)

    @benchmark
    def bench():
        d = tx.get_dumper(val, PyFormat.BINARY)
        for _ in range(100):
            d.dump(val)
