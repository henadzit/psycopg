import psycopg
from psycopg import pq
from psycopg import _oids


def test_uuid_loader_100(benchmark, conn):
    val = b"12345678-1234-5678-1234-567812345678"
    tx = psycopg.adapt.Transformer(conn)

    @benchmark
    def bench():
        loader = tx.get_loader(_oids.UUID_OID, pq.Format.TEXT)
        for _ in range(100):
            loader.load(val)


def test_uuid_binary_loader_100(benchmark, conn):
    val = b"\x12\x34\x56\x78\x12\x34\x56\x78\x12\x34\x56\x78\x12\x34\x56\x78"
    tx = psycopg.adapt.Transformer(conn)

    @benchmark
    def bench():
        loader = tx.get_loader(_oids.UUID_OID, pq.Format.BINARY)
        for _ in range(100):
            loader.load(val)
