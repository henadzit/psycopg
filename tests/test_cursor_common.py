# WARNING: this file is auto-generated by 'async_to_sync.py'
# from the original file 'test_cursor_common_async.py'
# DO NOT CHANGE! Change the original file instead.
"""
Tests common to psycopg.Cursor and its subclasses.
"""

import weakref
import datetime as dt
from typing import Any
from packaging.version import parse as ver

import pytest

import psycopg
from psycopg import pq, sql, rows
from psycopg.adapt import PyFormat
from psycopg.types import TypeInfo

from .utils import raiseif
from .acompat import closing
from .fix_crdb import crdb_encoding
from ._test_cursor import my_row_factory, ph
from ._test_cursor import execmany, _execmany  # noqa: F401

execmany = execmany  # avoid F811 underneath

cursor_classes = [psycopg.Cursor, psycopg.ClientCursor]
# Allow to import (not necessarily to run) the module with psycopg 3.1.
# Needed to test psycopg_pool 3.2 tests with psycopg 3.1 imported, i.e. to run
# `pytest -m pool`. (which might happen when releasing pool packages).
if ver(psycopg.__version__) >= ver("3.2.0.dev0"):
    cursor_classes.append(psycopg.RawCursor)


@pytest.fixture(params=cursor_classes)
def conn(conn, request, anyio_backend):
    conn.cursor_factory = request.param
    return conn


def test_init(conn):
    cur = conn.cursor_factory(conn)
    cur.execute("select 1")
    assert cur.fetchone() == (1,)

    conn.row_factory = rows.dict_row
    cur = conn.cursor_factory(conn)
    cur.execute("select 1 as a")
    assert cur.fetchone() == {"a": 1}


def test_init_factory(conn):
    cur = conn.cursor_factory(conn, row_factory=rows.dict_row)
    cur.execute("select 1 as a")
    assert cur.fetchone() == {"a": 1}


def test_close(conn):
    cur = conn.cursor()
    assert not cur.closed
    cur.close()
    assert cur.closed

    with pytest.raises(psycopg.InterfaceError):
        cur.execute("select 'foo'")

    cur.close()
    assert cur.closed


def test_cursor_close_fetchone(conn):
    cur = conn.cursor()
    assert not cur.closed

    query = "select * from generate_series(1, 10)"
    cur.execute(query)
    for _ in range(5):
        cur.fetchone()

    cur.close()
    assert cur.closed

    with pytest.raises(psycopg.InterfaceError):
        cur.fetchone()


def test_cursor_close_fetchmany(conn):
    cur = conn.cursor()
    assert not cur.closed

    query = "select * from generate_series(1, 10)"
    cur.execute(query)
    assert len(cur.fetchmany(2)) == 2

    cur.close()
    assert cur.closed

    with pytest.raises(psycopg.InterfaceError):
        cur.fetchmany(2)


def test_cursor_close_fetchall(conn):
    cur = conn.cursor()
    assert not cur.closed

    query = "select * from generate_series(1, 10)"
    cur.execute(query)
    assert len(cur.fetchall()) == 10

    cur.close()
    assert cur.closed

    with pytest.raises(psycopg.InterfaceError):
        cur.fetchall()


def test_context(conn):
    with conn.cursor() as cur:
        assert not cur.closed

    assert cur.closed


@pytest.mark.slow
def test_weakref(conn, gc_collect):
    cur = conn.cursor()
    w = weakref.ref(cur)
    cur.close()
    del cur
    gc_collect()
    assert w() is None


def test_pgresult(conn):
    cur = conn.cursor()
    cur.execute("select 1")
    assert cur.pgresult
    cur.close()
    assert not cur.pgresult


def test_statusmessage(conn):
    cur = conn.cursor()
    assert cur.statusmessage is None

    cur.execute("select generate_series(1, 10)")
    assert cur.statusmessage == "SELECT 10"

    cur.execute("create table statusmessage ()")
    assert cur.statusmessage == "CREATE TABLE"

    with pytest.raises(psycopg.ProgrammingError):
        cur.execute("wat")
    assert cur.statusmessage is None


def test_execute_sql(conn):
    cur = conn.cursor()
    cur.execute(sql.SQL("select {value}").format(value="hello"))
    assert cur.fetchone() == ("hello",)


def test_query_parse_cache_size(conn):
    cur = conn.cursor()
    cls = type(cur)

    # Warning: testing internal structures. Test might need refactoring with the code.
    cache: Any
    if cls is psycopg.Cursor:
        cache = psycopg._queries._query2pg
    elif cls is psycopg.ClientCursor:
        cache = psycopg._queries._query2pg_client
    elif cls is psycopg.RawCursor:
        pytest.skip("RawCursor has no query parse cache")
    else:
        assert False, cls

    cache.cache_clear()
    ci = cache.cache_info()
    h0, m0 = (ci.hits, ci.misses)
    tests = [
        (f"select 1 -- {'x' * 3500}", (), h0, m0 + 1),
        (f"select 1 -- {'x' * 3500}", (), h0 + 1, m0 + 1),
        (f"select 1 -- {'x' * 4500}", (), h0 + 1, m0 + 1),
        (f"select 1 -- {'x' * 4500}", (), h0 + 1, m0 + 1),
        (f"select 1 -- {'%s' * 40}", ("x",) * 40, h0 + 1, m0 + 2),
        (f"select 1 -- {'%s' * 40}", ("x",) * 40, h0 + 2, m0 + 2),
        (f"select 1 -- {'%s' * 60}", ("x",) * 60, h0 + 2, m0 + 2),
        (f"select 1 -- {'%s' * 60}", ("x",) * 60, h0 + 2, m0 + 2),
    ]
    for i, (query, params, hits, misses) in enumerate(tests):
        pq = cur._query_cls(psycopg.adapt.Transformer())
        pq.convert(query, params)
        ci = cache.cache_info()
        assert ci.hits == hits, f"at {i}"
        assert ci.misses == misses, f"at {i}"


def test_execute_many_results(conn):
    cur = conn.cursor()
    assert cur.nextset() is None

    rv = cur.execute("select 'foo'; select generate_series(1,3)")
    assert rv is cur
    assert cur.fetchall() == [("foo",)]
    assert cur.rowcount == 1
    assert cur.nextset()
    assert cur.fetchall() == [(1,), (2,), (3,)]
    assert cur.rowcount == 3
    assert cur.nextset() is None

    cur.close()
    assert cur.nextset() is None


def test_execute_sequence(conn):
    cur = conn.cursor()
    rv = cur.execute(ph(cur, "select %s::int, %s::text, %s::text"), [1, "foo", None])
    assert rv is cur
    assert len(cur._results) == 1
    assert cur.pgresult.get_value(0, 0) == b"1"
    assert cur.pgresult.get_value(0, 1) == b"foo"
    assert cur.pgresult.get_value(0, 2) is None
    assert cur.nextset() is None


@pytest.mark.parametrize("query", ["", " ", ";"])
def test_execute_empty_query(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    assert cur.pgresult.status == pq.ExecStatus.EMPTY_QUERY
    with pytest.raises(psycopg.ProgrammingError):
        cur.fetchone()


def test_execute_type_change(conn):
    # issue #112
    conn.execute("create table bug_112 (num integer)")
    cur = conn.cursor()
    sql = ph(cur, "insert into bug_112 (num) values (%s)")
    cur.execute(sql, (1,))
    cur.execute(sql, (100000,))
    cur.execute("select num from bug_112 order by num")
    assert cur.fetchall() == [(1,), (100000,)]


def test_executemany_type_change(conn):
    conn.execute("create table bug_112 (num integer)")
    cur = conn.cursor()
    sql = ph(cur, "insert into bug_112 (num) values (%s)")
    cur.executemany(sql, [(1,), (100000,)])
    cur.execute("select num from bug_112 order by num")
    assert cur.fetchall() == [(1,), (100000,)]


@pytest.mark.parametrize(
    "query", ["copy testcopy from stdin", "copy testcopy to stdout"]
)
def test_execute_copy(conn, query):
    cur = conn.cursor()
    cur.execute("create table testcopy (id int)")
    with pytest.raises(psycopg.ProgrammingError):
        cur.execute(query)


def test_fetchone(conn):
    cur = conn.cursor()
    cur.execute(ph(cur, "select %s::int, %s::text, %s::text"), [1, "foo", None])
    assert cur.pgresult.fformat(0) == 0

    row = cur.fetchone()
    assert row == (1, "foo", None)
    row = cur.fetchone()
    assert row is None


def test_binary_cursor_execute(conn):
    with raiseif(
        conn.cursor_factory is psycopg.ClientCursor, psycopg.NotSupportedError
    ) as ex:
        cur = conn.cursor(binary=True)
        cur.execute(ph(cur, "select %s, %s"), [1, None])
    if ex:
        return

    assert cur.fetchone() == (1, None)
    assert cur.pgresult.fformat(0) == 1
    assert cur.pgresult.get_value(0, 0) == b"\x00\x01"


def test_execute_binary(conn):
    cur = conn.cursor()
    with raiseif(
        conn.cursor_factory is psycopg.ClientCursor, psycopg.NotSupportedError
    ) as ex:
        cur.execute(ph(cur, "select %s, %s"), [1, None], binary=True)
    if ex:
        return

    assert cur.fetchone() == (1, None)
    assert cur.pgresult.fformat(0) == 1
    assert cur.pgresult.get_value(0, 0) == b"\x00\x01"


def test_binary_cursor_text_override(conn):
    cur = conn.cursor(binary=True)
    cur.execute(ph(cur, "select %s, %s"), [1, None], binary=False)
    assert cur.fetchone() == (1, None)
    assert cur.pgresult.fformat(0) == 0
    assert cur.pgresult.get_value(0, 0) == b"1"


@pytest.mark.parametrize("encoding", ["utf8", crdb_encoding("latin9")])
def test_query_encode(conn, encoding):
    conn.execute(f"set client_encoding to {encoding}")
    cur = conn.cursor()
    cur.execute("select '€'")
    (res,) = cur.fetchone()
    assert res == "€"


@pytest.mark.parametrize("encoding", [crdb_encoding("latin1")])
def test_query_badenc(conn, encoding):
    conn.execute(f"set client_encoding to {encoding}")
    cur = conn.cursor()
    with pytest.raises(UnicodeEncodeError):
        cur.execute("select '€'")


def test_executemany(conn, execmany):
    cur = conn.cursor()
    cur.executemany(
        ph(cur, "insert into execmany(num, data) values (%s, %s)"),
        [(10, "hello"), (20, "world")],
    )
    cur.execute("select num, data from execmany order by 1")
    rv = cur.fetchall()
    assert rv == [(10, "hello"), (20, "world")]


def test_executemany_name(conn, execmany):
    cur = conn.cursor()
    cur.executemany(
        ph(cur, "insert into execmany(num, data) values (%(num)s, %(data)s)"),
        [{"num": 11, "data": "hello", "x": 1}, {"num": 21, "data": "world"}],
    )
    cur.execute("select num, data from execmany order by 1")
    rv = cur.fetchall()
    assert rv == [(11, "hello"), (21, "world")]


def test_executemany_no_data(conn, execmany):
    cur = conn.cursor()
    cur.executemany(ph(cur, "insert into execmany(num, data) values (%s, %s)"), [])
    assert cur.rowcount == 0


def test_executemany_rowcount(conn, execmany):
    cur = conn.cursor()
    cur.executemany(
        ph(cur, "insert into execmany(num, data) values (%s, %s)"),
        [(10, "hello"), (20, "world")],
    )
    assert cur.rowcount == 2


def test_executemany_returning(conn, execmany):
    cur = conn.cursor()
    cur.executemany(
        ph(cur, "insert into execmany(num, data) values (%s, %s) returning num"),
        [(10, "hello"), (20, "world")],
        returning=True,
    )
    assert cur.rowcount == 1
    assert cur.fetchone() == (10,)
    assert cur.nextset()
    assert cur.rowcount == 1
    assert cur.fetchone() == (20,)
    assert cur.nextset() is None


def test_executemany_returning_discard(conn, execmany):
    cur = conn.cursor()
    cur.executemany(
        ph(cur, "insert into execmany(num, data) values (%s, %s) returning num"),
        [(10, "hello"), (20, "world")],
    )
    assert cur.rowcount == 2
    with pytest.raises(psycopg.ProgrammingError):
        cur.fetchone()
    assert cur.nextset() is None


def test_executemany_no_result(conn, execmany):
    cur = conn.cursor()
    cur.executemany(
        ph(cur, "insert into execmany(num, data) values (%s, %s)"),
        [(10, "hello"), (20, "world")],
        returning=True,
    )
    assert cur.rowcount == 1
    assert cur.statusmessage.startswith("INSERT")
    with pytest.raises(psycopg.ProgrammingError):
        cur.fetchone()
    pgresult = cur.pgresult
    assert cur.nextset()
    assert cur.rowcount == 1
    assert cur.statusmessage.startswith("INSERT")
    assert pgresult is not cur.pgresult
    assert cur.nextset() is None


def test_executemany_rowcount_no_hit(conn, execmany):
    cur = conn.cursor()
    cur.executemany(ph(cur, "delete from execmany where id = %s"), [(-1,), (-2,)])
    assert cur.rowcount == 0
    cur.executemany(ph(cur, "delete from execmany where id = %s"), [])
    assert cur.rowcount == 0
    cur.executemany(
        ph(cur, "delete from execmany where id = %s returning num"), [(-1,), (-2,)]
    )
    assert cur.rowcount == 0


@pytest.mark.parametrize(
    "query",
    [
        "insert into nosuchtable values (%s, %s)",
        "copy (select %s, %s) to stdout",
        "wat (%s, %s)",
    ],
)
def test_executemany_badquery(conn, query):
    cur = conn.cursor()
    with pytest.raises(psycopg.DatabaseError):
        cur.executemany(ph(cur, query), [(10, "hello"), (20, "world")])


@pytest.mark.parametrize("fmt_in", PyFormat)
def test_executemany_null_first(conn, fmt_in):
    cur = conn.cursor()
    cur.execute("create table testmany (a bigint, b bigint)")
    cur.executemany(
        ph(cur, f"insert into testmany values (%{fmt_in.value}, %{fmt_in.value})"),
        [[1, None], [3, 4]],
    )
    with pytest.raises((psycopg.DataError, psycopg.ProgrammingError)):
        cur.executemany(
            ph(cur, f"insert into testmany values (%{fmt_in.value}, %{fmt_in.value})"),
            [[1, ""], [3, 4]],
        )


def test_rowcount(conn):
    cur = conn.cursor()

    cur.execute("select 1 from generate_series(1, 0)")
    assert cur.rowcount == 0

    cur.execute("select 1 from generate_series(1, 42)")
    assert cur.rowcount == 42

    cur.execute("show timezone")
    assert cur.rowcount == 1

    cur.execute("create table test_rowcount_notuples (id int primary key)")
    assert cur.rowcount == -1

    cur.execute("insert into test_rowcount_notuples select generate_series(1, 42)")
    assert cur.rowcount == 42


def test_rownumber(conn):
    cur = conn.cursor()
    assert cur.rownumber is None

    cur.execute("select 1 from generate_series(1, 42)")
    assert cur.rownumber == 0

    cur.fetchone()
    assert cur.rownumber == 1
    cur.fetchone()
    assert cur.rownumber == 2
    cur.fetchmany(10)
    assert cur.rownumber == 12
    rns: list[int] = []
    for i in cur:
        assert cur.rownumber
        rns.append(cur.rownumber)
        if len(rns) >= 3:
            break
    assert rns == [13, 14, 15]
    assert len(cur.fetchall()) == 42 - rns[-1]
    assert cur.rownumber == 42


@pytest.mark.parametrize("query", ["", "set timezone to utc"])
def test_rownumber_none(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    assert cur.rownumber is None


def test_rownumber_mixed(conn):
    cur = conn.cursor()
    cur.execute(
        """
select x from generate_series(1, 3) x;
set timezone to utc;
select x from generate_series(4, 6) x;
"""
    )
    assert cur.rownumber == 0
    assert cur.fetchone() == (1,)
    assert cur.rownumber == 1
    assert cur.fetchone() == (2,)
    assert cur.rownumber == 2
    cur.nextset()
    assert cur.rownumber is None
    cur.nextset()
    assert cur.rownumber == 0
    assert cur.fetchone() == (4,)
    assert cur.rownumber == 1


def test_iter(conn):
    cur = conn.cursor()
    cur.execute("select generate_series(1, 3)")
    assert list(cur) == [(1,), (2,), (3,)]


def test_iter_stop(conn):
    cur = conn.cursor()
    cur.execute("select generate_series(1, 3)")
    for rec in cur:
        assert rec == (1,)
        break

    for rec in cur:
        assert rec == (2,)
        break

    assert cur.fetchone() == (3,)
    assert list(cur) == []


def test_row_factory(conn):
    cur = conn.cursor(row_factory=my_row_factory)

    cur.execute("reset search_path")
    with pytest.raises(psycopg.ProgrammingError):
        cur.fetchone()

    cur.execute("select 'foo' as bar")
    (r,) = cur.fetchone()
    assert r == "FOObar"

    cur.execute("select 'x' as x; select 'y' as y, 'z' as z")
    assert cur.fetchall() == [["Xx"]]
    assert cur.nextset()
    assert cur.fetchall() == [["Yy", "Zz"]]

    cur.scroll(-1)
    cur.row_factory = rows.dict_row
    assert cur.fetchone() == {"y": "y", "z": "z"}


def test_row_factory_none(conn):
    cur = conn.cursor(row_factory=None)
    assert cur.row_factory is rows.tuple_row
    cur.execute("select 1 as a, 2 as b")
    r = cur.fetchone()
    assert type(r) is tuple
    assert r == (1, 2)


def test_bad_row_factory(conn):

    def broken_factory(cur):
        1 / 0

    cur = conn.cursor(row_factory=broken_factory)
    with pytest.raises(ZeroDivisionError):
        cur.execute("select 1")

    def broken_maker(cur):

        def make_row(seq):
            1 / 0

        return make_row

    cur = conn.cursor(row_factory=broken_maker)
    cur.execute("select 1")
    with pytest.raises(ZeroDivisionError):
        cur.fetchone()


def test_scroll(conn):
    cur = conn.cursor()
    with pytest.raises(psycopg.ProgrammingError):
        cur.scroll(0)

    cur.execute("select generate_series(0,9)")
    cur.scroll(2)
    assert cur.fetchone() == (2,)
    cur.scroll(2)
    assert cur.fetchone() == (5,)
    cur.scroll(2, mode="relative")
    assert cur.fetchone() == (8,)
    cur.scroll(-1)
    assert cur.fetchone() == (8,)
    cur.scroll(-2)
    assert cur.fetchone() == (7,)
    cur.scroll(2, mode="absolute")
    assert cur.fetchone() == (2,)

    # on the boundary
    cur.scroll(0, mode="absolute")
    assert cur.fetchone() == (0,)
    with pytest.raises(IndexError):
        cur.scroll(-1, mode="absolute")

    cur.scroll(0, mode="absolute")
    with pytest.raises(IndexError):
        cur.scroll(-1)

    cur.scroll(9, mode="absolute")
    assert cur.fetchone() == (9,)
    with pytest.raises(IndexError):
        cur.scroll(10, mode="absolute")

    cur.scroll(9, mode="absolute")
    with pytest.raises(IndexError):
        cur.scroll(1)

    with pytest.raises(ValueError):
        cur.scroll(1, "wat")


@pytest.mark.parametrize(
    "query, params, want",
    [
        ("select %(x)s", {"x": 1}, (1,)),
        ("select %(x)s, %(y)s", {"x": 1, "y": 2}, (1, 2)),
        ("select %(x)s, %(x)s", {"x": 1}, (1, 1)),
    ],
)
def test_execute_params_named(conn, query, params, want):
    cur = conn.cursor()
    cur.execute(ph(cur, query), params)
    rec = cur.fetchone()
    assert rec == want


def test_stream(conn):
    cur = conn.cursor()
    recs = []
    for rec in cur.stream(
        ph(cur, "select i, '2021-01-01'::date + i from generate_series(1, %s) as i"),
        [2],
    ):
        recs.append(rec)

    assert recs == [(1, dt.date(2021, 1, 2)), (2, dt.date(2021, 1, 3))]


def test_stream_sql(conn):
    cur = conn.cursor()
    recs = list(
        cur.stream(
            sql.SQL(
                "select i, '2021-01-01'::date + i from generate_series(1, {}) as i"
            ).format(2)
        )
    )

    assert recs == [(1, dt.date(2021, 1, 2)), (2, dt.date(2021, 1, 3))]


def test_stream_row_factory(conn):
    cur = conn.cursor(row_factory=rows.dict_row)
    it = cur.stream("select generate_series(1,2) as a")
    assert next(it)["a"] == 1
    cur.row_factory = rows.namedtuple_row
    assert next(it).a == 2


def test_stream_no_row(conn):
    cur = conn.cursor()
    recs = list(cur.stream("select generate_series(2,1) as a"))
    assert recs == []


def test_stream_chunked_invalid_size(conn):
    cur = conn.cursor()
    with pytest.raises(ValueError, match="size must be >= 1"):
        next(cur.stream("select 1", size=0))


@pytest.mark.libpq("< 17")
def test_stream_chunked_not_supported(conn):
    cur = conn.cursor()
    with pytest.raises(psycopg.NotSupportedError):
        next(cur.stream("select generate_series(1, 4)", size=2))


@pytest.mark.libpq(">= 17")
def test_stream_chunked(conn):
    cur = conn.cursor()
    recs = list(cur.stream("select generate_series(1, 5) as a", size=2))
    assert recs == [(1,), (2,), (3,), (4,), (5,)]


@pytest.mark.libpq(">= 17")
def test_stream_chunked_row_factory(conn):
    cur = conn.cursor(row_factory=rows.scalar_row)
    it = cur.stream("select generate_series(1, 5) as a", size=2)
    for i in range(1, 6):
        assert next(it) == i
        assert [c.name for c in cur.description] == ["a"]


@pytest.mark.crdb_skip("no col query")
def test_stream_no_col(conn):
    cur = conn.cursor()
    recs = list(cur.stream("select"))
    assert recs == [()]


@pytest.mark.parametrize(
    "query", ["create table test_stream_badq ()", "copy (select 1) to stdout", "wat?"]
)
def test_stream_badquery(conn, query):
    cur = conn.cursor()
    with pytest.raises(psycopg.ProgrammingError):
        for rec in cur.stream(query):
            pass


def test_stream_error_tx(conn):
    cur = conn.cursor()
    with pytest.raises(psycopg.ProgrammingError):
        for rec in cur.stream("wat"):
            pass
    assert conn.info.transaction_status == pq.TransactionStatus.INERROR


def test_stream_error_notx(conn):
    conn.set_autocommit(True)
    cur = conn.cursor()
    with pytest.raises(psycopg.ProgrammingError):
        for rec in cur.stream("wat"):
            pass
    assert conn.info.transaction_status == pq.TransactionStatus.IDLE


def test_stream_error_python_to_consume(conn):
    cur = conn.cursor()
    with pytest.raises(ZeroDivisionError):
        with closing(cur.stream("select generate_series(1, 10000)")) as gen:
            for rec in gen:
                1 / 0
    assert conn.info.transaction_status in (
        pq.TransactionStatus.INTRANS,
        pq.TransactionStatus.INERROR,
    )


def test_stream_error_python_consumed(conn):
    cur = conn.cursor()
    with pytest.raises(ZeroDivisionError):
        gen = cur.stream("select 1")
        for rec in gen:
            1 / 0

    gen.close()
    assert conn.info.transaction_status == pq.TransactionStatus.INTRANS


@pytest.mark.parametrize("autocommit", [False, True])
def test_stream_close(conn, autocommit):
    conn.set_autocommit(autocommit)
    cur = conn.cursor()
    with pytest.raises(psycopg.OperationalError):
        for rec in cur.stream("select generate_series(1, 3)"):
            if rec[0] == 1:
                conn.close()
            else:
                assert False

    assert conn.closed


def test_stream_binary_cursor(conn):
    with raiseif(
        conn.cursor_factory is psycopg.ClientCursor, psycopg.NotSupportedError
    ):
        cur = conn.cursor(binary=True)
        recs = []
        for rec in cur.stream("select x::int4 from generate_series(1, 2) x"):
            recs.append(rec)
            assert cur.pgresult.fformat(0) == 1
            assert cur.pgresult.get_value(0, 0) == bytes([0, 0, 0, rec[0]])

        assert recs == [(1,), (2,)]


def test_stream_execute_binary(conn):
    cur = conn.cursor()
    recs = []
    with raiseif(
        conn.cursor_factory is psycopg.ClientCursor, psycopg.NotSupportedError
    ):
        for rec in cur.stream(
            "select x::int4 from generate_series(1, 2) x", binary=True
        ):
            recs.append(rec)
            assert cur.pgresult.fformat(0) == 1
            assert cur.pgresult.get_value(0, 0) == bytes([0, 0, 0, rec[0]])

        assert recs == [(1,), (2,)]


def test_stream_binary_cursor_text_override(conn):
    cur = conn.cursor(binary=True)
    recs = []
    for rec in cur.stream("select generate_series(1, 2)", binary=False):
        recs.append(rec)
        assert cur.pgresult.fformat(0) == 0
        assert cur.pgresult.get_value(0, 0) == str(rec[0]).encode()

    assert recs == [(1,), (2,)]


def test_str(conn):
    cur = conn.cursor()
    assert "[IDLE]" in str(cur)
    assert "[closed]" not in str(cur)
    assert "[no result]" in str(cur)
    cur.execute("select 1")
    assert "[INTRANS]" in str(cur)
    assert "[TUPLES_OK]" in str(cur)
    assert "[closed]" not in str(cur)
    assert "[no result]" not in str(cur)
    cur.close()
    assert "[closed]" in str(cur)
    assert "[INTRANS]" in str(cur)


@pytest.mark.pipeline
def test_message_0x33(conn):
    # https://github.com/psycopg/psycopg/issues/314
    notices = []
    conn.add_notice_handler(lambda diag: notices.append(diag.message_primary))

    conn.set_autocommit(True)
    with conn.pipeline():
        cur = conn.execute("select 'test'")
        assert cur.fetchone() == ("test",)

    assert not notices


def test_typeinfo(conn):
    info = TypeInfo.fetch(conn, "jsonb")
    assert info is not None
