# WARNING: this file is auto-generated by 'async_to_sync.py'
# from the original file 'test_connection_async.py'
# DO NOT CHANGE! Change the original file instead.
from __future__ import annotations

import sys
import time
import pytest
import logging
import weakref
from typing import Any

import psycopg
from psycopg import pq, errors as e
from psycopg.rows import tuple_row
from psycopg.conninfo import conninfo_to_dict, timeout_from_conninfo

from .acompat import is_async, skip_sync, skip_async
from ._test_cursor import my_row_factory
from ._test_connection import tx_params, tx_params_isolation, tx_values_map
from ._test_connection import conninfo_params_timeout
from ._test_connection import testctx  # noqa: F401  # fixture
from .test_adapt import make_bin_dumper, make_dumper


def test_connect(conn_cls, dsn):
    conn = conn_cls.connect(dsn)
    assert not conn.closed
    assert conn.pgconn.status == pq.ConnStatus.OK
    conn.close()


def test_connect_bad(conn_cls):
    with pytest.raises(psycopg.OperationalError):
        conn_cls.connect("dbname=nosuchdb")


def test_connect_str_subclass(conn_cls, dsn):

    class MyString(str):
        pass

    conn = conn_cls.connect(MyString(dsn))
    assert not conn.closed
    assert conn.pgconn.status == pq.ConnStatus.OK
    conn.close()


@pytest.mark.slow
@pytest.mark.timing
def test_connect_timeout(conn_cls, proxy):
    with proxy.deaf_listen():
        t0 = time.time()
        with pytest.raises(psycopg.OperationalError, match="timeout expired"):
            conn_cls.connect(proxy.client_dsn, connect_timeout=2)
        elapsed = time.time() - t0
    assert elapsed == pytest.approx(2.0, 0.1)


@pytest.mark.slow
@pytest.mark.timing
def test_multi_hosts(conn_cls, proxy, dsn, monkeypatch):
    args = conninfo_to_dict(dsn)
    args["host"] = f"{proxy.client_host},{proxy.server_host}"
    args["port"] = f"{proxy.client_port},{proxy.server_port}"
    args.pop("hostaddr", None)
    monkeypatch.setattr(psycopg.conninfo, "_DEFAULT_CONNECT_TIMEOUT", 2)
    with proxy.deaf_listen():
        t0 = time.time()
        with conn_cls.connect(**args) as conn:
            elapsed = time.time() - t0
            assert elapsed == pytest.approx(2.0, 0.1)
            assert conn.info.port == int(proxy.server_port)
            assert conn.info.host == proxy.server_host


@pytest.mark.slow
@pytest.mark.timing
def test_multi_hosts_timeout(conn_cls, proxy, dsn):
    args = conninfo_to_dict(dsn)
    args["host"] = f"{proxy.client_host},{proxy.server_host}"
    args["port"] = f"{proxy.client_port},{proxy.server_port}"
    args.pop("hostaddr", None)
    args["connect_timeout"] = "2"
    with proxy.deaf_listen():
        t0 = time.time()
        with conn_cls.connect(**args) as conn:
            elapsed = time.time() - t0
            assert elapsed == pytest.approx(2.0, 0.1)
            assert conn.info.port == int(proxy.server_port)
            assert conn.info.host == proxy.server_host


def test_close(conn):
    assert not conn.closed
    assert not conn.broken

    cur = conn.cursor()

    conn.close()
    assert conn.closed
    assert not conn.broken
    assert conn.pgconn.status == pq.ConnStatus.BAD

    conn.close()
    assert conn.closed
    assert conn.pgconn.status == pq.ConnStatus.BAD

    with pytest.raises(psycopg.OperationalError):
        cur.execute("select 1")


@pytest.mark.crdb_skip("pg_terminate_backend")
def test_broken(conn):
    with pytest.raises(psycopg.OperationalError):
        conn.execute("select pg_terminate_backend(%s)", [conn.pgconn.backend_pid])
    assert conn.closed
    assert conn.broken
    conn.close()
    assert conn.closed
    assert conn.broken


def test_cursor_closed(conn):
    conn.close()
    with pytest.raises(psycopg.OperationalError):
        with conn.cursor("foo"):
            pass
    with pytest.raises(psycopg.OperationalError):
        conn.cursor("foo")
    with pytest.raises(psycopg.OperationalError):
        conn.cursor()


# TODO: the INERROR started failing in the C implementation in Python 3.12a7
# compiled with Cython-3.0.0b3, not before.


@pytest.mark.slow
@pytest.mark.xfail(
    pq.__impl__ in ("c", "binary")
    and sys.version_info[:2] == (3, 12)
    and (not is_async(__name__)),
    reason="Something with Exceptions, C, Python 3.12",
)
def test_connection_warn_close(conn_cls, dsn, recwarn, gc_collect):
    conn = conn_cls.connect(dsn)
    conn.close()
    del conn
    assert not recwarn, [str(w.message) for w in recwarn.list]

    conn = conn_cls.connect(dsn)
    del conn
    gc_collect()
    assert "IDLE" in str(recwarn.pop(ResourceWarning).message)

    conn = conn_cls.connect(dsn)
    conn.execute("select 1")
    del conn
    gc_collect()
    assert "INTRANS" in str(recwarn.pop(ResourceWarning).message)

    conn = conn_cls.connect(dsn)
    try:
        conn.execute("select wat")
    except psycopg.ProgrammingError:
        pass
    del conn
    gc_collect()
    assert "INERROR" in str(recwarn.pop(ResourceWarning).message)

    with conn_cls.connect(dsn) as conn:
        pass
    del conn
    assert not recwarn, [str(w.message) for w in recwarn.list]


@pytest.mark.usefixtures("testctx")
def test_context_commit(conn_cls, conn, dsn):
    with conn:
        with conn.cursor() as cur:
            cur.execute("insert into testctx values (42)")

    assert conn.closed
    assert not conn.broken

    with conn_cls.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("select * from testctx")
            assert cur.fetchall() == [(42,)]


@pytest.mark.usefixtures("testctx")
def test_context_rollback(conn_cls, conn, dsn):
    with pytest.raises(ZeroDivisionError):
        with conn:
            with conn.cursor() as cur:
                cur.execute("insert into testctx values (42)")
                1 / 0

    assert conn.closed
    assert not conn.broken

    with conn_cls.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("select * from testctx")
            assert cur.fetchall() == []


def test_context_close(conn):
    with conn:
        conn.execute("select 1")
        conn.close()


@pytest.mark.crdb_skip("pg_terminate_backend")
def test_context_inerror_rollback_no_clobber(conn_cls, conn, dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg")

    with pytest.raises(ZeroDivisionError):
        with conn_cls.connect(dsn) as conn2:
            conn2.execute("select 1")
            conn.execute(
                "select pg_terminate_backend(%s::int)", [conn2.pgconn.backend_pid]
            )
            1 / 0

    assert len(caplog.records) == 1
    rec = caplog.records[0]
    assert rec.levelno == logging.WARNING
    assert "in rollback" in rec.message


@pytest.mark.crdb_skip("copy")
def test_context_active_rollback_no_clobber(conn_cls, dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg")

    with pytest.raises(ZeroDivisionError):
        with conn_cls.connect(dsn) as conn:
            conn.pgconn.exec_(b"copy (select generate_series(1, 10)) to stdout")
            assert not conn.pgconn.error_message
            status = conn.info.transaction_status
            assert status == pq.TransactionStatus.ACTIVE
            1 / 0

    assert len(caplog.records) == 1
    rec = caplog.records[0]
    assert rec.levelno == logging.WARNING
    assert "in rollback" in rec.message


@pytest.mark.slow
def test_weakref(conn_cls, dsn, gc_collect):
    conn = conn_cls.connect(dsn)
    w = weakref.ref(conn)
    conn.close()
    del conn
    gc_collect()
    assert w() is None


def test_commit(conn):
    conn.pgconn.exec_(b"drop table if exists foo")
    conn.pgconn.exec_(b"create table foo (id int primary key)")
    conn.pgconn.exec_(b"begin")
    assert conn.pgconn.transaction_status == pq.TransactionStatus.INTRANS
    conn.pgconn.exec_(b"insert into foo values (1)")
    conn.commit()
    assert conn.pgconn.transaction_status == pq.TransactionStatus.IDLE
    res = conn.pgconn.exec_(b"select id from foo where id = 1")
    assert res.get_value(0, 0) == b"1"

    conn.close()
    with pytest.raises(psycopg.OperationalError):
        conn.commit()


@pytest.mark.crdb_skip("deferrable")
def test_commit_error(conn):
    conn.execute(
        """
        drop table if exists selfref;
        create table selfref (
            x serial primary key,
            y int references selfref (x) deferrable initially deferred)
        """
    )
    conn.commit()

    conn.execute("insert into selfref (y) values (-1)")
    with pytest.raises(e.ForeignKeyViolation):
        conn.commit()
    assert conn.pgconn.transaction_status == pq.TransactionStatus.IDLE
    cur = conn.execute("select 1")
    assert cur.fetchone() == (1,)


def test_rollback(conn):
    conn.pgconn.exec_(b"drop table if exists foo")
    conn.pgconn.exec_(b"create table foo (id int primary key)")
    conn.pgconn.exec_(b"begin")
    assert conn.pgconn.transaction_status == pq.TransactionStatus.INTRANS
    conn.pgconn.exec_(b"insert into foo values (1)")
    conn.rollback()
    assert conn.pgconn.transaction_status == pq.TransactionStatus.IDLE
    res = conn.pgconn.exec_(b"select id from foo where id = 1")
    assert res.ntuples == 0

    conn.close()
    with pytest.raises(psycopg.OperationalError):
        conn.rollback()


def test_auto_transaction(conn):
    conn.pgconn.exec_(b"drop table if exists foo")
    conn.pgconn.exec_(b"create table foo (id int primary key)")

    cur = conn.cursor()
    assert conn.pgconn.transaction_status == pq.TransactionStatus.IDLE

    cur.execute("insert into foo values (1)")
    assert conn.pgconn.transaction_status == pq.TransactionStatus.INTRANS

    conn.commit()
    assert conn.pgconn.transaction_status == pq.TransactionStatus.IDLE
    cur.execute("select * from foo")
    assert cur.fetchone() == (1,)
    assert conn.pgconn.transaction_status == pq.TransactionStatus.INTRANS


def test_auto_transaction_fail(conn):
    conn.pgconn.exec_(b"drop table if exists foo")
    conn.pgconn.exec_(b"create table foo (id int primary key)")

    cur = conn.cursor()
    assert conn.pgconn.transaction_status == pq.TransactionStatus.IDLE

    cur.execute("insert into foo values (1)")
    assert conn.pgconn.transaction_status == pq.TransactionStatus.INTRANS

    with pytest.raises(psycopg.DatabaseError):
        cur.execute("meh")
    assert conn.pgconn.transaction_status == pq.TransactionStatus.INERROR

    with pytest.raises(psycopg.errors.InFailedSqlTransaction):
        cur.execute("select 1")

    conn.commit()
    assert conn.pgconn.transaction_status == pq.TransactionStatus.IDLE
    cur.execute("select * from foo")
    assert cur.fetchone() is None
    assert conn.pgconn.transaction_status == pq.TransactionStatus.INTRANS


@skip_sync
def test_autocommit_readonly_property(conn):
    with pytest.raises(AttributeError):
        conn.autocommit = True
    assert not conn.autocommit


def test_autocommit(conn):
    assert conn.autocommit is False
    conn.set_autocommit(True)
    assert conn.autocommit
    cur = conn.cursor()
    cur.execute("select 1")
    assert cur.fetchone() == (1,)
    assert conn.pgconn.transaction_status == pq.TransactionStatus.IDLE

    conn.set_autocommit("")
    assert isinstance(conn.autocommit, bool)
    assert conn.autocommit is False

    conn.set_autocommit("yeah")
    assert isinstance(conn.autocommit, bool)
    assert conn.autocommit is True


@skip_async
def test_autocommit_property(conn):
    assert conn.autocommit is False

    conn.autocommit = True
    assert conn.autocommit
    cur = conn.cursor()
    cur.execute("select 1")
    assert cur.fetchone() == (1,)
    assert conn.pgconn.transaction_status == pq.TransactionStatus.IDLE

    conn.autocommit = ""
    assert isinstance(conn.autocommit, bool)
    assert conn.autocommit is False

    conn.autocommit = "yeah"
    assert isinstance(conn.autocommit, bool)
    assert conn.autocommit is True


def test_autocommit_connect(conn_cls, dsn):
    conn = conn_cls.connect(dsn, autocommit=True)
    assert conn.autocommit
    conn.close()


def test_autocommit_intrans(conn):
    cur = conn.cursor()
    cur.execute("select 1")
    assert cur.fetchone() == (1,)
    assert conn.pgconn.transaction_status == pq.TransactionStatus.INTRANS
    with pytest.raises(psycopg.ProgrammingError):
        conn.set_autocommit(True)
    assert not conn.autocommit


def test_autocommit_inerror(conn):
    cur = conn.cursor()
    with pytest.raises(psycopg.DatabaseError):
        cur.execute("meh")
    assert conn.pgconn.transaction_status == pq.TransactionStatus.INERROR
    with pytest.raises(psycopg.ProgrammingError):
        conn.set_autocommit(True)
    assert not conn.autocommit


def test_autocommit_unknown(conn):
    conn.close()
    assert conn.pgconn.transaction_status == pq.TransactionStatus.UNKNOWN
    with pytest.raises(psycopg.OperationalError):
        conn.set_autocommit(True)
    assert not conn.autocommit


@pytest.mark.parametrize(
    "args, kwargs, want",
    [
        ((), {}, ""),
        (("",), {}, ""),
        (("host=foo.com user=bar",), {}, "host=foo.com user=bar hostaddr=1.1.1.1"),
        (("host=foo.com",), {"user": "baz"}, "host=foo.com user=baz hostaddr=1.1.1.1"),
        (
            ("dbname=foo port=5433",),
            {"dbname": "qux", "user": "joe"},
            "dbname=qux user=joe port=5433",
        ),
        (("host=foo.com",), {"user": None}, "host=foo.com hostaddr=1.1.1.1"),
    ],
)
def test_connect_args(
    conn_cls, monkeypatch, setpgenv, pgconn, fake_resolve, args, kwargs, want
):
    got_conninfo: str

    def fake_connect(conninfo, *, timeout=0.0):
        nonlocal got_conninfo
        got_conninfo = conninfo
        return pgconn
        yield

    setpgenv({})
    monkeypatch.setattr(psycopg.generators, "connect", fake_connect)
    conn = conn_cls.connect(*args, **kwargs)
    assert conninfo_to_dict(got_conninfo) == conninfo_to_dict(want)
    conn.close()


@pytest.mark.parametrize(
    "args, kwargs, exctype",
    [
        (("host=foo", "host=bar"), {}, TypeError),
        (("", ""), {}, TypeError),
        ((), {"nosuchparam": 42}, psycopg.ProgrammingError),
    ],
)
def test_connect_badargs(conn_cls, monkeypatch, pgconn, args, kwargs, exctype):
    with pytest.raises(exctype):
        conn_cls.connect(*args, **kwargs)


@pytest.mark.crdb_skip("pg_terminate_backend")
def test_broken_connection(conn):
    cur = conn.cursor()
    with pytest.raises(psycopg.DatabaseError):
        cur.execute("select pg_terminate_backend(pg_backend_pid())")
    assert conn.closed


@pytest.mark.crdb_skip("do")
def test_notice_handlers(conn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg")
    messages = []
    severities = []

    def cb1(diag):
        messages.append(diag.message_primary)

    def cb2(res):
        raise Exception("hello from cb2")

    conn.add_notice_handler(cb1)
    conn.add_notice_handler(cb2)
    conn.add_notice_handler("the wrong thing")
    conn.add_notice_handler(lambda diag: severities.append(diag.severity_nonlocalized))

    conn.pgconn.exec_(b"set client_min_messages to notice")
    cur = conn.cursor()
    cur.execute("do $$begin raise notice 'hello notice'; end$$ language plpgsql")
    assert messages == ["hello notice"]
    assert severities == ["NOTICE"]

    assert len(caplog.records) == 2
    rec = caplog.records[0]
    assert rec.levelno == logging.ERROR
    assert "hello from cb2" in rec.message
    rec = caplog.records[1]
    assert rec.levelno == logging.ERROR
    assert "the wrong thing" in rec.message

    conn.remove_notice_handler(cb1)
    conn.remove_notice_handler("the wrong thing")
    cur.execute("do $$begin raise warning 'hello warning'; end$$ language plpgsql")
    assert len(caplog.records) == 3
    assert messages == ["hello notice"]
    assert severities == ["NOTICE", "WARNING"]

    with pytest.raises(ValueError):
        conn.remove_notice_handler(cb1)


def test_execute(conn):
    cur = conn.execute("select %s, %s", [10, 20])
    assert cur.fetchone() == (10, 20)
    assert cur.format == 0
    assert cur.pgresult.fformat(0) == 0

    cur = conn.execute("select %(a)s, %(b)s", {"a": 11, "b": 21})
    assert cur.fetchone() == (11, 21)

    cur = conn.execute("select 12, 22")
    assert cur.fetchone() == (12, 22)


def test_execute_binary(conn):
    cur = conn.execute("select %s, %s", [10, 20], binary=True)
    assert cur.fetchone() == (10, 20)
    assert cur.format == 1
    assert cur.pgresult.fformat(0) == 1


def test_row_factory(conn_cls, dsn):
    defaultconn = conn_cls.connect(dsn)
    assert defaultconn.row_factory is tuple_row
    defaultconn.close()

    conn = conn_cls.connect(dsn, row_factory=my_row_factory)
    assert conn.row_factory is my_row_factory

    cur = conn.execute("select 'a' as ve")
    assert cur.fetchone() == ["Ave"]

    with conn.cursor(row_factory=lambda c: lambda t: set(t)) as cur1:
        cur1.execute("select 1, 1, 2")
        assert cur1.fetchall() == [{1, 2}]

    with conn.cursor(row_factory=tuple_row) as cur2:
        cur2.execute("select 1, 1, 2")
        assert cur2.fetchall() == [(1, 1, 2)]

    # TODO: maybe fix something to get rid of 'type: ignore' below.
    conn.row_factory = tuple_row
    cur3 = conn.execute("select 'vale'")
    r = cur3.fetchone()
    assert r and r == ("vale",)
    conn.close()


def test_str(conn):
    assert "[IDLE]" in str(conn)
    conn.close()
    assert "[BAD]" in str(conn)


def test_fileno(conn):
    assert conn.fileno() == conn.pgconn.socket
    conn.close()
    with pytest.raises(psycopg.OperationalError):
        conn.fileno()


def test_cursor_factory(conn):
    assert conn.cursor_factory is psycopg.Cursor

    class MyCursor(psycopg.Cursor[psycopg.rows.Row]):
        pass

    conn.cursor_factory = MyCursor
    with conn.cursor() as cur:
        assert isinstance(cur, MyCursor)

    with conn.execute("select 1") as cur:
        assert isinstance(cur, MyCursor)


def test_cursor_factory_connect(conn_cls, dsn):

    class MyCursor(psycopg.Cursor[psycopg.rows.Row]):
        pass

    with conn_cls.connect(dsn, cursor_factory=MyCursor) as conn:
        assert conn.cursor_factory is MyCursor
        cur = conn.cursor()
        assert type(cur) is MyCursor


def test_server_cursor_factory(conn):
    assert conn.server_cursor_factory is psycopg.ServerCursor

    class MyServerCursor(psycopg.ServerCursor[psycopg.rows.Row]):
        pass

    conn.server_cursor_factory = MyServerCursor
    with conn.cursor(name="n") as cur:
        assert isinstance(cur, MyServerCursor)


@pytest.mark.parametrize("param", tx_params)
def test_transaction_param_default(conn, param):
    assert getattr(conn, param.name) is None
    cur = conn.execute(
        "select current_setting(%s), current_setting(%s)",
        [f"transaction_{param.guc}", f"default_transaction_{param.guc}"],
    )
    current, default = cur.fetchone()
    assert current == default


@skip_sync
@pytest.mark.parametrize("param", tx_params)
def test_transaction_param_readonly_property(conn, param):
    with pytest.raises(AttributeError):
        setattr(conn, param.name, None)


@pytest.mark.parametrize("autocommit", [True, False])
@pytest.mark.parametrize("param", tx_params_isolation)
def test_set_transaction_param_implicit(conn, param, autocommit):
    conn.set_autocommit(autocommit)
    for value in param.values:
        getattr(conn, f"set_{param.name}")(value)
        cur = conn.execute(
            "select current_setting(%s), current_setting(%s)",
            [f"transaction_{param.guc}", f"default_transaction_{param.guc}"],
        )
        pgval, default = cur.fetchone()
        if autocommit:
            assert pgval == default
        else:
            assert tx_values_map[pgval] == value
        conn.rollback()


@pytest.mark.parametrize("param", tx_params_isolation)
def test_set_transaction_param_reset(conn, param):
    conn.execute(
        "select set_config(%s, %s, false)",
        [f"default_transaction_{param.guc}", param.non_default],
    )
    conn.commit()

    for value in param.values:
        getattr(conn, f"set_{param.name}")(value)
        cur = conn.execute("select current_setting(%s)", [f"transaction_{param.guc}"])
        (pgval,) = cur.fetchone()
        assert tx_values_map[pgval] == value
        conn.rollback()

        getattr(conn, f"set_{param.name}")(None)
        cur = conn.execute("select current_setting(%s)", [f"transaction_{param.guc}"])
        (pgval,) = cur.fetchone()
        assert tx_values_map[pgval] == tx_values_map[param.non_default]
        conn.rollback()


@pytest.mark.parametrize("autocommit", [True, False])
@pytest.mark.parametrize("param", tx_params_isolation)
def test_set_transaction_param_block(conn, param, autocommit):
    conn.set_autocommit(autocommit)
    for value in param.values:
        getattr(conn, f"set_{param.name}")(value)
        with conn.transaction():
            cur = conn.execute(
                "select current_setting(%s)", [f"transaction_{param.guc}"]
            )
            pgval = cur.fetchone()[0]
        assert tx_values_map[pgval] == value


@pytest.mark.parametrize("param", tx_params)
def test_set_transaction_param_not_intrans_implicit(conn, param):
    conn.execute("select 1")
    value = param.values[0]
    with pytest.raises(psycopg.ProgrammingError):
        getattr(conn, f"set_{param.name}")(value)


@pytest.mark.parametrize("param", tx_params)
def test_set_transaction_param_not_intrans_block(conn, param):
    value = param.values[0]
    with conn.transaction():
        with pytest.raises(psycopg.ProgrammingError):
            getattr(conn, f"set_{param.name}")(value)


@pytest.mark.parametrize("param", tx_params)
def test_set_transaction_param_not_intrans_external(conn, param):
    value = param.values[0]
    conn.set_autocommit(True)
    conn.execute("begin")
    with pytest.raises(psycopg.ProgrammingError):
        getattr(conn, f"set_{param.name}")(value)


@skip_async
@pytest.mark.crdb("skip", reason="transaction isolation")
def test_set_transaction_param_all_property(conn):
    params: list[Any] = tx_params[:]
    params[2] = params[2].values[0]

    for param in params:
        value = param.values[0]
        setattr(conn, param.name, value)

    for param in params:
        cur = conn.execute("select current_setting(%s)", [f"transaction_{param.guc}"])
        pgval = cur.fetchone()[0]
        assert tx_values_map[pgval] == value


@pytest.mark.crdb("skip", reason="transaction isolation")
def test_set_transaction_param_all(conn):
    params: list[Any] = tx_params[:]
    params[2] = params[2].values[0]

    for param in params:
        value = param.values[0]
        getattr(conn, f"set_{param.name}")(value)

    for param in params:
        cur = conn.execute("select current_setting(%s)", [f"transaction_{param.guc}"])
        pgval = cur.fetchone()[0]
        assert tx_values_map[pgval] == value


def test_set_transaction_param_strange(conn):
    for val in ("asdf", 0, 5):
        with pytest.raises(ValueError):
            conn.set_isolation_level(val)

    conn.set_isolation_level(psycopg.IsolationLevel.SERIALIZABLE.value)
    assert conn.isolation_level is psycopg.IsolationLevel.SERIALIZABLE

    conn.set_read_only(1)
    assert conn.read_only is True

    conn.set_deferrable(0)
    assert conn.deferrable is False


@skip_async
def test_set_transaction_param_strange_property(conn):
    for val in ("asdf", 0, 5):
        with pytest.raises(ValueError):
            conn.isolation_level = val

    conn.isolation_level = psycopg.IsolationLevel.SERIALIZABLE.value
    assert conn.isolation_level is psycopg.IsolationLevel.SERIALIZABLE

    conn.read_only = 1
    assert conn.read_only is True

    conn.deferrable = 0
    assert conn.deferrable is False


@pytest.mark.parametrize("dsn, kwargs, exp", conninfo_params_timeout)
def test_get_connection_params(conn_cls, dsn, kwargs, exp, setpgenv):
    setpgenv({})
    params = conn_cls._get_connection_params(dsn, **kwargs)
    assert params == exp[0]
    assert timeout_from_conninfo(params) == exp[1]


def test_connect_context_adapters(conn_cls, dsn):
    ctx = psycopg.adapt.AdaptersMap(psycopg.adapters)
    ctx.register_dumper(str, make_bin_dumper("b"))
    ctx.register_dumper(str, make_dumper("t"))

    conn = conn_cls.connect(dsn, context=ctx)

    cur = conn.execute("select %s", ["hello"])
    assert cur.fetchone()[0] == "hellot"
    cur = conn.execute("select %b", ["hello"])
    assert cur.fetchone()[0] == "hellob"
    conn.close()


def test_connect_context_copy(conn_cls, dsn, conn):
    conn.adapters.register_dumper(str, make_bin_dumper("b"))
    conn.adapters.register_dumper(str, make_dumper("t"))

    conn2 = conn_cls.connect(dsn, context=conn)

    cur = conn2.execute("select %s", ["hello"])
    assert cur.fetchone()[0] == "hellot"
    cur = conn2.execute("select %b", ["hello"])
    assert cur.fetchone()[0] == "hellob"
    conn2.close()


def test_cancel_closed(conn):
    conn.close()
    conn.cancel()


def test_cancel_safe_closed(conn):
    conn.close()
    conn.cancel_safe()


@pytest.mark.slow
@pytest.mark.timing
def test_cancel_safe_error(conn_cls, proxy, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg")
    proxy.start()
    with conn_cls.connect(proxy.client_dsn) as conn:
        proxy.stop()
        with pytest.raises(
            e.OperationalError, match="(Connection refused)|(connect\\(\\) failed)"
        ) as ex:
            conn.cancel_safe(timeout=2)
        assert not caplog.records

        # Note: testing an internal method. It's ok if this behaviour changes
        conn._try_cancel(timeout=2.0)
        assert len(caplog.records) == 1
        caplog.records[0].message == str(ex.value)


@pytest.mark.slow
@pytest.mark.timing
@pytest.mark.libpq(">= 17")
def test_cancel_safe_timeout(conn_cls, proxy):
    proxy.start()
    with conn_cls.connect(proxy.client_dsn) as conn:
        proxy.stop()
        with proxy.deaf_listen():
            t0 = time.time()
            with pytest.raises(e.CancellationTimeout, match="timeout expired"):
                conn.cancel_safe(timeout=1)
    elapsed = time.time() - t0
    assert elapsed == pytest.approx(1.0, 0.1)


def test_resolve_hostaddr_conn(conn_cls, monkeypatch, fake_resolve):
    got = ""

    def fake_connect_gen(conninfo, **kwargs):
        nonlocal got
        got = conninfo
        1 / 0

    monkeypatch.setattr(conn_cls, "_connect_gen", fake_connect_gen)

    with pytest.raises(ZeroDivisionError):
        conn_cls.connect("host=foo.com")

    assert conninfo_to_dict(got) == {"host": "foo.com", "hostaddr": "1.1.1.1"}
