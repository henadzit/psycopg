"""
Entry point into the adaptation system.
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
from functools import partial

from . import exceptions as exc
from .pq import Format
from .cursor import BaseCursor
from .types.oids import type_oid, INVALID_OID
from .connection import BaseConnection


class Adapter:
    globals = {}

    def __init__(self, cls, conn):
        self.cls = cls
        self.conn = conn

    def adapt(self, obj):
        raise NotImplementedError()

    @staticmethod
    def register(cls, adapter=None, context=None, format=Format.TEXT):
        if adapter is None:
            # used as decorator
            return partial(Adapter.register, cls, format=format)

        if not isinstance(cls, type):
            raise TypeError(
                f"adapters should be registered on classes, got {cls} instead"
            )

        if context is not None and not isinstance(
            context, (BaseConnection, BaseCursor)
        ):
            raise TypeError(
                f"the context should be a connection or cursor,"
                f" got {type(context).__name__}"
            )

        if not (
            callable(adapter)
            or (isinstance(adapter, type) and issubclass(adapter, Adapter))
        ):
            raise TypeError(
                f"adapters should be callable or Adapter subclasses,"
                f" got {adapter} instead"
            )

        where = context.adapters if context is not None else Adapter.globals
        where[cls, format] = adapter
        return adapter

    @staticmethod
    def register_binary(cls, adapter=None, context=None):
        return Adapter.register(cls, adapter, context, format=Format.BINARY)


class Typecaster:
    globals = {}

    def __init__(self, oid, conn):
        self.oid = oid
        self.conn = conn

    def cast(self, data):
        raise NotImplementedError()

    @staticmethod
    def register(oid, caster=None, context=None, format=Format.TEXT):
        if caster is None:
            # used as decorator
            return partial(Typecaster.register, oid, format=format)

        if not isinstance(oid, int):
            raise TypeError(
                f"typecasters should be registered on oid, got {oid} instead"
            )

        if context is not None and not isinstance(
            context, (BaseConnection, BaseCursor)
        ):
            raise TypeError(
                f"the context should be a connection or cursor,"
                f" got {type(context).__name__}"
            )

        if not (
            callable(caster)
            or (isinstance(caster, type) and issubclass(caster, Typecaster))
        ):
            raise TypeError(
                f"adapters should be callable or Typecaster subclasses,"
                f" got {caster} instead"
            )

        where = context.adapters if context is not None else Typecaster.globals
        where[oid, format] = caster
        return caster

    @staticmethod
    def register_binary(oid, caster=None, context=None):
        return Typecaster.register(oid, caster, context, format=Format.BINARY)


class Transformer:
    """
    An object that can adapt efficiently between Python and PostgreSQL.

    The life cycle of the object is the query, so it is assumed that stuff like
    the server version or connection encoding will not change. It can have its
    state so adapting several values of the same type can use optimisations.
    """

    def __init__(self, context):
        if context is None:
            self.connection = None
            self.cursor = None
        elif isinstance(context, BaseConnection):
            self.connection = context
            self.cursor = None
        elif isinstance(context, BaseCursor):
            self.connection = context.conn
            self.cursor = context
        else:
            raise TypeError(
                f"the context should be a connection or cursor,"
                f" got {type(context).__name__}"
            )

        # mapping class, fmt -> adaptation function
        self._adapt_funcs = {}

        # mapping oid, fmt -> cast function
        self._cast_funcs = {}

        # The result to return values from
        self._result = None

        # sequence of cast function from value to python
        # the length of the result columns
        self._row_casters = None

    @property
    def result(self):
        return self._result

    @result.setter
    def result(self, result):
        if self._result is result:
            return

        rc = self._row_casters = []
        for c in range(result.nfields):
            oid = result.ftype(c)
            fmt = result.fformat(c)
            func = self.get_cast_function(oid, fmt)
            rc.append(func)

    def adapt_sequence(self, objs, fmts):
        out = []
        types = []

        for var, fmt in zip(objs, fmts):
            data = self.adapt(var, fmt)
            if isinstance(data, tuple):
                oid = data[1]
                data = data[0]
            else:
                oid = type_oid["text"]

            out.append(data)
            types.append(oid)

        return out, types

    def adapt(self, obj, fmt):
        if obj is None:
            return None, type_oid["text"]

        cls = type(obj)
        func = self.get_adapt_function(cls, fmt)
        return func(obj)

    def get_adapt_function(self, cls, fmt):
        try:
            return self._adapt_funcs[cls, fmt]
        except KeyError:
            pass

        adapter = self.lookup_adapter(cls, fmt)
        if isinstance(adapter, type):
            adapter = adapter(cls, self.connection).adapt

        return adapter

    def lookup_adapter(self, cls, fmt):
        key = (cls, fmt)

        cur = self.cursor
        if cur is not None and key in cur.adapters:
            return cur.adapters[key]

        conn = self.connection
        if conn is not None and key in conn.adapters:
            return conn.adapters[key]

        if key in Adapter.globals:
            return Adapter.globals[key]

        raise exc.ProgrammingError(
            f"cannot adapt type {cls.__name__} to format {Format(fmt).name}"
        )

    def cast_row(self, result, n):
        self.result = result

        for col, func in enumerate(self._row_casters):
            v = result.get_value(n, col)
            if v is not None:
                v = func(v)
            yield v

    def get_cast_function(self, oid, fmt):
        try:
            return self._cast_funcs[oid, fmt]
        except KeyError:
            pass

        caster = self.lookup_caster(oid, fmt)
        if isinstance(caster, type):
            caster = caster(oid, self.connection).cast

        return caster

    def lookup_caster(self, oid, fmt):
        key = (oid, fmt)

        cur = self.cursor
        if cur is not None and key in cur.casters:
            return cur.casters[key]

        conn = self.connection
        if conn is not None and key in conn.casters:
            return conn.casters[key]

        if key in Typecaster.globals:
            return Typecaster.globals[key]

        return Typecaster.globals[INVALID_OID, fmt]


@Typecaster.register(INVALID_OID)
class UnknownCaster(Typecaster):
    """
    Fallback object to convert unknown types to Python
    """

    def __init__(self, oid, conn):
        super().__init__(oid, conn)
        if conn is not None:
            self.decode = conn.codec.decode
        else:
            self.decode = codecs.lookup("utf8").decode

    def cast(self, data):
        return self.decode(data)[0]


@Typecaster.register_binary(INVALID_OID)
def cast_unknown(data):
    return data
