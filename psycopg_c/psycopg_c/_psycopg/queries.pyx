# cython: language_level=3
"""
Utility module to manipulate queries (Cython version)
"""

import cython
import re
from functools import lru_cache
from collections.abc import Mapping, Sequence

# Import project modules
from psycopg import errors as e
from psycopg import pq
from psycopg.abc import Buffer, Params, Query
from psycopg.sql import Composable
from psycopg._enums import PyFormat
from psycopg._compat import TypeAlias, TypeGuard
from psycopg._encodings import conn_encoding

# Constants
cdef int MAX_CACHED_STATEMENT_LENGTH = 4096
cdef int MAX_CACHED_STATEMENT_PARAMS = 50

# Preserve the NamedTuple style for QueryPart for compatibility.
from typing import NamedTuple

class QueryPart(NamedTuple):
    pre: bytes
    item: object  # int or str
    format: PyFormat


cdef class PostgresQuery:
    """
    Helper to convert a Python query and parameters into Postgres format.
    """
    cdef public bytes query
    cdef public object params       # Sequence[Buffer|None] or None
    cdef public tuple types         # tuple of ints
    cdef public object formats      # Sequence[pq.Format] or None

    cdef object _tx
    cdef object _want_formats      # list[PyFormat] or None
    cdef list _parts               # list of QueryPart
    cdef str _encoding
    cdef object _order             # list[str] or None

    def __init__(self, transformer):
        self._tx = transformer
        self.params = None
        self.types = ()
        self._want_formats = None
        self.formats = None
        self._encoding = conn_encoding(transformer.connection)
        self._parts = []
        self.query = b""
        self._order = None

    def convert(self, query, vars):
        """
        Set up the query and parameters to convert.
        The results are available in attributes: query, params, types, formats.
        """
        cdef bytes bquery
        if isinstance(query, str):
            bquery = query.encode(self._encoding)
        elif isinstance(query, Composable):
            bquery = query.as_bytes(self._tx)
        else:
            bquery = query

        if vars is not None:
            if len(bquery) <= MAX_CACHED_STATEMENT_LENGTH and len(vars) <= MAX_CACHED_STATEMENT_PARAMS:
                f = _query2pg
            else:
                f = _query2pg_nocache
            (self.query, self._want_formats, self._order, self._parts) = f(bquery, self._encoding)
        else:
            self.query = bquery
            self._want_formats = self._order = None

        self.dump(vars)

    def dump(self, vars):
        """
        Process a new set of variables on the query processed by convert().
        Updates self.params and self.types.
        """
        if vars is not None:
            params = PostgresQuery.validate_and_reorder_params(self._parts, vars, self._order)
            # Dump the sequence using the transformer’s method.
            self.params = self._tx.dump_sequence(params, self._want_formats)
            self.types = self._tx.types or ()
            self.formats = self._tx.formats
        else:
            self.params = None
            self.types = ()
            self.formats = None

    @staticmethod
    def is_params_sequence(vars):
        # Try concrete types first, then abstract types.
        t = type(vars)
        if t is list or t is tuple:
            sequence = True
        elif t is dict:
            sequence = False
        elif isinstance(vars, Sequence) and not isinstance(vars, (bytes, str)):
            sequence = True
        elif isinstance(vars, Mapping):
            sequence = False
        else:
            raise TypeError(
                "query parameters should be a sequence or a mapping, got %s" % type(vars).__name__
            )
        return sequence

    @staticmethod
    def validate_and_reorder_params(parts, vars, order):
        """
        Verify the compatibility between a query and a set of params.
        """
        if PostgresQuery.is_params_sequence(vars):
            if len(vars) != len(parts) - 1:
                raise e.ProgrammingError(
                    "the query has %d placeholders but %d parameters were passed" %
                    (len(parts) - 1, len(vars))
                )
            if vars and not isinstance(parts[0].item, int):
                raise TypeError("named placeholders require a mapping of parameters")
            return vars
        else:
            if vars and len(parts) > 1 and not isinstance(parts[0].item, str):
                raise TypeError(
                    "positional placeholders (%s) require a sequence of parameters"
                )
            try:
                if order:
                    return [vars[item] for item in order]
                else:
                    return ()
            except KeyError:
                missing = ", ".join(sorted(i for i in order or () if i not in vars))
                raise e.ProgrammingError("query parameter missing: " + missing)


cdef class PostgresClientQuery(PostgresQuery):
    """
    PostgresQuery subclass merging query and arguments client-side.
    """
    cdef public bytes template

    def convert(self, query, vars):
        """
        Set up the query and parameters to convert.
        Results are available in attributes: query, params, types, formats.
        """
        cdef bytes bquery
        if isinstance(query, str):
            bquery = query.encode(self._encoding)
        elif isinstance(query, Composable):
            bquery = query.as_bytes(self._tx)
        else:
            bquery = query

        if vars is not None:
            if len(bquery) <= MAX_CACHED_STATEMENT_LENGTH and len(vars) <= MAX_CACHED_STATEMENT_PARAMS:
                f = _query2pg_client
            else:
                f = _query2pg_client_nocache
            (self.template, self._order, self._parts) = f(bquery, self._encoding)
        else:
            self.query = bquery
            self._order = None

        self.dump(vars)

    def dump(self, vars):
        """
        Process a new set of variables on the query processed by convert().
        Updates self.params and self.types.
        """
        if vars is not None:
            params = PostgresQuery.validate_and_reorder_params(self._parts, vars, self._order)
            self.params = tuple(
                self._tx.as_literal(p) if p is not None else b"NULL" for p in params
            )
            self.query = self.template % self.params
        else:
            self.params = None


#
# Conversion functions
#

cpdef tuple _query2pg_nocache(bytes query, str encoding):
    """
    Convert Python query and params into something Postgres understands.
    Replaces Python placeholders with Postgres ones.
    """
    cdef list parts = _split_query(query, encoding)
    cdef list order = None
    cdef list chunks = []
    cdef list formats = []
    cdef dict seen = {}

    if isinstance(parts[0].item, int):
        for part in parts[:-1]:
            chunks.append(part.pre)
            chunks.append(b"$%d" % (part.item + 1))
            formats.append(part.format)
    elif isinstance(parts[0].item, str):
        order = []
        for part in parts[:-1]:
            chunks.append(part.pre)
            if part.item not in seen:
                ph = b"$%d" % (len(seen) + 1)
                seen[part.item] = (ph, part.format)
                order.append(part.item)
                chunks.append(ph)
                formats.append(part.format)
            else:
                if seen[part.item][1] != part.format:
                    raise e.ProgrammingError(
                        "placeholder '%s' cannot have different formats" % part.item
                    )
                chunks.append(seen[part.item][0])
    # Append the final part.
    chunks.append(parts[-1].pre)
    return b"".join(chunks), formats, order, parts

_query2pg = lru_cache(_query2pg_nocache)


cpdef tuple _query2pg_client_nocache(bytes query, str encoding):
    """
    Convert Python query and params into a template for client-side binding.
    """
    cdef list parts = _split_query(query, encoding, collapse_double_percent=False)
    cdef list order = None
    cdef list chunks = []
    cdef dict seen = {}

    if isinstance(parts[0].item, int):
        for part in parts[:-1]:
            chunks.append(part.pre)
            chunks.append(b"%s")
    elif isinstance(parts[0].item, str):
        order = []
        for part in parts[:-1]:
            chunks.append(part.pre)
            if part.item not in seen:
                ph = b"%s"
                seen[part.item] = (ph, part.format)
                order.append(part.item)
                chunks.append(ph)
            else:
                chunks.append(seen[part.item][0])
                order.append(part.item)
    # Append the final part.
    chunks.append(parts[-1].pre)
    return b"".join(chunks), order, parts

_query2pg_client = lru_cache(_query2pg_client_nocache)


# Precompile the placeholder regex.
cdef object _re_placeholder = re.compile(
    rb"""(?x)
        %                       # a literal %
        (?:
            (?:
                \( ([^)]+) \)   # or a name in (braces)
                .               # followed by a format
            )
            |
            (?:.)               # or any char, really
        )
        """
)


cpdef list _split_query(bytes query, str encoding="ascii", bint collapse_double_percent=True):
    """
    Split the query into parts using the _re_placeholder regex.
    Returns a list of QueryPart.
    """
    cdef list parts = []
    cdef int cur = 0
    cdef object m = None
    # Build a list of tuples (fragment, match)
    for m in _re_placeholder.finditer(query):
        parts.append((query[cur: m.span(0)[0]], m))
        cur = m.span(0)[1]
    if m:
        parts.append((query[cur:], None))
    else:
        parts.append((query, None))

    cdef list rv = []
    cdef int i = 0
    cdef object phtype = None
    cdef bytes ph
    cdef object item
    while i < len(parts):
        pre, m = parts[i]
        if m is None:
            rv.append(QueryPart(pre, 0, PyFormat.AUTO))
            break

        ph = m.group(0)
        if ph == b"%%":
            if collapse_double_percent:
                ph = b"%"
            pre1, m1 = parts[i+1]
            parts[i+1] = (pre + ph + pre1, m1)
            del parts[i]
            continue

        if ph == b"%(":
            raise e.ProgrammingError(
                "incomplete placeholder: '%s'" % query[m.span(0)[0]:].split()[0].decode(encoding)
            )
        elif ph == b"% ":
            raise e.ProgrammingError(
                "incomplete placeholder: '%'; if you want to use '%%' as an operator you can double it up"
            )
        elif ph[-1:] not in b"sbt":
            raise e.ProgrammingError(
                "only '%s', '%b', '%t' are allowed as placeholders, got '%s'" %
                (b"%s", b"%s", m.group(0).decode(encoding))
            )
        if m.group(1) is not None:
            item = m.group(1).decode(encoding)
        else:
            item = i

        if phtype is None:
            phtype = type(item)
        elif phtype is not type(item):
            raise e.ProgrammingError("positional and named placeholders cannot be mixed")

        rv.append(QueryPart(pre, item, _ph_to_fmt[ph[-1:]]))
        i += 1
    return rv


cdef dict _ph_to_fmt = {
    b"s": PyFormat.AUTO,
    b"t": PyFormat.TEXT,
    b"b": PyFormat.BINARY,
}
