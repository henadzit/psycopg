cimport cython

from types import ModuleType
from cpython.bytes cimport PyBytes_AsString
from cpython.long cimport PyLong_FromUnsignedLongLong
from cpython.unicode cimport PyUnicode_AsUTF8


uuid: ModuleType | None = None


@cython.final
cdef class UUIDDumper(CDumper):
    format = PQ_TEXT
    oid = oids.UUID_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef const char *src = PyUnicode_AsUTF8(obj.hex)
        cdef char *buf = CDumper.ensure_size(rv, offset, 32)
        memcpy(buf, src, 32)
        return 32


@cython.final
cdef class UUIDBinaryDumper(CDumper):
    format = PQ_BINARY
    oid = oids.UUID_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef const char *src = PyBytes_AsString(obj.bytes)
        cdef char *buf = CDumper.ensure_size(rv, offset, 16)
        memcpy(buf, src, 16)
        return 16

cdef extern from *:
    """
static const char hex_to_int_map[] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 0-15
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 16-31
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 32-47
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0, 0, 0, 0,  // 48-63 ('0'-'9')
    0, 10, 11, 12, 13, 14, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 64-79 ('A'-'F')
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 80-95
    0, 10, 11, 12, 13, 14, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 96-111 ('a'-'f')
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 112-127
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 128-143
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 144-159
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 160-175
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 176-191
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 192-207
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 208-223
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 224-239
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0   // 240-255
};
"""
    const char[256] hex_to_int_map


@cython.final
cdef class UUIDLoader(CLoader):
    format = PQ_TEXT

    def __cinit__(self, oid: int, context: AdaptContext | None = None):
        global uuid
        # uuid is slow to import, lazy load it
        if uuid is None:
            import uuid

    cdef object cload(self, const char *data, size_t length):
        cdef uint64_t high = 0
        cdef uint64_t low = 0
        cdef int i
        cdef int ndigits = 0
        cdef char c

        for i in range(length):
            c = data[i]
            if c == b'-':
                continue

            if ndigits < 16:
                high = (high << 4) | hex_to_int_map[c]
            else:
                low = (low << 4) | hex_to_int_map[c]
            ndigits += 1

        cdef object py_high = PyLong_FromUnsignedLongLong(high)
        cdef object py_low = PyLong_FromUnsignedLongLong(low)

        u = uuid.UUID.__new__(uuid.UUID)
        object.__setattr__(u, 'is_safe', uuid.SafeUUID.unknown)
        object.__setattr__(u, 'int', (py_high << 64) | py_low)
        return u


@cython.final
cdef class UUIDBinaryLoader(CLoader):
    format = PQ_BINARY

    def __cinit__(self, oid: int, context: AdaptContext | None = None):
        global uuid
        # uuid is slow to import, lazy load it
        if uuid is None:
            import uuid

    cdef object cload(self, const char *data, size_t length):
        cdef unsigned long long high = 0
        cdef unsigned long long low = 0
        cdef int i

        # Construct the 128-bit integer from the bytes in big-endian order
        for i in range(8):
            high = (high << 8) | <unsigned char>data[i]
        for i in range(8, 16):
            low = (low << 8) | <unsigned char>data[i]

        cdef object py_high = PyLong_FromUnsignedLongLong(high)
        cdef object py_low = PyLong_FromUnsignedLongLong(low)

        u = uuid.UUID.__new__(uuid.UUID)
        object.__setattr__(u, 'is_safe', uuid.SafeUUID.unknown)
        object.__setattr__(u, 'int', (py_high << 64) | py_low)
        return u
