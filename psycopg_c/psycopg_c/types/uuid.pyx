cimport cython

from cpython.bytes cimport PyBytes_AsString
from cpython.unicode cimport PyUnicode_AsUTF8


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


@cython.final
cdef class UUIDLoader(CLoader):
    format = PQ_TEXT

    cdef object cload(self, const char *data, size_t length):
        cdef char[33] hex_str
        cdef int i
        cdef int j = 0
        for i in range(36):
            if data[i] == b'-':
                continue
            hex_str[j] = data[i]
            j += 1
        hex_str[32] = 0

        # uuid is slow to import, lazy load it
        import uuid

        u = uuid.UUID.__new__(uuid.UUID)
        object.__setattr__(u, 'is_safe', uuid.SafeUUID.unknown)
        object.__setattr__(u, 'int', PyLong_FromString(hex_str, NULL, 16))
        return u


@cython.final
cdef class UUIDBinaryLoader(CLoader):
    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        # uuid is slow to import, lazy load it
        import uuid

        u = uuid.UUID.__new__(uuid.UUID)
        object.__setattr__(u, 'is_safe', uuid.SafeUUID.unknown)
        object.__setattr__(u, 'int', int.from_bytes(data[:length], 'big'))
        return u
