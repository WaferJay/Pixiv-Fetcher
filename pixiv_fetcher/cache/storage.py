# -*- coding: utf-8 -*-
import struct
import threading
from weakref import WeakValueDictionary

import os

from pixiv_fetcher.utils.path import make_direct_open


class BaseStorage(object):

    def set(self, key, value):
        raise NotImplemented()

    def get(self, key, default=None):
        raise NotImplemented()

    def has(self, key):
        raise NotImplemented()

    def delete(self, key):
        raise NotImplemented()

    def clear(self):
        raise NotImplemented()

    @property
    def count(self):
        raise NotImplemented()

    @property
    def size(self):
        raise NotImplemented()


class SimpleStorage(BaseStorage):

    class Entry(object):

        def __init__(self, key, value, size):
            self.key = key
            self.value = value
            self.size = size
            self.lock = threading.RLock()

        def __enter__(self):
            self.lock.__enter__()

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.lock.__exit__(exc_type, exc_val, exc_tb)

    def __init__(self, size_func=None):
        self._data = {}
        self._total_size = 0
        self._size_func = size_func or len

        self._lock = threading.RLock()

    def set(self, key, value):
        value_size = self._size_func(value)

        if key not in self._data:
            with self._lock:
                if key not in self._data:
                    entry = self.Entry(key, value, value_size)
                    self._data[key] = entry
                    self._total_size += value_size
                    return True

        entry = self._data.get(key)
        if entry is not None:
            with entry, self._lock:
                self._total_size = self._total_size - entry.size + value_size
                entry.value = value
                entry.size = value_size

        return False

    def get(self, key, default=None):
        entry = self._data.get(key, default)
        return default if entry is None else entry.value

    def has(self, key):
        return key in self._data

    def delete(self, key, verbose=True):
        if verbose:
            entry = self._data.pop(key, None)
        else:
            entry = self._data.pop(key)

        if entry is None:
            return None

        with self._lock:
            self._total_size -= entry.size

        return entry.value

    def clear(self):
        self._data.clear()

    @property
    def count(self):
        return len(self._data)

    @property
    def size(self):
        return self._total_size


class DiskStorage(BaseStorage):

    INFO_FILE = 'data.bin'

    class _Info(object):

        _int_fmt = '<Q'
        _int_length = 8

        def __init__(self, p):
            self._info_file = make_direct_open(p, 'r+b', buffering=0)
            self._total_size = 0
            self._total_count = 0
            self._lock = threading.RLock()

        @classmethod
        def load_file(cls, file_path):
            obj = cls(file_path)
            with obj._lock:
                obj._info_file.seek(0)
                raw = obj._info_file.read(cls._int_length * 2)
                try:
                    raw_size = raw[:cls._int_length]
                    raw_count = raw[cls._int_length:]
                    obj._total_size = struct.unpack(cls._int_fmt, raw_size)[0]
                    obj._total_count = struct.unpack(cls._int_fmt, raw_count)[0]
                except struct.error:
                    raise StandardError(repr(raw))
            return obj

        def _write_num(self, i, n):
            with self._lock:
                self._info_file.seek(i * self._int_length)
                bin_str = struct.pack(self._int_fmt, n)
                self._info_file.write(bin_str)

        def reset(self):
            with self._lock:
                self._total_count = 0
                self._total_size = 0
                self._info_file.flush()

        @property
        def size(self):
            return self._total_size

        @size.setter
        def size(self, v):
            self._write_num(0, v)
            self._total_size = v

        @property
        def count(self):
            return self._total_count

        @count.setter
        def count(self, v):
            self._write_num(1, v)
            self._total_count = v

    def __init__(self, path, path_func=None):
        self._storage_path = os.path.join(path, "cache")
        self._path_func = path_func or (lambda _: _)

        self._file_locks = WeakValueDictionary()
        self._load_info_file()

    def _load_info_file(self):
        info_path = os.path.join(self._storage_path, self.INFO_FILE)
        if os.path.isfile(info_path):
            try:
                self._info = self._Info.load_file(info_path)
            except StandardError:
                self._info = self._Info(info_path)
        else:
            self._info = self._Info(info_path)

    def _get_file_lock(self, path):
        return self._file_locks.setdefault(path, threading.RLock())

    def full_path(self, key):
        return os.path.join(self._storage_path, self._path_func(key))

    def has(self, key):
        return os.path.isfile(self.full_path(key))

    def set(self, key, value):
        full_path = self.full_path(key)
        tmp_path = full_path+".tmp"

        with self._get_file_lock(full_path):
            try:
                with make_direct_open(tmp_path, 'wb') as fp:
                    fp.write(value)
            except IOError:
                return False
            else:
                os.rename(tmp_path, full_path)
                self._info.count += 1
                self._info.size += len(value)
                return True

    def get(self, key, default=None):
        full_path = self.full_path(key)
        if os.path.isfile(full_path):
            with self._get_file_lock(full_path):
                if os.path.isfile(full_path):
                    with open(full_path, 'rb') as fp:
                        return fp.read()
        return default

    def clear(self):
        os.rmdir(self._storage_path)
        self._info.reset()

    def delete(self, key):
        full_path = self.full_path(key)
        if os.path.isfile(full_path):
            with self._get_file_lock(full_path):
                if os.path.isfile(full_path):
                    self._info.size -= os.path.getsize(full_path)
                    self._info.count -= 1
                    os.remove(full_path)

    @property
    def size(self):
        return self._info.size

    @property
    def count(self):
        return self._info.count
