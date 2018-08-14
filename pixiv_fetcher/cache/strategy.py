# -*- coding: utf-8 -*-
import struct
import threading

from pixiv_fetcher.utils.log import get_logger
from pixiv_fetcher.utils.path import make_direct_open


class BaseStrategy(object):

    def __init__(self, maxsize=None, maxcount=None):
        self.maxsize = maxsize
        self.maxcount = maxcount

    def reset(self, key):
        raise NotImplemented()

    def handle_set(self, key, value):
        raise NotImplemented()

    def handle_hit(self, key, value):
        raise NotImplemented()

    def handle_missing(self, key):
        pass

    def remove_keys(self, storage):
        raise NotImplemented()

    def is_excess(self, storage):
        return storage.size > self.maxsize or storage.count > self.maxcount


class DoNothingStrategy(BaseStrategy):

    def handle_set(self, *args, **kw):
        pass

    record_get = handle_set
    reset = handle_set
    remove_keys = handle_set
    is_excess = handle_set


class FifoMemoryStrategy(BaseStrategy):

    def __init__(self, maxsize=None, maxcount=None):
        super(FifoMemoryStrategy, self).__init__(maxsize, maxcount)
        self._keys = []

    def reset(self, key):
        while key in self._keys:
            try:
                self._keys.remove(key)
            except ValueError:
                pass

    def handle_set(self, key, value):
        if key not in self._keys:
            self._keys.append(key)

    def handle_hit(self, key, value):
        # do nothing
        pass

    def remove_keys(self, storage):
        while self.is_excess(storage) and self._keys:
            storage.delete(self._keys.pop(0))


class LruMemoryStrategy(BaseStrategy):
    """
    Least recently used strategy
    """

    def __init__(self, maxsize=None, maxcount=None):
        super(LruMemoryStrategy, self).__init__(maxsize, maxcount)
        self._keys = []
        self._lock = threading.RLock()
        self._log = get_logger(self)

    def reset(self, key):
        raise NotImplemented()

    def handle_set(self, key, value):
        with self._lock:
            while key in self._keys:
                self._keys.remove(key)

            self._keys.insert(0, key)

    def handle_hit(self, key, value):
        self.handle_set(key, value)

    def remove_keys(self, storage):
        while self.is_excess(storage) and self._keys:
            with self._lock:
                to_rm = self._keys.pop()
                storage.delete(to_rm)
            self._log.debug(u'清理缓存: %r', to_rm)


class DiskRecord(object):

    _int_fmt = '<L'
    _int_size = 4

    def __init__(self, path, key_func=None, recover_func=None, key_len=16):
        self._path = path
        self._fp = make_direct_open(self._path, 'r+b', buffering=0)
        self._key_len = key_len
        self._key_func = key_func or (lambda _: _)
        self._key_recover_func = recover_func or (lambda _: _)
        self._lock = threading.RLock()

    @property
    def _row_length(self):
        return self._key_len + self._int_size

    def _pack_key(self, key):
        k = self._key_func(key)
        less = self._key_len - len(k)
        k += chr(less) * less
        return k

    def _pack(self, key, c):
        k = self._pack_key(key)
        value = struct.pack(self._int_fmt, c)
        return k + value

    def _unpack(self, raw):
        less = ord(raw[self._key_len-1])
        value = struct.unpack(self._int_fmt, raw[self._key_len:])[0]
        key = raw[:self._key_len]

        if key <= less and key.endswith(chr(less) * less):
            key = key[:-less]

        return key, value

    def insert(self, i, key, value):
        curr_row = self._pack(key, value)
        with self._lock:
            num = self.length()
            if i < 0:
                i += num
            elif i > num:
                i = num
            point = self._row_length * i + 5

            self._fp.seek(point)
            next_row = self._fp.read(self._row_length)
            for i in xrange(num-i+1):

                # 读到末尾空字符串位置不变
                if next_row:
                    self._fp.seek(-self._row_length, 1)
                self._fp.write(curr_row)
                curr_row, next_row = next_row, self._fp.read(self._row_length)
            self._write_length(num+1)

    def set(self, key, value):
        with self._lock:
            idx, v = self.search(key)
            if idx != -1:
                p = self._row_length * idx + 5
                self._fp.seek(p+self._key_len)
                self._fp.write(struct.pack(self._int_fmt, value))
            else:
                idx = self.length()
                end = self._row_length * idx + 5
                self._fp.seek(end)
                raw = self._pack(key, value)
                self._fp.write(raw)
                self._write_length(idx + 1)
            return idx

    def set_idx(self, i, key, value):
        with self._lock:
            p = 5 + i * self._row_length
            self._fp.seek(p)
            data = self._pack(key, value)
            self._fp.write(data)

    def swap(self, i1, i2):
        with self._lock:
            num = self.length()
            if i1 >= num:
                raise IndexError("%d (Length: %d)" % (i1, num))
            if i2 >= num:
                raise IndexError("%d (Length: %d)" % (i2, num))

            p1 = 5 + self._row_length * i1
            p2 = 5 + self._row_length * i2

            self._fp.seek(p1)
            data1 = self._fp.read(self._row_length)
            self._fp.seek(p2)
            data2 = self._fp.read(self._row_length)

            self._fp.seek(p1)
            self._fp.write(data2)
            self._fp.seek(p2)
            self._fp.write(data1)

    def search(self, key):
        num = self.length()
        key = self._pack_key(key)
        with self._lock:
            self._fp.seek(5)
            for i in xrange(num):
                raw = self._fp.read(self._row_length)
                k, value = self._unpack(raw)
                if k == key:
                    return i, value
        return -1, None

    def get(self, key):
        _, v = self.search(key)
        return v

    def get_idx(self, idx):
        with self._lock:
            p = 5 + self._row_length * idx
            self._fp.seek(p)
            raw = self._fp.read(self._row_length)
            k, v = self._unpack(raw)
            return self._key_recover_func(k), v

    def has(self, key):
        key = self._pack_key(key)
        with self._lock:
            num = self.length()
            self._fp.seek(5)
            for i in xrange(num):
                k = self._fp.read(self._key_len)
                if k == key:
                    return True
                self._fp.seek(self._int_size, 1)
        return False

    def pop(self, key):
        with self._lock:
            idx, value = self.search(key)
            self.pop_idx(idx)
            return value

    def pop_idx(self, idx=-1):
        with self._lock:
            num = self.length()
            # FIXME: 索引越界
            idx = num + idx if idx < 0 else idx
            self._fp.seek(5 + self._row_length * idx)
            raw_data = self._fp.read(self._row_length)
            k, v = self._unpack(raw_data)
            for i in xrange(num-idx):
                next_row = self._fp.read(self._row_length)
                self._fp.seek(-self._row_length*2, 1)
                self._fp.write(next_row)
                self._fp.seek(self._row_length, 1)
            self._write_length(num-1)
            return self._key_recover_func(k), v

    def length(self):
        with self._lock:
            self._fp.seek(1)
            bs = self._fp.read(4)
            num = struct.unpack(self._int_fmt, bs)[0] if bs else 0
            return num

    def _write_length(self, l):
        with self._lock:
            self._fp.seek(1)
            bs = struct.pack(self._int_fmt, l)
            self._fp.write(bs)

    def close(self):
        if self._fp and not self._fp.closed:
            self._fp.close()

    def flush(self):
        with self._lock:
            self._fp.seek(0)
            self._fp.flush()


class LfuDiskStrategy(BaseStrategy):

    def __init__(self, path, maxsize=None, maxcount=None, key_func=None,
                 recover_func=None, key_len=16):
        super(LfuDiskStrategy, self).__init__(maxsize, maxcount)
        self._record = DiskRecord(path, key_func, recover_func, key_len)
        self._lock = threading.RLock()
        self._log = get_logger(self)

    def remove_keys(self, storage):
        with self._lock:
            while self.is_excess(storage) and self._record.length():
                to_remove = self._record.pop_idx()[0]
                storage.delete(to_remove)
                self._log.debug(u'清理缓存: %r', to_remove)

    def handle_hit(self, key, value):
        with self._lock:
            idx, value = self._record.search(key)
            value += 1
            self._record.set_idx(idx, key, value)
            pre_idx = idx - 1
            while pre_idx >= 0 and self._record.get_idx(pre_idx)[1] <= value:
                self._record.swap(pre_idx+1, pre_idx)
                pre_idx -= 1

    def handle_set(self, key, value):
        with self._lock:
            if not self._record.has(key):
                i = self._record.length() - 1
                while i >= 0 and self._record.get_idx(i)[1] <= 0:
                    i -= 1
                self._record.insert(i+1, key, 0)

    def reset(self, key):
        self._record.pop(key)
