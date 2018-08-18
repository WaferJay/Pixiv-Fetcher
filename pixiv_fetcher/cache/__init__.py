# -*- coding: utf-8 -*-
import hashlib
import threading

from pixiv_fetcher.utils.log import get_logger
from .strategy import DoNothingStrategy


def hash_key(key):
    h = hashlib.md5(key)
    return h.digest()


class _CacheRate(object):

    def __init__(self):
        self._hit_count = 0
        self._missing_count = 0
        self._lock = threading.RLock()

    def hit(self):
        with self._lock:
            self._hit_count += 1

    def missing(self):
        with self._lock:
            self._missing_count += 1

    def update(self, is_hit=False):
        if is_hit:
            self.hit()
        else:
            self.missing()

    def reset(self):
        with self._lock:
            self._hit_count = 0
            self._missing_count = 0

    @property
    def hit_count(self):
        return self._hit_count

    @property
    def missing_count(self):
        return self._missing_count

    @property
    def total(self):
        return self._hit_count + self._missing_count

    @property
    def hit_rate(self):
        return self._hit_count / float(self.total)

    @property
    def missing_rate(self):
        return self._missing_count / float(self.total)

    def __str__(self):
        return '[HitCount: %d, HitRate: %.2f, Total: %d]' \
               % (self.hit_count, self.hit_rate, self.total)


class Cache(object):

    def __init__(self, storage, strategy=None, hash_func=None):
        """
        :param storage:
        :type storage: pixiv_fetcher.cache.storage.BaseStorage
        :param strategy:
        :type strategy: pixiv_fetcher.cache.strategy.BaseStrategy
        """
        self._storage = storage
        self._strategy = strategy or DoNothingStrategy()
        self._hash_func = hash_func or hash_key

        self._rate = _CacheRate()
        self._log = get_logger(self)

    def get(self, key, default=None):
        k = self._hash_func(key)
        value = self._storage.get(k, default)
        if value is not None:
            self._rate.hit()
            self._strategy.handle_hit(k, value)
            self._log.debug('缓存命中: %r [%s]', key, self)
        else:
            self._rate.missing()
            self._strategy.handle_missing(k)
            self._log.info('缓存缺失: %r [%s]', key, self)

        return value

    def set(self, key, value):
        key = self._hash_func(key)
        result = self._storage.set(key, value)

        self._strategy.handle_set(key, value)
        self._clean_up_storage()
        return result

    def _clean_up_storage(self):
        self._strategy.remove_keys(self._storage)

    @property
    def count(self):
        return self._storage.count

    @property
    def size(self):
        return self._storage.size

    @property
    def state(self):
        return self._rate

    def __str__(self):
        cls_name = self.__class__.__name__
        strategy_name = self._strategy.__class__.__name__
        storage_name = self._storage.__class__.__name__
        return '<%s@0x%x [state=%s, strategy=%s, storage=%s]>' \
               % (cls_name, id(self), self.state, strategy_name, storage_name)


class CombinationCache(object):

    def __init__(self, *caches):
        self._caches = caches
        self._rate = _CacheRate()

    def get(self, key, default=None):
        for i, cache in enumerate(self._caches):
            value = cache.get(key, default)

            if value:
                self._rate.hit()
                pre_index = i - 1
                if pre_index >= 0:
                    self._caches[pre_index].set(key, value)
                return value

        self._rate.missing()
        return default

    def set(self, key, value):
        for cache in self._caches:
            cache.set(key, value)

    @property
    def size(self):
        return reduce(lambda a, b: a.size + b.size, self._caches)

    @property
    def count(self):
        return reduce(lambda a, b: a.count + b.count, self._caches)

    @property
    def state(self):
        return self._rate
