from collections import namedtuple

import redis

class CounterException(Exception):
    pass
class KeyExist(CounterException):
    pass
class KeyNotFound(CounterException):
    pass
class CountLimit(CounterException):
    pass

class RedisCounter(object):
    Key = namedtuple('Key', 'key,maxcount,duration')

    def __init__(self, uri):
        self._keys = {}
        self._rc = redis.from_url(uri)

    def add(self, key, maxcount, duration):
        if key in self._keys:
            raise KeyExist
        self._keys[key] = self.Key(key, maxcount, duration)

    def remove(self, key):
        if key not in self._keys:
            raise KeyNotFound
        del self._keys[key]

    def count(self, key, sk='', weight=1):
        if key not in self._keys:
            raise KeyNotFound
        k = self._key(key, sk)
        self._rc.set(k, 0, self._keys[key].duration, nx=True)
        if self._rc.incr(k, weight) > self._keys[key].maxcount:
            raise CountLimit

    def reset(self, key, sk='', must_exist=False):
        if must_exist and key not in self._keys:
            raise KeyNotFound
        k = self._key(key, sk)
        self._rc.delete(k)

    def get(self, key, sk='', must_exist=False):
        if must_exist and key not in self._keys:
            raise KeyNotFound
        k = self._key(key, sk)
        r = self._rc.get(k)
        return int(r) if r else r

    def clear(self, key):
        k = self._key(key, '*')
        for i in self._rc.scan_iter(k):
            self._rc.delete(i)

    def _key(self, key, sk=''):
        return '{}_{}'.format(key, sk)
