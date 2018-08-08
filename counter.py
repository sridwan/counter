from collections import namedtuple

import redis

class KeyExist(Exception):
    pass
class CountLimit(Exception):
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
            raise KeyError
        del self._keys[key]

    def count(self, key, count=1):
        if key not in self._keys:
            raise KeyError
        self._rc.set(key, 0, self._keys[key].duration, nx=True)
        if self._rc.incr(key) > self._keys[key].maxcount:
            raise CountLimit

    def clear(self, key):
        if key not in self._keys:
            raise KeyError
        self._rc.delete(key)
