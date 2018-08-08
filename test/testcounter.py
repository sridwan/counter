from unittest import TestCase

from counter import RedisCounter, KeyExist, CountLimit

from config import *


class TestRedisCounter(TestCase):
    def setUp(self):
        rc = RedisCounter(TEST_REDIS)
        rc._rc.delete('some_key')
    def test_sanity(self):
        rc = RedisCounter(TEST_REDIS)
    def test_basic(self):
        rc = RedisCounter(TEST_REDIS)
        rc.add('some_key', 10, 10)
        rc.remove('some_key')
    def test_key_unique(self):
        rc = RedisCounter(TEST_REDIS)
        rc.add('some_key', 10, 10)
        with self.assertRaises(KeyExist):
            rc.add('some_key', 10, 20)
    def test_remove_key_must_exist(self):
        rc = RedisCounter(TEST_REDIS)
        rc.add('some_key', 10, 10)
        rc.remove('some_key')
        with self.assertRaises(KeyError):
            rc.remove('some_key')
    def test_count_key_must_exist(self):
        rc = RedisCounter(TEST_REDIS)
        with self.assertRaises(KeyError):
            rc.count('some_key')
        rc.add('some_key', 10, 10)
        rc.count('some_key')
    def test_maxcount(self):
        rc = RedisCounter(TEST_REDIS)
        rc.add('some_key', 1, 10)
        rc.count('some_key')
        with self.assertRaises(CountLimit):
            rc.count('some_key')
    def test_clear(self):
        rc = RedisCounter(TEST_REDIS)
        rc.add('some_key', 1, 10)
        rc.count('some_key')
        with self.assertRaises(CountLimit):
            rc.count('some_key')
        rc.clear('some_key')
        rc.count('some_key')
    def test_clear_key_must_exist(self):
        rc = RedisCounter(TEST_REDIS)
        with self.assertRaises(KeyError):
            rc.clear('some_key')
