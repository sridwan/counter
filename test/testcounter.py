from unittest import TestCase

from counter import RedisCounter, KeyExist, KeyNotFound, CountLimit

from config import *


class TestRedisCounter(TestCase):
    def setUp(self):
        rc = RedisCounter(TEST_REDIS)
        rc.clear('some_key')

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
        with self.assertRaises(KeyNotFound):
            rc.remove('some_key')

    def test_count_key_must_exist(self):
        rc = RedisCounter(TEST_REDIS)
        with self.assertRaises(KeyNotFound):
            rc.count('some_key')
        rc.add('some_key', 10, 10)
        rc.count('some_key')

    def test_count_weight(self):
        rc = RedisCounter(TEST_REDIS)
        rc.add('some_key', 5, 10)
        with self.assertRaises(CountLimit):
            rc.count('some_key', weight=6)

    def test_count_subkey(self):
        rc = RedisCounter(TEST_REDIS)
        rc.add('some_key', 10, 10)
        rc.count('some_key', 'one')
        rc.count('some_key', 'two')
        rc.count('some_key', 'two')
        self.assertEqual(rc.get('some_key', 'one'), 1)
        self.assertEqual(rc.get('some_key', 'two'), 2)

    def test_get_key_must_exist_false(self):
        rc = RedisCounter(TEST_REDIS)
        rc.add('some_key', 10, 10)
        rc.get('some_key')

    def test_get_key_must_exist_true(self):
        rc = RedisCounter(TEST_REDIS)
        with self.assertRaises(KeyNotFound):
            rc.get('some_key', must_exist=True)

    def test_maxcount(self):
        rc = RedisCounter(TEST_REDIS)
        rc.add('some_key', 1, 10)
        rc.count('some_key')
        with self.assertRaises(CountLimit):
            rc.count('some_key')

    def test_reset_key(self):
        rc = RedisCounter(TEST_REDIS)
        rc.add('some_key', 1, 10)
        rc.count('some_key')
        with self.assertRaises(CountLimit):
            rc.count('some_key')
        rc.reset('some_key')
        rc.count('some_key')

    def test_reset_key_must_exist_false(self):
        rc = RedisCounter(TEST_REDIS)
        rc.reset('some_key')

    def test_reset_key_must_exist_true(self):
        rc = RedisCounter(TEST_REDIS)
        with self.assertRaises(KeyNotFound):
            rc.reset('some_key', must_exist=True)

    def test_clear(self):
        rc = RedisCounter(TEST_REDIS)
        rc.add('some_key', 5, 10)
        rc.count('some_key', 'one')
        rc.count('some_key', 'two')
        rc.count('some_key', 'three')
        rc.clear('some_key')
        #~ raise
        self.assertEqual(rc.get('some_key', 'one'), None)
        self.assertEqual(rc.get('some_key', 'two'), None)
        self.assertEqual(rc.get('some_key', 'three'), None)
