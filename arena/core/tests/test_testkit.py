from collections import OrderedDict
import unittest

from ..fork2 import *
from ..testkit import *


class TestArgsForker(unittest.TestCase):
    def test_args(self):
        self.check_args(ArgsForker(), [
            (), {}
        ])

        self.check_args(ArgsForker(args=[1, None, 3]), [
            (1, None, 3), {}
        ])

        self.check_args(ArgsForker(args=[1, FlatForker([10, 11]), FlatForker([3, 4])]), [
            (1, 10, 3), {},
            (1, 10, 4), {},
            (1, 11, 3), {},
            (1, 11, 4), {},
        ])

        self.check_args(ArgsForker(kwargs={'a': 1, 'b': 2, 'c': None}), [
            (), {'a': 1, 'b': 2, 'c': None}
        ])

        kwargs = OrderedDict([('a', 1), ('b', FlatForker([2, 3])), ('c', FlatForker([4, 5]))])
        self.check_args(ArgsForker(kwargs=kwargs), [
            (), {'a': 1, 'b': 2, 'c': 4},
            (), {'a': 1, 'b': 2, 'c': 5},
            (), {'a': 1, 'b': 3, 'c': 4},
            (), {'a': 1, 'b': 3, 'c': 5},
        ])

        self.check_args(ArgsForker(args=[1, FlatForker([10, 11]), 3], kwargs=kwargs), [
            (1, 10, 3), {'a': 1, 'b': 2, 'c': 4},
            (1, 10, 3), {'a': 1, 'b': 2, 'c': 5},
            (1, 10, 3), {'a': 1, 'b': 3, 'c': 4},
            (1, 10, 3), {'a': 1, 'b': 3, 'c': 5},
            (1, 11, 3), {'a': 1, 'b': 2, 'c': 4},
            (1, 11, 3), {'a': 1, 'b': 2, 'c': 5},
            (1, 11, 3), {'a': 1, 'b': 3, 'c': 4},
            (1, 11, 3), {'a': 1, 'b': 3, 'c': 5},
        ])

    def check_args(self, forker, expected):
        result = list(forker)
        self.assertEqual(len(result), len(expected) / 2)
        for i, v in enumerate(result):
            args, kwargs = v
            with self.subTest(i=i):
                self.assertTupleEqual(args, expected[i * 2])
                self.assertDictEqual(kwargs, expected[i * 2 + 1])


# func to be test
def add_func(a, b):
    return a + b


class ForkTestDemo(unittest.TestCase):
    @fork_test
    def test_add_func1(self):
        tk = testkit()
        a = tk.fork_range(0, 10)
        b = tk.fork_range(0, 10)
        tk.execute(self.check_add_func, a, b)

    @fork_test
    def test_add_func2(self):
        tk = testkit()
        a = tk.fork_range(0, 10)
        b = tk.fork_range(0, 10)
        self.check_add_func(a, b)

    @fork_test(fork_asserts=True)
    def test_add_func3(self):
        tk = testkit()
        a = tk.fork_range(0, 10)
        b = tk.fork_range(0, 10)
        self.assertEqual(self.add_func(a, b),  a + b)
        self.check_add_func(a, b)

    @fork_exec
    def print(self, msg, *args, **kwargs):
        self.assertIsInstance(msg, str)
        msg = msg.format(msg, *args, **kwargs)
        print(msg)

    @fork_exec
    def check_add_func(self, a, b):
        self.assertEqual(add_func(a, b), a + b)

    @fork_exec
    def add_func(self, a, b):
        return add_func(a, b)