import unittest

from arena.core.fork import *
from arena.core.testkit import *


# func to be test
def add_func(a, b):
    return a + b


class DemoObj:
    def __init__(self, v):
        self.v = v

    @property
    def next_two(self):
        return FlatForker([self.v + 1, self.v + 2])


class ForkTestDemo(unittest.TestCase):
    @fork_test
    def test_add_func(self):
        tk = testkit()
        a = yield tk.pick(RangeForker(0, 10))
        b = yield tk.pick_range(0, 10)
        self.assertEqual(add_func(a, b), a + b)

    @fork_test
    def test_demo_obj(self):
        tk = testkit()
        a = yield tk.pick_enum(DemoObj(1), DemoObj(2))
        b = yield tk.pick(a.next_two)
        self.assertIn(b - a.v, [1, 2])
