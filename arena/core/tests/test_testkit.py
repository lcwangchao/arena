import unittest

from arena.core.fork import *
from arena.core.testkit import *


# func to be test
def add_func(a, b):
    return a + b


@fork_exec
def another_add_func(a, b):
    return a + b


class ForkTestDemo(unittest.TestCase):
    def setUp(self) -> None:
        self.print = False

    @fork_test(fork_asserts=False)
    def test_add_func1(self):
        tk = testkit()
        a = tk.fork(RangeForker(0, 10))
        b = tk.fork(RangeForker(0, 10))
        tk.execute(self.check_add_func, a, b)
        tk.set_fork_name("{} + {}", a, b)

    @fork_test(fork_asserts=False)
    def test_add_func2(self):
        tk = testkit()
        a = tk.fork_range(0, 10)
        b = tk.fork_range(0, 10)
        self.check_add_func(a, b)
        tk.set_fork_name("{} + {}", a, b)

    @fork_test
    def test_add_func3(self):
        tk = testkit()
        a = tk.fork_enum(1, 3, 5)
        b = tk.fork_enum(2, 4, 6)
        self.assertEqual(self.add_func(a, b),  a + b)
        self.check_add_func(a, b)
        tk.set_fork_name("{} + {}", a, b)

    @fork_test
    def test_add_func4(self):
        tk = testkit()
        a = tk.fork_enum(1, 3, 5)
        b = tk.fork_enum(2, 4, 6)
        c = tk.fork(a + b)
        self.assertEqual(self.add_func(a, b), c)
        tk.set_fork_name("{} + {}", a, b)

    @fork_test
    def test_add_func5(self):
        tk = testkit()
        a = tk.fork_enum(1, 3, 5)
        b = tk.fork_enum(2, 4, 6)
        self.assertEqual(another_add_func(a, b),  a + b)
        tk.set_fork_name("{} + {}", a, b)

    @fork_exec
    def print(self, msg, *args, **kwargs):
        self.assertIsInstance(msg, str)
        msg = msg.format(msg, *args, **kwargs)
        print(msg)

    @fork_exec
    def check_add_func(self, a, b):
        expected = a + b
        self.assertEqual(add_func(a, b), expected)
        if self.print:
            print(f'{a} + {b} = {expected}')

    @fork_exec
    def add_func(self, a, b):
        return add_func(a, b)
