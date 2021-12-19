import unittest

from arena.core.fork import *
from arena.core.testkit import *


# func to be test
def add_func(a, b):
    return a + b


@execute
def another_add_func(a, b):
    return a + b


class DemoObj:
    def __init__(self, v):
        self.v = v

    @property
    def next_two(self):
        return FlatForker([self.v + 1, self.v + 2])


class ForkTestDemo(unittest.TestCase):
    def setUp(self) -> None:
        self.print = False

    @fork_test
    def test_add_func1(self):
        tk = testkit()
        a = tk.pick(RangeForker(0, 10), skip_safe_check=True)
        b = tk.pick(RangeForker(0, 10), skip_safe_check=True)
        tk.execute(self.check_add_func, a, b)
        tk.set_name(tk.format("{} + {}", a, b))

    @fork_test
    def test_add_func2(self):
        tk = testkit()
        a = tk.pick_range(0, 10)
        b = tk.pick_range(0, 10)
        self.check_add_func(a, b)
        tk.set_name(tk.format("{} + {}", a, b))

    @fork_test
    def test_add_func3(self):
        tk = testkit()
        a = tk.pick_enum(1, 3, 5)
        b = tk.pick_enum(2, 4, 6)
        self.assertEqual(self.add_func(a, b),  a + b)
        self.check_add_func(a, b)
        tk.set_name(tk.format("{} + {}", a, b))

    @fork_test
    def test_add_func4(self):
        tk = testkit()
        a = tk.pick_enum(1, 3, 5)
        b = tk.pick_enum(2, 4, 6)
        c = tk.pick(a + b)
        self.assertEqual(self.add_func(a, b), c)
        tk.set_name(tk.format("{} + {}", a, b))

    @fork_test
    def test_add_func5(self):
        tk = testkit()
        a = tk.pick_enum(1, 3, 5)
        b = tk.pick_enum(2, 4, 6)
        self.assertEqual(another_add_func(a, b),  a + b)
        tk.set_name(tk.format("{} + {}", a, b))

    @fork_test
    def test_if_1(self):
        tk = testkit()
        a = tk.pick_enum(1, 2, 3, 4, 5)
        b = tk.if_(a == 1) \
            .then(lambda: tk.pick("== 1")) \
            .elif_then(a <= 3, lambda: tk.pick("<= 3")) \
            .elif_then(a <= 4, lambda: tk.pick("<= 4")) \
            .done()

        @execute
        def _check():
            if a == 1:
                self.assertEqual(b, "== 1")
            if a == 2:
                self.assertEqual(b, "<= 3")
            if a == 3:
                self.assertEqual(b, "<= 3")
            if a == 4:
                self.assertEqual(b, "<= 4")
            if a == 5:
                self.assertEqual(b, None)

        _check()

    @fork_test
    def test_if_2(self):
        tk = testkit()
        a = tk.pick_enum(1, 2, 3, 4, 5)
        b = tk.if_(a == 1) \
            .then(lambda: tk.pick("== 1")) \
            .elif_then(a <= 3, lambda: tk.pick("<= 3")) \
            .elif_then(a <= 4, lambda: tk.pick("<= 4")) \
            .else_then(lambda: tk.pick("else")) \
            .done()

        @execute
        def _check():
            if a == 1:
                self.assertEqual(b, "== 1")
            if a == 2:
                self.assertEqual(b, "<= 3")
            if a == 3:
                self.assertEqual(b, "<= 3")
            if a == 4:
                self.assertEqual(b, "<= 4")
            if a == 5:
                self.assertEqual(b, "else")

        _check()

    @fork_test
    def test_if_2(self):
        tk = testkit()
        a = tk.pick_enum(1, 2, 3, 4, 5)
        b = tk.if_(a == 1) \
            .then(lambda: tk.pick("== 1")) \
            .elif_then(a <= 3, lambda: tk.pick("<= 3")) \
            .elif_then(a <= 4, lambda: tk.pick("<= 4")) \
            .else_then(lambda: tk.pick("else")) \
            .done()

        tk.if_(a == 1) \
            .then(self.assertEqual, b, "== 1") \
            .elif_then(a == 3, self.assertEqual, b, "<= 3") \
            .elif_then(a == 4, self.assertEqual, b, "<= 4") \
            .elif_then(a == 5, self.assertEqual, b, "else")

    @fork_test
    def test_demo_obj(self):
        tk = testkit()
        a = tk.pick_enum(DemoObj(1), DemoObj(2))
        b = tk.pick(a.next_two)
        self.assertIn(b - a.v, [1, 2])

    @execute
    def check_add_func(self, a, b):
        expected = a + b
        self.assertEqual(add_func(a, b), expected)
        if self.print:
            print(f'{a} + {b} = {expected}')

    @execute
    def add_func(self, a, b):
        return add_func(a, b)
