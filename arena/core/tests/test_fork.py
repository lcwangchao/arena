import unittest

from ..fork2 import *


class TestForkContext(unittest.TestCase):
    def test_new_item(self):
        ctx = ForkContext()
        obj = object()
        ele = ctx.new_item(obj)
        self.assertIs(ele.context, ctx)
        self.assertIs(ele.value, obj)

    def test_new_fork_result(self):
        ctx = ForkContext()
        objs = [object(), object(), object()]
        ctx.new_fork_result(objs)
        for i, ele in enumerate(list(ctx.new_fork_result(objs))):
            with self.subTest(i=i):
                self.assertIs(ele.context, ctx)
                self.assertIs(ele.value, objs[i])


class TestForkItem(unittest.TestCase):
    def test_equal(self):
        ctx1 = ForkContext()
        ctx2 = ForkContext()
        item1 = ForkItem(ctx1, "ab")
        item2 = ForkItem(ctx1, "a" + "b")
        item3 = ForkItem(ctx2, "ab")
        item4 = ForkItem(ctx1, "abc")
        self.assertEqual(item1, item2)
        self.assertNotEqual(item1, item3)
        self.assertNotEqual(item1, item4)


class TestForker(unittest.TestCase):
    def test_iter(self):
        ctx = ForkContext()
        values = ["a", "b", "c"]

        class _MockForker(Forker[str]):
            def do_fork(self, *, context: ForkContext) -> ForkResult[str]:
                return ctx.new_fork_result(values)

        forker = _MockForker()
        self.assertListEqual(list(forker), values)
        for i, ele in enumerate(forker.do_fork(context=ctx)):
            with self.subTest(i=i):
                self.assertEqual(ele, ctx.new_item(values[i]))

    def test_flat_forker(self):
        ctx = ForkContext()
        values = ["a", "b", "c"]
        forker = FlatForker(values)
        self.assertListEqual(list(forker.do_fork(context=ctx)), list(ctx.new_fork_result(values)))

    def test_chained_forker(self):
        ctx = ForkContext()
        values1 = ["a", "b", "c"]
        values2 = ["d", "e", "f"]
        forker = ChainedForker([
            FlatForker(values1),
            FlatForker(values2),
        ])
        self.assertListEqual(
            list(forker.do_fork(context=ctx)),
            list(ctx.new_fork_result([*values1, *values2])),
        )

    def test_transform(self):
        ctx = ForkContext()
        forker = FlatForker([1, 2, 3]).transform_result(lambda r: r.map_value(lambda v: v + 1))
        self.assertListEqual(
            list(forker.do_fork(context=ctx)),
            list(ctx.new_fork_result([2, 3, 4])),
        )

        forker = forker.transform_result(lambda r: r.filter_value(lambda v: v >= 3))
        self.assertListEqual(
            list(forker.do_fork(context=ctx)),
            list(ctx.new_fork_result([3, 4])),
        )

        forker = TransformForker(FlatForker([1, 2, 3]), [
            lambda r: r.map_value(lambda v: v + 1),
            lambda r: r.filter_value(lambda v: v >= 3)
        ])
        self.assertListEqual(
            list(forker.do_fork(context=ctx)),
            list(ctx.new_fork_result([3, 4])),
        )

    def test_map(self):
        ctx = ForkContext()
        ctx2 = ForkContext()
        forker = FlatForker([1, 2, 3]).map(
            lambda item: ctx2.new_item(item.value+1) if item.value < 2 else item.context.new_item(item.value + 2)
        )
        result = list(forker.do_fork(context=ctx))
        self.assertIs(result[0].context, ctx2)
        self.assertIs(result[1].context, ctx)
        self.assertIs(result[2].context, ctx)
        self.assertListEqual(list(forker.do_fork(context=ctx).to_values()), [2, 4, 5])

    def test_map_values(self):
        ctx = ForkContext()
        forker = FlatForker([1, 2, 3]).map_value(lambda v: v + 1)
        self.assertListEqual(
            list(forker.do_fork(context=ctx)),
            list(ctx.new_fork_result([2, 3, 4])),
        )

    def test_filter(self):
        ctx = ForkContext()
        forker = FlatForker([1, 2, 3]).filter(lambda item: item.value > 1)
        self.assertListEqual(
            list(forker.do_fork(context=ctx)),
            list(ctx.new_fork_result([2, 3])),
        )

    def test_filter_value(self):
        ctx = ForkContext()
        forker = FlatForker([1, 2, 3]).filter_value(lambda value: value > 1)
        self.assertListEqual(
            list(forker.do_fork(context=ctx)),
            list(ctx.new_fork_result([2, 3])),
        )


class TestAssemblyForker(unittest.TestCase):
    def test_simple_assembly(self):
        ctx = ForkContext()
        forker = AssemblyForker(
            children=[
                FlatForker([1, 2]),
                FlatForker([4, 5]),
                FlatForker([6]),
                FlatForker([7, 8])
            ]
        )

        expected = [
            (1, 4, 6, 7),
            (1, 4, 6, 8),
            (1, 5, 6, 7),
            (1, 5, 6, 8),
            (2, 4, 6, 7),
            (2, 4, 6, 8),
            (2, 5, 6, 7),
            (2, 5, 6, 8),
        ]

        items = list(forker.do_fork(context=ctx))
        self.assertEqual(len(items), len(expected))
        for i, item in enumerate(items):
            self.assertIs(item.context, ctx)
            self.assertTupleEqual(item.value, expected[i])

    def test_conditional_assembly(self):
        ctx = ForkContext()
        forker = AssemblyForker(
            children=[
                FlatForker([1, 2]),
                lambda b: FlatForker([3, 4]) if b.values[0] == 1 else FlatForker([5, 6, 7]),
                FlatForker([8])
            ]
        )

        expected = [
            (1, 3, 8),
            (1, 4, 8),
            (2, 5, 8),
            (2, 6, 8),
            (2, 7, 8),
        ]

        items = list(forker.do_fork(context=ctx))
        self.assertEqual(len(items), len(expected))
        for i, item in enumerate(items):
            self.assertIs(item.context, ctx)
            self.assertTupleEqual(item.value, expected[i])

    def test_update_context(self):
        class _MockForker(Forker[int]):
            def __init__(self, values):
                self.values = values

            def do_fork(self, *, context: ForkContext) -> ForkResult[T]:
                fork_items = []
                for v in self.values:
                    new_context = context.set_var(str(v), context.get_var(str(v), default=0) + 1)
                    fork_items.append(ForkItem(new_context, v))
                return ForkResult(fork_items)

            def __str__(self):
                return 'MockForker'

        ctx = ForkContext()
        forker = AssemblyForker(
            children=[
                _MockForker([1, 2]),
                _MockForker([3, 4, 2]),
            ]
        )

        expected = [
            (1, 3),
            (1, 4),
            (1, 2),
            (2, 3),
            (2, 4),
            (2, 2),
        ]

        items = list(forker.do_fork(context=ctx))
        self.assertEqual(len(items), len(expected))
        self.assertIsNone(ctx.get_var('1'))
        self.assertIsNone(ctx.get_var('2'))
        self.assertIsNone(ctx.get_var('3'))
        self.assertIsNone(ctx.get_var('4'))
        for i, item in enumerate(items):
            self.assertTupleEqual(item.value, expected[i])
            expected_dict = {}
            for value in item.value:
                expected_dict[str(value)] = expected_dict.get(str(value), 0) + 1
            self.assertDictEqual(item.context.vars, expected_dict)
