from collections import OrderedDict

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


class ForkTestDemo(unittest.TestCase):
    @fork_test
    def test_fork(self, tk: TestKit):
        v1 = tk.add_forker(FlatForker([1, 2, 3]))
        v2 = tk.add_forker(FlatForker(['a', 'b', 'c']))
        msg1 = self.format("[{}] v1: {}, v2: {}", 'T1', v1, v2)
        msg2 = self.format("[{title}] v1: {v1}, v2: {v2}", title='T1', v1=v1, v2=v2)
        self.assertEqual(msg1, msg2)

    def format(self, msg, *args, **kwargs):
        self.assertIsInstance(msg, str)
        return msg.format(*args, **kwargs)
