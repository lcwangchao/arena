from __future__ import annotations

import functools
import threading
import unittest

from arena.core.fork import *

__all__ = ['testkit', 'fork_test', 'TestKit']

g = threading.local()


def testkit() -> TestKit:
    return g.tk


class TestKit:
    def __init__(self, name, *, ut):
        self._name = name
        self._ut = ut
        self._path = []
        self._defers = []
        self._debug = False
        self._state = {}

    @property
    def path(self):
        return self._path

    @property
    def name(self):
        return self._name

    @property
    def ut(self):
        return self._ut

    @property
    def state(self):
        return self._state

    def debug(self, debug=True):
        self._debug = debug

    def log_path(self, topic, msg):
        if self._debug:
            print('    [{}] {}'.format(topic, msg))
        self._path.append((topic, msg))

    def defer(self, func, *args, **kwargs):
        self._defers.append((func, args, kwargs))

    def __enter__(self):
        g.tk = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self._defers.reverse()
            defers = self._defers
            self._defers = []
            for func, args, kwargs in defers:
                func(*args, **kwargs)
        finally:
            g.tk = None

    @classmethod
    def pick(cls, v):
        return v

    @classmethod
    def pick_range(cls, start, end):
        return cls.pick(RangeForker(start, end))

    @classmethod
    def pick_enum(cls, *values):
        return cls.pick(FlatForker(values))

    @classmethod
    def pick_bool(cls):
        return cls.pick_enum(False, True)


class CaseExecutor:
    def __init__(self, func, *, ut: unittest.TestCase, index, debug):
        self._func = func
        self._ut = ut
        self._index = index
        self._name = f'[{index}]'
        self._debug = debug

    def run(self):
        with self._ut.subTest(self._name):
            if self._debug:
                print(f'\n--> {self._name}')
            try:
                yield from self._run()
                if self._debug:
                    print('\n    SUCCEED')
            except Exception:
                if self._debug:
                    print('\n    FAILED')
                raise

    def _run(self):
        with TestKit(self._name, ut=self._ut) as tk:
            try:
                tk.debug(self._debug)
                yield from self._func(self._ut)
                tk.log_path('OK', 'test ok, do some clear works later ...')
            except AssertionError as e:
                raise self._handle_assertion_error(tk, e)
            except Exception as e:
                path_msg = self._fork_path_detail_message(tk.path)
                raise RuntimeError(str(e) + '\n\n' + path_msg)

    def _handle_assertion_error(self, tk, e):
        path_msg = self._fork_path_detail_message(tk.path)
        if e.args[0]:
            parts = e.args[0].split('\n', 1)
            detail = parts[1] if len(parts) > 1 else ''
            e.args = ((parts[0] + '\n\n' + path_msg + '\n' + detail),) + e.args[1:]
        else:
            e.args = ('None\n' + path_msg,)
        return e

    def _fork_path_detail_message(self, path):
        max_topic_len = 0
        for topic, _ in path:
            if topic and len(topic) > max_topic_len:
                max_topic_len = len(topic)

        fmt = '  {:' + str(max_topic_len + 2) + '} {}'
        msgs = [fmt.format('[' + tp + ']', msg) for tp, msg in path]
        return f'{self._name}\n' + '\n'.join(msgs)


def fork_test(func=None, *, debug=False):
    def _wrapper(_func):
        @functools.wraps(_func)
        def _test_func(self):
            if debug:
                print(f'\n*** Start fork test: {_func.__name__} ***')

            index = 0

            def _generate():
                nonlocal index
                index += 1
                executor = CaseExecutor(_func, ut=self, debug=debug, index=index)
                yield from executor.run()

            for _ in GeneratorForker(_generate):
                pass

        return _test_func

    if func:
        return _wrapper(func)

    return _wrapper
