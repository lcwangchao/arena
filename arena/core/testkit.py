from __future__ import annotations

import functools
import inspect
import threading
import unittest

import abc
import typing

from arena.core.fork import *

__all__ = ['testkit', 'fork_test', 'execute', 'TestKit']

from arena.core.reflect import BUILTIN_OPS

g = threading.local()


def decorate_evaluate_safe_forker(cls):
    def _op_func(name):
        def _func(self, *args, **kwargs):
            op_func = getattr(self._forker, name)
            return cls(op_func(*args, **kwargs))
        _func.__name__ = name
        return _func

    for op in BUILTIN_OPS:
        setattr(cls, op, _op_func(op))
    return cls


@decorate_evaluate_safe_forker
class EvaluateSafeForker(Forker):
    def __init__(self, forker: Forker = None, call_safe=False):
        self._forker = forker
        self._call_safe = call_safe

    @property
    def forker(self):
        return self._forker

    def do_fork(self, *args, **kwargs) -> ForkResult:
        return self._forker.do_fork(*args, **kwargs)

    def concat(self, forker) -> Forker:
        if isinstance(forker, EvaluateSafeForker):
            return EvaluateSafeForker(self._forker.concat(forker._forker))
        return self._forker.concat(forker)

    def transform_result(self, func) -> Forker:
        return self._forker.transform_result(func)

    def record(self, *args, **kwargs):
        key, forker = self._forker.record(*args, **kwargs)
        return key, EvaluateSafeForker(forker)

    def __getattr__(self, item):
        return EvaluateSafeForker(self._forker.__getattr__(item))

    def __call__(self, *args, **kwargs):
        call_safe = self._call_safe
        args = list(args)
        for i, forker in enumerate(args):
            if isinstance(forker, EvaluateSafeForker):
                args[i] = forker._forker
            elif call_safe:
                call_safe = False

        for k, forker in kwargs.items():
            if isinstance(forker, EvaluateSafeForker):
                kwargs[k] = forker
            elif call_safe:
                call_safe = False

        forker = self.call(self._forker, *args, **kwargs)
        return EvaluateSafeForker(forker) if call_safe else forker


def testkit() -> TestKit:
    return g.tk


class TestKit(abc.ABC):
    def __init__(self, ut):
        self._name = None
        self._ut: unittest.TestCase = ut
        self._path = []
        self._defers = []
        self.state = {}

    @abc.abstractmethod
    def pick(self, v, *, key=None, key_prefix=None, skip_safe_check=False):
        pass

    @abc.abstractmethod
    def execute(self, forker, *args, **kwargs):
        pass

    @property
    def ut(self) -> unittest.TestCase:
        return self._ut

    @property
    def path(self):
        return self._path

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        if isinstance(name, Forker) and not isinstance(name, EvaluateSafeForker):
            raise ValueError('name forker must be evaluate safe')
        self._name = name

    def log_path(self, topic, msg):
        self.execute(lambda: self._path.append((topic, msg)))

    def defer(self, func):
        self.execute(lambda: self._defers.append(func))

    def pick_range(self, start, end, **kwargs):
        return self.pick(RangeForker(start, end), **kwargs, skip_safe_check=True)

    def pick_enum(self, *values, **kwargs):
        return self.pick(FlatForker(values), **kwargs, skip_safe_check=True)

    def pick_bool(self, **kwargs):
        return self.pick_enum(False, True, **kwargs)

    def print(self, *msg):
        self.execute(print, *msg)

    def format(self, msg, *args, **kwargs):
        return self.execute(msg.format, _call_safe=True, *args, **kwargs)

    def __enter__(self):
        g.tk = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        g.tk = None
        for func in self._defers:
            func()


class BuilderTestKit(TestKit):
    def __init__(self, ut):
        super().__init__(ut)
        self._forkers = []

    @property
    def forkers(self):
        return self._forkers

    def pick(self, v, *, key=None, key_prefix=None, skip_safe_check=False):
        key_prefix = key_prefix if (key_prefix and not key) else None
        if not key:
            key_prefix = 'tk_v_'

        record_key, forker = self._value_forker(v, key=key, key_prefix=key_prefix, skip_safe_check=skip_safe_check)
        self._forkers.append(forker)
        return EvaluateSafeForker(
            ContextRecordForker(key=record_key)
        )

    def execute(self, func, *args, _call_safe=False, **kwargs):
        if not isinstance(func, Forker):
            func = EvaluateSafeForker(SingleValueForker(func), call_safe=_call_safe)

        return func(*args, **kwargs)

    @classmethod
    def _value_forker(cls, v, *, key, key_prefix, skip_safe_check):
        if not isinstance(v, Forker):
            return SingleValueForker(v).record(key=key, key_prefix=key_prefix)

        if not skip_safe_check and not isinstance(v, EvaluateSafeForker):
            raise ValueError('forker must be evaluate safe')

        def _func(item: ForkItem):
            if isinstance(item.value, Forker):
                return item.value.do_fork(item.context)
            return [item]

        return v.flat_map(_func).record(key=key, key_prefix=key_prefix)


class ExecuteTestKit(TestKit):
    def __init__(self, ut, values: typing.Iterator):
        super().__init__(ut)
        self._values = values
        self._executing = False

    @property
    def executing(self):
        return self._executing

    def pick(self, *_, **__):
        if self._executing:
            raise RuntimeError("pick cannot be nested in execute")
        return next(self._values)

    def execute(self, forker, *args, **kwargs):
        self._executing = True
        try:
            return forker(*args, **kwargs)
        finally:
            self._executing = False


class CaseExecutor:
    def __init__(self, func, *, name, values: typing.Iterable):
        self._name = name
        self._func = func
        self._values = values

    def run(self, case: unittest.TestCase, **params):
        with case.subTest(self._name, **params):
            self._run(case)

    def _run(self, case):
        values = iter(self._values)
        with ExecuteTestKit(case, values) as tk:
            try:
                tk.name = self._name
                self._func(case)
                for _ in values:
                    raise RuntimeError('should not reach here')
            except AssertionError as e:
                path_msg = self._fork_path_detail_message(tk.path)
                if e.args[0]:
                    parts = e.args[0].split('\n', 1)
                    detail = e.args[1] if len(parts) > 1 else ''
                    e.args = ((parts[0] + '\n\n' + path_msg + detail),) + e.args[1:]
                else:
                    e.args = ('None\n' + path_msg,)
                raise e

    @classmethod
    def _fork_path_detail_message(cls, path):
        max_topic_len = 0
        for topic, _ in path:
            if topic and len(topic) > max_topic_len:
                max_topic_len = len(topic)

        fmt = '  {:' + str(max_topic_len + 2) + '} {}'
        msgs = [fmt.format('[' + tp + ']', msg) for tp, msg in path]
        return f'Execute path:\n' + '\n'.join(msgs)


class CaseExecutorForker(Forker):
    class _CaseProxy:
        def __init__(self, case: unittest.TestCase):
            self._case = case

        def __getattr__(self, name):
            value = getattr(self._case, name)
            if inspect.ismethod(value) and (name.startswith('assert') or name.startswith('fail')):
                return execute(value)
            return value

    def __init__(self, case: unittest.TestCase, func):
        self._case = self._CaseProxy(case)
        self._func = func

    def do_fork(self, context: ForkContext) -> ForkResult:
        return ForkResult(self._case_generator(context))

    def _case_generator(self, context: ForkContext):
        with BuilderTestKit(self._case) as tk:
            self._func(self._case)
            seed = ChainForker(tk.forkers)
            index = 0
            for item in ReactionForker(seed).do_fork(context):
                index += 1
                name = f'c_{index}'
                if tk.name:
                    values = list(tk.name.do_fork(item.context).collect_values())
                    if values:
                        name = values[0]
                yield item.context.new_item(CaseExecutor(
                    self._func,
                    name=name,
                    values=item.value
                ))


def fork_test(func=None):
    def _wrapper(_func):
        @functools.wraps(_func)
        def _test_func(self):
            for ut in CaseExecutorForker(self, _func):
                ut.run(self)

        return _test_func

    if func:
        return _wrapper(func)

    return _wrapper


def execute(func=None):
    def _wrapper(_func):
        @functools.wraps(_func)
        def _wrap_func(*args, **kwargs):
            tk = testkit()
            return tk.execute(_func, *args, **kwargs)

        return _wrap_func

    if func:
        return _wrapper(func)

    return _wrapper
