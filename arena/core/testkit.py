import functools
import inspect
import threading
import unittest

import abc
import typing

from arena.core.fork2 import *

_ARG_STUB = object()
_LOCAL = threading.local()


__all__ = ['testkit', 'fork_test', 'fork_exec', 'ArgsForker', 'TestKit']


class ArgsForker(Forker):
    _NONE = object()

    def __init__(self, *, args=None, kwargs=None):
        self._args = [self._value_forker(v) for v in args] if args else []
        self._kwargs = [self._key_value_forker(kv) for kv in kwargs.items()] if kwargs else []

    def do_fork(self, context: ForkContext) -> ForkResult:
        args = self._args_forker()
        kwargs = self._kwargs_forker()
        return ReactionForker(ChainForker([args, kwargs])).do_fork(context)

    def _args_forker(self):
        def _map(args):
            return tuple(arg for arg in args if arg != self._NONE)

        return DefaultValueForker(
            ReactionForker(ChainForker(self._args, initializer=lambda: ())).map_value(_map),
            default={}
        )

    def _kwargs_forker(self):
        def _reduce(state, item):
            k, v = item
            if v != self._NONE:
                state[k] = v
            return {**state, k: v}

        return DefaultValueForker(
            ReactionForker(ChainForker(self._kwargs, initializer=lambda: dict(), reduce=_reduce)),
            default=()
        )

    @classmethod
    def _value_forker(cls, value):
        return DefaultValueForker(value, default=cls._NONE) \
            if isinstance(value, Forker) else SingleValueForker(_ARG_STUB)

    @classmethod
    def _key_value_forker(cls, item):
        key, value = item
        if isinstance(value, Forker):
            return DefaultValueForker(value, default=cls._NONE).map_value(lambda v: (key, v))
        return SingleValueForker((key, _ARG_STUB))


class TestKit(abc.ABC):
    @abc.abstractmethod
    def add_forker(self, forker, *, record=True, record_prefix='tk_'):
        pass

    @abc.abstractmethod
    def execute(self, forker, *args, **kwargs):
        pass

    def fork_range(self, start, end, **kwargs):
        return self.add_forker(RangeForker(start, end, **kwargs))


def testkit() -> TestKit:
    return _LOCAL.tk


class Execute:
    def __init__(self, func, *, args, kwargs):
        self._func = func
        self._args = args
        self._kwargs = kwargs

    def __call__(self, *args, **kwargs):
        func_args = list(self._args)
        for i, arg in enumerate(func_args):
            if arg == _ARG_STUB:
                func_args[i] = args[i]

        func_kwargs = self._kwargs.copy()
        for k, v in func_kwargs.items():
            if v == _ARG_STUB:
                func_kwargs[k] = kwargs[k]

        return self._func(*func_args, **func_kwargs)


class ExecuteForker(Forker[Execute]):
    def __init__(self, func, *, args=None, kwargs=None):
        self._func = func
        self._args = args
        self._kwargs = kwargs

    def do_fork(self, context: ForkContext) -> ForkResult[Execute]:
        def _map(v):
            func, (args, kwargs) = v
            return Execute(func, args=args, kwargs=kwargs)

        seed = ChainForker([
            self._func if isinstance(self._func, Forker) else SingleValueForker(self._func),
            ArgsForker(args=self._args, kwargs=self._kwargs)
        ])
        return ReactionForker(seed).do_fork(context).map_value(_map)


class BuilderTestKit(TestKit):
    def __init__(self):
        self._forkers = []

    @property
    def forkers(self):
        return self._forkers

    def add_forker(self, forker, *, record=True, record_prefix='tk_'):
        record_key = None
        if record:
            record_key, forker = forker.record(key_prefix=record_prefix)
        self._forkers.append(forker)

        if record_key is not None:
            return ContextRecordForker(key=record_key)
        else:
            return None

    def execute(self, func, *args, **kwargs):
        self.add_forker(ExecuteForker(func, args=args, kwargs=kwargs), record=False)
        return _ARG_STUB


class ExecuteTestKit(TestKit):
    def __init__(self, actions: typing.Iterator):
        self._actions = actions
        self._executing = False

    def add_forker(self, forker, *, record=True, record_prefix='tk_'):
        if self._executing:
            raise RuntimeError("Cannot call add_forker in execute")

        return next(self._actions)

    def execute(self, forker, *args, **kwargs):
        if self._executing:
            raise RuntimeError("Execute cannot be nested")

        self._executing = True
        try:
            exe = next(self._actions)
            return exe(*args, **kwargs)
        finally:
            self._executing = False


class CaseProxy:
    def __init__(self, case: unittest.TestCase, *, tk: TestKit, fork_asserts=False):
        self._case = case
        self._tk = tk
        self._fork_asserts = fork_asserts

    def __getattr__(self, name):
        value = getattr(self._case, name)
        if self._is_fork_exec(name, value):
            def _call(*args, **kwargs):
                return self._tk.execute(value, *args, **kwargs)

            return _call
        return value

    def _is_fork_exec(self, name, method):
        if not callable(method):
            return False

        if self._fork_asserts and inspect.ismethod(method) and name.startswith('assert'):
            return True

        return getattr(method, 'fork_exec', False)


class CaseExecutor:
    def __init__(self, func, actions: typing.Iterable, *, extend_unittest=None):
        self._func = func
        self._actions = actions
        self._extend_unittest = extend_unittest

    def run(self, case):
        if not self._actions:
            return

        actions = iter(self._actions)
        _LOCAL.tk = ExecuteTestKit(actions)
        try:
            case = self._extend_unittest(case) if self._extend_unittest else case
            self._func(case)
            for _ in actions:
                raise RuntimeError('should not reach here')
        except AssertionError:
            raise
        finally:
            _LOCAL.tk = None


class CaseExecutorForker(Forker):
    def __init__(self, case: unittest.TestCase, func, *, extend_unittest=None):
        self._case = case
        self._func = func
        self._extend_unittest = extend_unittest

    def do_fork(self, context: ForkContext) -> ForkResult:
        _LOCAL.tk = BuilderTestKit()
        try:
            case = self._extend_unittest(self._case) if self._extend_unittest else self._case
            self._func(case)
            seed = ChainForker(_LOCAL.tk.forkers)
            return ReactionForker(seed).do_fork(context).map_value(self._to_case)
        finally:
            _LOCAL.tk = None

    @property
    def name(self):
        return self._case.__name__

    def _to_case(self, actions):
        return CaseExecutor(self._func, actions, extend_unittest=self._extend_unittest)


def fork_test(func=None, *, fork_asserts=False):
    def _extend_unittest(case):
        return CaseProxy(case, tk=testkit(), fork_asserts=fork_asserts)

    def _wrapper(_func):
        @functools.wraps(_func)
        def _test_func(self):
            forker = CaseExecutorForker(self, _func, extend_unittest=_extend_unittest)
            for case in forker:
                case.run(self)
        return _test_func

    if func:
        return _wrapper(func)

    return _wrapper


def fork_exec(func=None):
    def _wrapper(_func):
        setattr(_func, 'fork_exec', True)
        return _func

    if func:
        return _wrapper(func)

    return _wrapper
