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
            if isinstance(value, Forker) else SingleValueForker(value)

    @classmethod
    def _key_value_forker(cls, item):
        key, value = item
        if isinstance(value, Forker):
            return DefaultValueForker(value, default=cls._NONE).map_value(lambda v: (key, v))
        return SingleValueForker((key, value))


class TestKit(abc.ABC):
    @abc.abstractmethod
    def fork(self, forker, *, record=True, record_prefix='tk_'):
        pass

    @abc.abstractmethod
    def execute(self, forker, *args, **kwargs):
        pass

    @abc.abstractmethod
    def set_name(self, name, *args, **kwargs):
        pass

    def fork_value(self, value, **kwargs):
        return self.fork(SingleValueForker(value), **kwargs)

    def fork_range(self, start, end, **kwargs):
        return self.fork(RangeForker(start, end, **kwargs))

    def fork_enum(self, *enum, **kwargs):
        return self.fork(FlatForker(enum, **kwargs))

    def fork_bool(self, **kwargs):
        return self.fork_enum(True, False, **kwargs)


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
        self._name = None
        self._forkers = []

    @property
    def forkers(self):
        return self._forkers

    def fork(self, forker, *, record=True, record_prefix='tk_'):
        record_key = None
        if record:
            record_key, forker = forker.record(key_prefix=record_prefix)
        self._forkers.append(forker)

        if record_key is not None:
            return ContextRecordForker(key=record_key)
        else:
            return None

    def execute(self, func, *args, **kwargs):
        args = tuple(arg if isinstance(arg, Forker) else _ARG_STUB for arg in args)
        kwargs = {k: (v if isinstance(v, Forker) else _ARG_STUB) for k, v in kwargs.items()}
        self.fork(ExecuteForker(func, args=args, kwargs=kwargs), record=False)
        return _ARG_STUB

    @property
    def name(self):
        return self._name

    def set_name(self, name, *args, **kwargs):
        if not isinstance(name, Forker):
            name = SingleValueForker(name)
        forker = ExecuteForker(name.format, args=args, kwargs=kwargs).map_value(lambda func: func())
        self._name = forker


class ExecuteTestKit(TestKit):
    def __init__(self, actions: typing.Iterator):
        self._actions = actions
        self._executing = False

    @property
    def executing(self):
        return self._executing

    def fork(self, forker, *, record=True, record_prefix='tk_'):
        if self._executing:
            raise RuntimeError("fork cannot be nested in execute")

        return next(self._actions)

    def execute(self, forker, *args, **kwargs):
        if self._executing:
            raise RuntimeError("execute cannot be nested")

        self._executing = True
        try:
            exe = next(self._actions)
            return exe(*args, **kwargs)
        finally:
            self._executing = False

    def set_name(self, name, *args, **kwargs):
        pass


class CaseProxy:
    def __init__(self, case: unittest.TestCase, *, tk: TestKit, fork_asserts=False):
        self._case = case
        self._tk = tk
        self._fork_asserts = fork_asserts

    def __getattr__(self, name):
        value = getattr(self._case, name)
        if self._fork_asserts and inspect.ismethod(value) and name.startswith('assert'):
            return fork_exec(value)

        return value


class CaseExecutor:
    def __init__(self, func, *, name, actions: typing.Iterable, extend_unittest=None):
        self.name = name
        self._func = func
        self._actions = actions
        self._extend_unittest = extend_unittest

    def run(self, case: unittest.TestCase, **params):
        with case.subTest(self.name, **params):
            self._run(case)

    def _run(self, case):
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
        tk = BuilderTestKit()
        _LOCAL.tk = tk
        try:
            case = self._extend_unittest(self._case) if self._extend_unittest else self._case
            self._func(case)
            seed = ChainForker(tk.forkers)
            return ReactionForker(seed).do_fork(context).map(self._to_case(tk))
        finally:
            _LOCAL.tk = None

    @property
    def name(self):
        return self._case.__name__

    def _to_case(self, tk: BuilderTestKit):
        def _map(item):
            name = None
            if tk.name:
                for name_item in tk.name.do_fork(item.context):
                    name = name_item.value

            return item.context.new_item(CaseExecutor(
                self._func, actions=item.value, name=name, extend_unittest=self._extend_unittest
            ))

        return _map


def fork_test(func=None, *, fork_asserts=False):
    def _extend_unittest(case):
        return CaseProxy(case, tk=testkit(), fork_asserts=fork_asserts)

    def _wrapper(_func):
        @functools.wraps(_func)
        def _test_func(self):
            forker = CaseExecutorForker(self, _func, extend_unittest=_extend_unittest)
            i = 0
            for case in forker:
                i += 1
                if not case.name:
                    case.name = f"fork_{i}"
                case.run(self)

        return _test_func

    if func:
        return _wrapper(func)

    return _wrapper


def fork_exec(func=None):
    def _wrapper(_func):
        @functools.wraps(_func)
        def _wrap_func(*args, **kwargs):
            tk: TestKit = _LOCAL.tk
            if not tk:
                return _func(*args, **kwargs)

            if isinstance(tk, ExecuteTestKit) and tk.executing:
                return _func(*args, **kwargs)
            else:
                return tk.execute(_func, *args, **kwargs)

        return _wrap_func

    if func:
        return _wrapper(func)

    return _wrapper

