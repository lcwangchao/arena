import functools
import unittest

import abc
import typing

from arena.core.fork2 import *

_ARG_STUB = object()


class CaseContext:
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


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


class ProxyTestCase:
    def __init__(self, case: unittest.TestCase, tk: TestKit):
        self._case = case
        self._tk = tk

    @property
    def inner_case(self):
        return self._case

    def __getattr__(self, item):
        value = getattr(self._case, item)
        if callable(value):
            def _call(*args, **kwargs):
                return self._tk.execute(value, *args, **kwargs)

            return _call
        return value


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
    def __init__(self, context, actions: typing.Iterator):
        self._context = context
        self._actions = actions
        self._executing = False

    def add_forker(self, forker, *, record=True, record_prefix='tk_'):
        if self._executing:
            raise RuntimeError("Cannot call add_forker in execute")

        next(self._actions)

    def execute(self, forker, *args, **kwargs):
        if self._executing:
            raise RuntimeError("Execute cannot be nested")

        self._executing = True
        try:
            exe = next(self._actions)
            return exe(*args, **kwargs)
        finally:
            self._executing = False


class CaseExecutor:
    def __init__(self, func, actions: typing.Iterable):
        self._func = func
        self._actions = actions

    def run(self, case):
        if not self._actions:
            return

        with CaseContext() as ctx:
            actions = iter(self._actions)
            tk = ExecuteTestKit(ctx, actions)
            case = ProxyTestCase(case, tk)
            self._func(case, tk=tk)
            for _ in actions:
                raise RuntimeError('should not reach here')


class CaseExecutorForker(Forker):
    def __init__(self, case: unittest.TestCase, func):
        self._case = case
        self._func = func

    def do_fork(self, context: ForkContext) -> ForkResult:
        tk = BuilderTestKit()
        case = ProxyTestCase(self._case, tk)
        self._func(case, tk)
        seed = ChainForker(tk.forkers)
        return ReactionForker(seed).do_fork(context).map_value(self._to_case)

    @property
    def name(self):
        return self._case.__name__

    def _to_case(self, actions):
        return CaseExecutor(self._func, actions)


def fork_test(func):
    @functools.wraps(func)
    def _test_func(self):
        forker = CaseExecutorForker(self, func)
        for case in forker:
            case.run(self)

    return _test_func
