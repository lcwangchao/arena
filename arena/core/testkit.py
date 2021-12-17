import functools
import inspect
import threading
import unittest

import abc
import typing

from arena.core.fork import *

_ARG_STUB = object()
g = threading.local()

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
    def __init__(self, ut):
        self._ut: unittest.TestCase = ut
        self._path = []
        self._defers = []
        self.state = {}

    @abc.abstractmethod
    def fork(self, forker, *, record=True, record_prefix='tk_'):
        pass

    @abc.abstractmethod
    def execute(self, forker, *args, **kwargs):
        pass

    @abc.abstractmethod
    def set_fork_name(self, name, *args, **kwargs):
        pass

    @abc.abstractmethod
    def record_path(self, topic, msg, *args, **kwargs):
        pass

    @abc.abstractmethod
    def defer(self, func):
        pass

    @property
    def ut(self) -> unittest.TestCase:
        return self._ut

    @property
    def path(self):
        return self._path

    def fork_value(self, value, **kwargs):
        return self.fork(SingleValueForker(value), **kwargs)

    def fork_range(self, start, end, **kwargs):
        return self.fork(RangeForker(start, end, **kwargs))

    def fork_enum(self, *enum, **kwargs):
        return self.fork(FlatForker(enum, **kwargs))

    def fork_bool(self, **kwargs):
        return self.fork_enum(True, False, **kwargs)

    def print(self, msg, *args, **kwargs):
        self.execute(self._print, msg, *args, **kwargs)

    def fmt(self, msg, *args, **kwargs):
        return self.execute(self._fmt, msg, *args, **kwargs)

    def _defer(self, func):
        self._defers.append(func)

    def _record_path(self, topic, msg, *args, **kwargs):
        self._path.append((topic, msg.format(*args, **kwargs)))

    @classmethod
    def _print(cls, msg, *args, **kwargs):
        print(msg.format(*args, **kwargs))

    @classmethod
    def _fmt(cls, msg, *args, **kwargs):
        return msg.format(*args, **kwargs)


def testkit() -> TestKit:
    return g.tk


class Execute:
    def __init__(self, func, *, args, kwargs):
        self._func = func
        self._args = args
        self._kwargs = kwargs

    @property
    def args(self):
        return self._args

    @property
    def kwargs(self):
        return self._kwargs

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)


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
    def __init__(self, ut):
        super().__init__(ut)
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
        self.fork(ExecuteForker(func, args=args, kwargs=kwargs), record=False)
        return _ARG_STUB

    def record_path(self, topic, msg, *args, **kwargs):
        return self.execute(self._record_path, topic, msg, *args, **kwargs)

    def defer(self, func):
        return self.execute(self._defer, func)

    @property
    def name(self):
        return self._name

    def set_fork_name(self, name, *args, **kwargs):
        if not isinstance(name, Forker):
            name = SingleValueForker(name)
        forker = ExecuteForker(name.format, args=args, kwargs=kwargs)\
            .map_value(lambda func: func(*func.args, **func.kwargs))
        self._name = forker

    def __enter__(self):
        g.tk = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        g.tk = None


class ExecuteTestKit(TestKit):
    def __init__(self, ut, actions: typing.Iterator):
        super().__init__(ut)
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

    def record_path(self, topic, msg, *args, **kwargs):
        self._record_path(topic, msg, *args, **kwargs)

    def defer(self, func):
        return self._defer(func)

    def set_fork_name(self, name, *args, **kwargs):
        pass

    def __enter__(self):
        g.tk = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        g.tk = None
        for func in self._defers:
            func()


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
        with ExecuteTestKit(case, actions) as tk:
            try:
                case = self._extend_unittest(case) if self._extend_unittest else case
                self._func(case)
                for _ in actions:
                    raise RuntimeError('should not reach here')
            except AssertionError as e:
                e.args = ((self._fork_path_detail_message(tk.path) + e.args[0]),) + e.args[1:]
                raise e

    @classmethod
    def _fork_path_detail_message(cls, path):
        max_topic_len = 0
        for topic, _ in path:
            if topic and len(topic) > max_topic_len:
                max_topic_len = len(topic)

        fmt = '  {:' + str(max_topic_len + 2) + '} {}'
        msgs = [fmt.format('[' + tp + ']', msg) for tp, msg in path]
        return f'\n\nExecute path:\n' + '\n'.join(msgs) + '\n\n'


class CaseExecutorForker(Forker):
    def __init__(self, case: unittest.TestCase, func, *, extend_unittest=None):
        self._case = case
        self._func = func
        self._extend_unittest = extend_unittest

    def do_fork(self, context: ForkContext) -> ForkResult:
        with BuilderTestKit(self._case) as tk:
            case = self._extend_unittest(self._case) if self._extend_unittest else self._case
            self._func(case)
            seed = ChainForker(tk.forkers)
            return ReactionForker(seed).do_fork(context).map(self._to_case(tk))

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


def fork_test(func=None, *, fork_asserts=True):
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


def fork_exec(func=None, *, return_class=None):
    def _wrapper(_func):
        @functools.wraps(_func)
        def _wrap_func(*args, **kwargs):
            tk: TestKit = g.tk
            if not tk:
                return _func(*args, **kwargs)

            if isinstance(tk, ExecuteTestKit) and tk.executing:
                return _func(*args, **kwargs)
            else:
                ret = tk.execute(_func, *args, **kwargs)
                if isinstance(tk, BuilderTestKit) and return_class:
                    ret = return_class(tk)
                return ret

        return _wrap_func

    if func:
        return _wrapper(func)

    return _wrapper

