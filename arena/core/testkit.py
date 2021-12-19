from __future__ import annotations

import itertools

import collections
import functools
import inspect
import threading
import unittest

import abc
import typing

from arena.core.fork import *
from arena.core.reflect import BUILTIN_OPS

__all__ = ['testkit', 'fork_test', 'execute', 'TestKit']

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


class IfStatement(abc.ABC):
    @abc.abstractmethod
    def then(self, func, *args, **kwargs) -> IfStatement:
        pass

    @abc.abstractmethod
    def elif_then(self, cond, func, *args, **kwargs) -> IfStatement:
        pass

    @abc.abstractmethod
    def else_then(self, func, *args, **kwargs) -> IfStatement:
        pass

    @abc.abstractmethod
    def done(self):
        pass

    def then_return(self, ret) -> IfStatement:
        return self.then(lambda: ret)

    def elif_return(self, cond, ret) -> IfStatement:
        return self.elif_then(cond, lambda: ret)

    def else_return(self, ret) -> IfStatement:
        return self.else_then(lambda: ret)


class TestKit(abc.ABC):
    def __init__(self, ut):
        self._name = None
        self._ut: unittest.TestCase = ut
        self._path = []
        self._defers = []
        self._debug = False
        self.state = {}

    @abc.abstractmethod
    def pick(self, v, *, key=None, key_prefix=None, skip_safe_check=False):
        pass

    @abc.abstractmethod
    def execute(self, forker, *args, **kwargs):
        pass

    @abc.abstractmethod
    def if_(self, cond) -> IfStatement:
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

    def debug(self, debug=True):
        self._debug = debug

    def set_name(self, name):
        if isinstance(name, Forker) and not isinstance(name, EvaluateSafeForker):
            raise ValueError('name forker must be evaluate safe')
        self._name = name

    def log_path(self, topic, msg):
        def _func():
            if self._debug:
                print('    [{}] {}'.format(topic, msg))
            self._path.append((topic, msg))
        self.execute(_func)

    def defer(self, func, *args, **kwargs):
        self.execute(lambda: self._defers.append((func, args, kwargs)))

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

    def execute_or_not(self, flag: bool, func, *args, **kwargs):
        def _exec():
            if flag:
                return func(*args, **kwargs)
        self.execute(_exec)

    def __enter__(self):
        g.tk = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        g.tk = None
        self._defers.reverse()
        defers = self._defers
        self._defers = []
        for func, args, kwargs in defers:
            func(*args, **kwargs)


class BuilderTestKit(TestKit):
    def __init__(self, ut):
        super().__init__(ut)
        self._forkers = []
        self._sub_stmt_stack = []

    @property
    def forkers(self):
        return self._forkers

    def pick(self, v, *, key=None, key_prefix=None, skip_safe_check=False):
        if self._sub_stmt_stack and not self._sub_stmt_stack[-1].executing:
            raise ValueError('The previous stmt is not terminated')

        key_prefix = key_prefix if (key_prefix and not key) else None
        if not key:
            key_prefix = 'tk_v_'

        record_key, forker = self._value_forker(v, key=key, key_prefix=key_prefix, skip_safe_check=skip_safe_check)

        if self._sub_stmt_stack:
            stmt = self._sub_stmt_stack[-1]
            stmt.add_value_forker(forker)
        else:
            self._forkers.append(forker)

        return EvaluateSafeForker(
            ContextRecordForker(key=record_key)
        )

    def execute(self, func, *args, _call_safe=False, **kwargs):
        if self._sub_stmt_stack and not self._sub_stmt_stack[-1].executing:
            raise ValueError('The previous stmt is not terminated')

        if not isinstance(func, Forker):
            func = EvaluateSafeForker(SingleValueForker(func), call_safe=_call_safe)

        return func(*args, **kwargs)

    def if_(self, cond):
        if self._sub_stmt_stack and not self._sub_stmt_stack[-1].executing:
            raise ValueError('The previous stmt is not terminated')

        def _done_func(forker):
            self._forkers.append(forker)
            self._sub_stmt_stack = self._sub_stmt_stack[:-1]

        stmt = BuilderIfStatement(self, cond, _done_func)
        self._sub_stmt_stack.append(stmt)
        return stmt

    def build_values_forker(self, case, func):
        func(case)
        if self._sub_stmt_stack:
            raise ValueError('Some sub statement not finished')

        return ChainForker(self._forkers).reaction().map_value(Values)

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


class BuilderIfStatement(IfStatement):
    def __init__(self, tk: BuilderTestKit, cond, done_func):
        if isinstance(cond, Forker) and not isinstance(cond, EvaluateSafeForker):
            raise ValueError('condition forker must be evaluate safe')

        self._tk = tk
        self._if_cond = cond
        self._done_func = done_func

        self._value_builder = IfConditionForker.builder()
        self._return_builder = IfConditionForker.builder()
        self._add_value_forker = None
        self._executing = False
        self._has_else_then = False

    def then(self, func, *args, **kwargs):
        ret, v = self._get_then(func, *args, **kwargs)
        self._return_builder.if_then(self._if_cond, ret)
        self._value_builder.if_then(self._if_cond, v)
        return self

    def elif_then(self, cond, func, *args, **kwargs):
        if not isinstance(cond, EvaluateSafeForker):
            raise ValueError('condition forker must be evaluate safe')

        ret, v = self._get_then(func, *args, **kwargs)
        self._return_builder.elif_then(cond, ret)
        self._value_builder.elif_then(cond, v)
        return self

    def else_then(self, func, *args, **kwargs):
        ret, v = self._get_then(func, *args, **kwargs)
        self._return_builder.else_then(ret)
        self._value_builder.else_then(v)
        self._has_else_then = True
        return self

    def done(self):
        if not self._has_else_then:
            self.else_then(lambda: None)
        self._done_func(self._value_builder.build())
        return self._return_builder.build()

    def add_value_forker(self, forker):
        self._add_value_forker(forker)

    @property
    def executing(self):
        return self._executing

    def _get_then(self, func, *args, **kwargs):
        value_forkers = []
        self._add_value_forker = value_forkers.append
        self._executing = True
        try:
            ret = func(*args, **kwargs)
            if value_forkers:
                return ret, ChainForker(value_forkers).reaction().map_value(Values)
            else:
                return ret, SingleValueForker(Values())
        finally:
            self._add_value_forker = None
            self._executing = False


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

    def if_(self, cond):
        return ExecuteIfStatement(cond)


class ExecuteIfStatement(IfStatement):
    def __init__(self, cond):
        self._cond = cond
        self._then_called = False
        self._ret = None
        self._has_ret = False

    def then(self, func, *args, **kwargs):
        if self._then_called:
            raise ValueError('Cannot call then twice')
        self._then_called = True
        if self._cond:
            self._ret = func(*args, **kwargs)
            self._has_ret = True
        return self

    def elif_then(self, cond, func, *args, **kwargs):
        if not self._then_called:
            raise ValueError('then must be called before elif_then')

        if not self._has_ret and cond:
            self._ret = func(*args, **kwargs)
            self._has_ret = True
        return self

    def else_then(self, func, *args, **kwargs):
        if not self._then_called:
            raise ValueError('then must be called before elif_then')

        if not self._has_ret:
            self._ret = func(*args, **kwargs)
        return self

    def done(self):
        self._has_ret = True
        return self._ret


class Values(collections.Iterable):
    def __init__(self, values=None):
        self._values = list(itertools.chain(
            *[list(v) if isinstance(v, Values) else [v] for v in (values or [])]
        ))

    def __iter__(self):
        return iter(self._values)


class CaseExecutor:
    def __init__(self, func, *, name, values: typing.Iterable):
        self._name = name
        self._func = func
        self._values = values

    def run(self, case: unittest.TestCase, *, debug, **params):
        with case.subTest(self._name, **params):
            if debug:
                print(f'\n-->  Forked: {self._name}')
            try:
                self._run(case, debug=debug)
                if debug:
                    print('\n    SUCCEED')
            except Exception:
                if debug:
                    print('\n    FAILED')
                raise

    def _run(self, case, *, debug):
        values = iter(self._values)
        with ExecuteTestKit(case, values) as tk:
            try:
                tk.debug(debug)
                tk.set_name(self._name)
                self._func(case)
                for _ in values:
                    raise RuntimeError('should not reach here')
            except AssertionError as e:
                raise self._handle_error(tk, e)

    def _handle_error(self, tk, e):
        path_msg = self._fork_path_detail_message(tk.path)
        if e.args[0]:
            parts = e.args[0].split('\n', 1)
            detail = parts[1] if len(parts) > 1 else ''
            e.args = ((parts[0] + '\n\n' + path_msg + detail),) + e.args[1:]
        else:
            e.args = ('None\n' + path_msg,)
        return e

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
            index = 0
            for item in tk.build_values_forker(self._case, self._func).do_fork(context):
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


def fork_test(func=None, *, debug=False):
    def _wrapper(_func):
        @functools.wraps(_func)
        def _test_func(self):
            if debug:
                print(f'\n*** Start fork test: {_func.__name__} ***')

            for ut in CaseExecutorForker(self, _func):
                ut.run(self, debug=debug)

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
