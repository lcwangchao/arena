from __future__ import annotations

import threading
from collections import Iterator, Iterable
from dataclasses import dataclass

import abc
import itertools
from typing import TypeVar, Generic, Dict, Callable, Any

from arena.core.reflect import BUILTIN_OPS

T = TypeVar('T')

__all__ = ['ForkContext', 'ForkItem', 'ForkResult', 'Forker', 'TransformForker', 'FlatForker', 'SingleValueForker',
           'RangeForker',
           'ConcatForker',
           'DefaultValueForker',
           'ReactionForker',
           'ContextRecordForker',
           'ChainForker']


class ForkContext:
    def __init__(self, *, variables: Dict = None):
        self._vars = variables.copy() if variables else {}

    @property
    def vars(self):
        return self._vars

    def set_var(self, name, value) -> ForkContext:
        new_vars = self._vars.copy()
        new_vars[name] = value
        return ForkContext(variables=new_vars)

    def get_var(self, name: str, *, default=None):
        if name in self._vars:
            return self._vars[name]
        return default

    def has_var(self, name: str):
        return name in self._vars

    def new_item(self, value: T) -> ForkItem[T]:
        return ForkItem.new(self, value)

    def new_fork_result(self, values: Iterable[T]) -> ForkResult[T]:
        items = map(lambda value: self.new_item(value), values)
        return ForkResult(items)


@dataclass(frozen=True)
class ForkItem(Generic[T]):
    context: ForkContext
    value: T

    @classmethod
    def new(cls, context, value: T) -> ForkItem[T]:
        return ForkItem(context=context, value=value)

    def set_var(self, *args, **kwargs) -> ForkItem[T]:
        return self.context.set_var(*args, **kwargs).new_item(self.value)

    def get_var(self, *args, **kwargs):
        return self.context.get_var(*args, **kwargs)

    def __eq__(self, o: ForkItem[T]) -> bool:
        if o is None:
            return False
        return self.context == o.context and self.value == o.value


class ForkResult(Generic[T], Iterator[ForkItem[T]]):
    def __init__(self, items: Iterable[ForkItem[T]] = None):
        self._iter = iter(items or tuple())

    def __next__(self) -> ForkItem[T]:
        return next(self._iter)

    def collect(self):
        return list(self)

    def collect_values(self):
        return list(self.to_values_iter())

    def to_values_iter(self):
        return map(lambda item: item.value, self)

    def map(self, func):
        return ForkResult(map(func, self._iter))

    def map_value(self, func):
        return ForkResult(map(
            lambda item: item.context.new_item(func(item.value)),
            self._iter
        ))

    def foreach(self, func):
        def _foreach(item):
            func(item)
            return item

        return ForkResult(map(
            _foreach,
            self._iter
        ))

    def foreach_value(self, func):
        def _foreach(item):
            func(item.value)
            return item

        return ForkResult(map(
            _foreach,
            self._iter
        ))

    def filter(self, func):
        return ForkResult(filter(func, self._iter))

    def filter_value(self, func):
        return ForkResult(filter(
            lambda item: func(item.value),
            self._iter
        ))


_RECORD_INDEX = 0
_LOCK = threading.Lock()


def decorate_forker(cls):
    def _op_func(name):
        def _func(self, *args, **kwargs):
            return self.map_value(lambda v: getattr(v, name))(*args, **kwargs)
        _func.__name__ = name
        return _func

    for op in BUILTIN_OPS:
        setattr(cls, op, _op_func(op))
    return cls


@decorate_forker
class Forker(abc.ABC, Generic[T], Iterable[T]):
    @abc.abstractmethod
    def do_fork(self, context: ForkContext) -> ForkResult[T]:
        pass

    def __iter__(self) -> Iterator[T]:
        it = self.do_fork(context=ForkContext())
        return map(lambda item: item.value, it)

    def concat(self, forker) -> Forker[T]:
        if isinstance(self, ConcatForker):
            return self.add_forker(forker)
        return ConcatForker((self, forker))

    def transform_result(self, func) -> Forker[T]:
        if isinstance(self, TransformForker):
            return self.add_func(func)
        return TransformForker(self, [func])

    def map(self, func):
        return self.transform_result(lambda r: r.map(func))

    def map_value(self, func):
        return self.transform_result(lambda r: r.map_value(func))

    def foreach(self, func):
        return self.transform_result(lambda r: r.foreach(func))

    def filter(self, func):
        return self.transform_result(lambda r: r.filter(func))

    def filter_value(self, func):
        return self.transform_result(lambda r: r.filter_value(func))

    def record(self, *, key=None, key_prefix=None):
        if key is None:
            key_prefix = 'r_' if key_prefix is None else key_prefix
            index = self._get_record_index()
            key = key_prefix + str(index)

        def _record(item: ForkItem):
            context = item.context
            if context.has_var(key):
                raise ValueError(f'{key} already record')

            return item.context.set_var(key, item.value).new_item(item.value)
        return key, self.map(_record)

    def __getattr__(self, name):
        return self.map_value(lambda v: getattr(v, name))

    def __call__(self, *args, **kwargs):
        return self.call(self, *args, **kwargs)

    @classmethod
    def _get_record_index(cls):
        global _RECORD_INDEX
        with _LOCK:
            _RECORD_INDEX += 1
            return _RECORD_INDEX

    @classmethod
    def call(cls, func, *args, **kwargs) -> Forker:
        if not isinstance(func, Forker):
            func = SingleValueForker(func)
        seed = ChainForker([func, _ArgsForker(args=args, kwargs=kwargs)])
        return ReactionForker(seed).map_value(lambda value: value[0](*value[1][0], **value[1][1]))


class _ArgsForker(Forker):
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


class ContextRecordForker(Forker[T]):
    def __init__(self, key):
        self._key = key

    @property
    def key(self):
        return self._key

    def do_fork(self, context: ForkContext) -> ForkResult[T]:
        return context.new_fork_result([context.get_var(self._key)])


class TransformForker(Forker[T]):
    def __init__(self, forker, funcs, *, name=None):
        self._forker = forker
        self._funcs = tuple(funcs)
        self._name = name

    def add_func(self, func):
        return TransformForker(self._forker, self._funcs + (func,), name=self._name)

    def do_fork(self, context: ForkContext) -> ForkResult[T]:
        result = self._forker.do_fork(context=context)
        for func in self._funcs:
            result = func(result)
        return result

    def __str__(self):
        return self._name or f'Transform({self._forker})'


class FlatForker(Forker[T]):
    def __init__(self, values: Iterable[T], *, name=None):
        self._values = values
        self._name = name

    def do_fork(self, context: ForkContext) -> ForkResult[T]:
        return context.new_fork_result(self._values)

    def __str__(self):
        return self._name or f'FlatForker({str(self._values)})'


class SingleValueForker(Forker[T]):
    def __new__(cls, value, **kwargs):
        return FlatForker([value], **kwargs)

    def do_fork(self, context: ForkContext) -> ForkResult[T]:
        pass


class RangeForker(Forker[int]):
    def __new__(cls, start, end, **kwargs):
        return FlatForker(range(start, end), **kwargs)

    def do_fork(self, context: ForkContext) -> ForkResult[T]:
        pass


class DefaultValueForker(Forker[T]):
    def __init__(self, forker: Forker[T], *, default=None):
        self._forker = forker
        self._default = default

    def do_fork(self, context: ForkContext) -> ForkResult[T]:
        def _generator():
            need_default = True
            for item in self._forker.do_fork(context):
                need_default = False
                yield item

            if need_default:
                yield context.new_item(self._default)

        return ForkResult(_generator())


class ConcatForker(Forker[T]):
    def __init__(self, forkers=None, *, name=None):
        self._forkers = forkers or tuple()
        self._name = name

    def do_fork(self, context: ForkContext) -> ForkResult[T]:
        iters = map(lambda f: f.do_fork(context=context), self._forkers)
        return ForkResult(itertools.chain(*iters))

    def add_forker(self, forker):
        return ConcatForker(tuple(self._forkers) + (forker,), name=self._name)

    def __str__(self):
        return self._name or f'ConcatForker({", ".join([str(forker) for forker in self._forkers])})'


class ReactionForker(Forker[T]):
    def __init__(self, seed: Forker, *, name=None, stop: Callable[[ForkItem], bool] = None):
        self._seed = seed
        self._name = name
        self._stop = stop or (lambda _: False)

    def do_fork(self, context: ForkContext) -> ForkResult[T]:
        items = self._generate(context)
        return ForkResult(items)

    def _generate(self, context: ForkContext):
        for item in self._seed.do_fork(context):
            if self._stop(item) or not isinstance(item.value, Forker):
                yield item
                continue

            next_forker = ReactionForker(item.value, stop=self._stop)
            for new_item in next_forker.do_fork(item.context):
                yield new_item

    def __str__(self):
        return self._name or f'{self.__class__.__name__}#{id(self)}'


class ChainForker(Forker):
    def __init__(self, forkers, *, reduce=None, initializer=None):
        self._forkers = list(forkers)
        self._reduce = reduce or self._default_reduce_func
        self._state = initializer() if initializer else None

    def do_fork(self, context: ForkContext) -> ForkResult:
        if not self._forkers:
            return context.new_fork_result([self._state])

        forker = self._forkers[0]
        if callable(forker) and not isinstance(forker, Forker):
            forker = forker(self._state)

        next_forkers = self._forkers[1:]

        def _map_to_forker(value):
            next_state = self._reduce(self._state, value)
            return self.__class__(
                forkers=next_forkers,
                reduce=self._reduce,
                initializer=lambda: next_state,
            )

        return forker.do_fork(context).map_value(_map_to_forker)

    @staticmethod
    def _default_reduce_func(state, value):
        if state is None:
            return value,
        return state + (value,)
