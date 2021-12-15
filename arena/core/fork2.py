from __future__ import annotations

import threading
from collections import Iterator, Iterable
from dataclasses import dataclass

import abc
import itertools
from typing import TypeVar, Generic, Dict, Callable, Any

T = TypeVar('T')

__all__ = ['ForkContext', 'ForkItem', 'ForkResult', 'Forker', 'TransformForker', 'FlatForker', 'SingleValueForker',
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


class Forker(abc.ABC, Generic[T], Iterable[T]):
    _RECORD_INDEX = 0
    _LOCK = threading.Lock()

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
            return item.context.set_var(key, item.value).new_item(item.value)
        return key, self.map(_record)

    def __getattr__(self, name):
        return self.map_value(lambda value: getattr(value, name))

    @classmethod
    def _get_record_index(cls):
        with cls._LOCK:
            cls._RECORD_INDEX += 1
            return cls._RECORD_INDEX


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
        if callable(forker):
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
