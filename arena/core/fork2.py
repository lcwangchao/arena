from __future__ import annotations

from collections import Iterator, Iterable
from dataclasses import dataclass

import abc
import itertools
from typing import TypeVar, Generic, Dict

T = TypeVar('T')


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

    def __eq__(self, o: ForkItem[T]) -> bool:
        if o is None:
            return False
        return self.context == o.context and self.value == o.value


class ForkResult(Generic[T], Iterator[ForkItem[T]]):
    def __init__(self, items: Iterable[ForkItem[T]] = None):
        self._iter = iter(items or tuple())

    def __next__(self) -> ForkItem[T]:
        return next(self._iter)

    def to_values(self):
        return map(lambda item: item.value, self)

    def map(self, func):
        return ForkResult(map(func, self._iter))

    def map_value(self, func):
        return ForkResult(map(
            lambda item: item.context.new_item(func(item.value)),
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
    @abc.abstractmethod
    def do_fork(self, *, context: ForkContext) -> ForkResult[T]:
        pass

    def __iter__(self) -> Iterator[T]:
        it = self.do_fork(context=ForkContext())
        return map(lambda item: item.value, it)

    def transform_result(self, func) -> Forker[T]:
        if isinstance(self, TransformForker):
            return self.add_func(func)
        return TransformForker(self, [func])

    def map(self, func):
        return self.transform_result(lambda r: r.map(func))

    def map_value(self, func):
        return self.transform_result(lambda r: r.map_value(func))

    def filter(self, func):
        return self.transform_result(lambda r: r.filter(func))

    def filter_value(self, func):
        return self.transform_result(lambda r: r.filter_value(func))


class TransformForker(Forker[T]):
    def __init__(self, forker, funcs, *, name=None):
        self._forker = forker
        self._funcs = tuple(funcs)
        self._name = name

    def add_func(self, func):
        return TransformForker(self._forker, self._funcs + (func,))

    def do_fork(self, *, context: ForkContext) -> ForkResult[T]:
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

    def do_fork(self, *, context: ForkContext) -> ForkResult[T]:
        return context.new_fork_result(self._values)

    def __str__(self):
        return self._name or f'FlatForker({str(self._values)})'


class ChainedForker(Forker[T]):
    def __init__(self, forkers=None, *, name=None):
        self._forkers = forkers or tuple()
        self._name = name

    def do_fork(self, *, context: ForkContext) -> ForkResult[T]:
        iters = map(lambda f: f.do_fork(context=context), self._forkers)
        return ForkResult(itertools.chain(*iters))

    def __str__(self):
        return self._name or f'ChainedForker({", ".join([str(forker) for forker in self._forkers])})'


class AssemblyBuilder(abc.ABC, Generic[T]):
    @abc.abstractmethod
    def update(self, value) -> AssemblyBuilder[T]:
        pass

    @abc.abstractmethod
    def build(self) -> T:
        pass


class DefaultAssemblyBuilder:
    def __init__(self):
        self._values = tuple()

    @property
    def values(self):
        return self._values

    def update(self, value) -> DefaultAssemblyBuilder:
        next_builder = DefaultAssemblyBuilder()
        next_builder._values = self._values + (value,)
        return next_builder

    def build(self):
        return self._values


class AssemblyForker(Forker[T]):
    def __init__(self, *, children=None, builder: AssemblyBuilder = None, name=None):
        self._children = tuple(children) if children else tuple()
        self._builder = builder or DefaultAssemblyBuilder()
        self._name = name

    def do_fork(self, *, context: ForkContext) -> ForkResult[T]:
        if not self._children:
            return ForkResult()

        forker = self._children[0]
        if callable(forker):
            forker = forker(self._builder)

        items = self._assemble(context, forker, self._children[1:])
        return ForkResult(items)

    def _assemble(self, context, forker, next_children):
        for item in forker.do_fork(context=context):
            next_builder = self._builder.update(item.value)
            if next_children:
                next_assembly = AssemblyForker(children=next_children, builder=next_builder)
                for next_item in next_assembly.do_fork(context=item.context):
                    yield next_item
            else:
                yield item.context.new_item(next_builder.build())

    def __str__(self):
        return self._name or f'AssemblyForker({", ".join([str(forker) for forker in self._children])})'
