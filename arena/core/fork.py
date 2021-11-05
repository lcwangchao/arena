from __future__ import annotations

import abc
import typing

from collections import Iterator, Iterable


class Stack:
    def __init__(self):
        self._prev = None
        self._content = None
        self._len = 0

    def prev(self):
        return self._prev

    def peek(self):
        return self._content

    def push(self, content) -> Stack:
        node = Stack()
        node._prev = self
        node._content = content
        node._len = self._len + 1
        return node

    def pop(self) -> Stack:
        if self._prev is None:
            raise IndexError('pop from empty stack')

        return self._prev

    def iter_reverse(self):
        cur = self
        while cur._prev is not None:
            yield cur._content
            cur = cur._prev

    def __len__(self):
        return self._len


class RecursionState:
    def __init__(self, forker: Forker, *, stack: Stack = None, builder: RecursionEntityBuilder):
        self._forker = forker
        self._stack = stack or Stack()
        self._builder = builder

    def push_entity(self, entity) -> RecursionState:
        return RecursionState(
            self._forker,
            stack=self._stack.push(entity),
            builder=self._builder.update(entity)
        )

    def iter_stack_reverse(self):
        return self._stack.iter_reverse()

    @property
    def stack_items(self):
        items = list(self._stack.iter_reverse())
        items.reverse()
        return items

    @property
    def forker(self) -> Forker:
        return self._forker

    @property
    def builder(self) -> RecursionEntityBuilder:
        return self._builder

    @property
    def stack(self):
        return self._stack

    def __str__(self):
        return str(self._forker)


class ForkContext:
    def __init__(self, *, variables: typing.Dict = None, recursions: Stack = None):
        self._recursions = recursions or Stack()
        self._vars = variables.copy() if variables else {}

    @property
    def vars(self):
        return self._vars

    def set_var(self, name, value) -> ForkContext:
        new_vars = self._vars.copy()
        new_vars[name] = value
        return self._replace_vars(new_vars)

    def get_var(self, name: str, *, default=None):
        if name in self._vars:
            return self._vars[name]
        return default

    @property
    def recursions(self):
        return self._recursions

    def enter_recursion(self, forker: Forker, builder: RecursionEntityBuilder) -> ForkContext:
        return ForkContext(
            variables=self._vars,
            recursions=self.recursions.push(RecursionState(forker, builder=builder))
        )

    def exit_recursion(self) -> ForkContext:
        return ForkContext(
            variables=self._vars,
            recursions=self.recursions.pop()
        )

    @property
    def current_recursion(self) -> RecursionState:
        return self._recursions.peek()

    def push_entity(self, entity) -> ForkContext:
        recursion = self.current_recursion.push_entity(entity)
        recursions = self.recursions.pop()
        return ForkContext(variables=self._vars, recursions=recursions.push(recursion))

    def _replace_vars(self, variables: typing.Dict) -> ForkContext:
        return ForkContext(variables=variables, recursions=self._recursions)


class Forker(abc.ABC, Iterable[typing.Tuple[ForkContext, typing.Any]]):
    @abc.abstractmethod
    def do_fork(self, *, ctx: ForkContext) -> Iterator[typing.Tuple[ForkContext, typing.Any]]:
        pass

    def __iter__(self) -> Iterator[typing.Tuple[ForkContext, typing.Any]]:
        return self.do_fork(ctx=ForkContext())

    def map(self, func, **kwargs) -> MapForker:
        return MapForker(self, func, **kwargs)

    def flat_map(self, func, **kwargs) -> FlatMapForker:
        return FlatMapForker(self, func, **kwargs)

    def filter(self, func) -> FilterForker:
        return FilterForker(self, func)


class MapForker(Forker):
    def __init__(self, forker: Forker, func: typing.Callable[[ForkContext, typing.Any], typing.Any], *,
                 desc=None, update_ctx: typing.Callable[[ForkContext, typing.Any], ForkContext] = None):
        self._forker = forker
        self._func = func
        self._update_ctx = update_ctx
        self._desc = desc

    def do_fork(self, *, ctx: ForkContext) -> Iterator[typing.Tuple[ForkContext, typing.Any]]:
        return self._do_fork(ctx=ctx)

    def _do_fork(self, *, ctx: ForkContext):
        for new_ctx, entity in self._forker.do_fork(ctx=ctx):
            entity = self._func(new_ctx, entity)
            if self._update_ctx:
                new_ctx = self._update_ctx(new_ctx, entity)
            yield new_ctx, entity

    def __str__(self) -> str:
        return self._desc or f'map{str(self._forker)}'


class FlatMapForker(Forker):
    def __init__(self, forker: Forker, func: typing.Callable[[ForkContext, typing.Any], Iterable], *,
                 desc=None, update_ctx: typing.Callable[[ForkContext, typing.Any], ForkContext] = None):
        self._forker = forker
        self._func = func
        self._update_ctx = update_ctx
        self._desc = desc

    def do_fork(self, *, ctx: ForkContext) -> Iterator[typing.Tuple[ForkContext, typing.Any]]:
        return self._do_fork(ctx=ctx)

    def _do_fork(self, *, ctx: ForkContext):
        for entity_ctx, entity in self._forker.do_fork(ctx=ctx):
            for new_entity in self._func(entity_ctx, entity):
                new_ctx = entity_ctx
                if self._update_ctx:
                    new_ctx = self._update_ctx(new_ctx, new_entity)
                yield new_ctx, new_entity

    def __str__(self) -> str:
        return self._desc or f'flatmap{str(self._forker)}'


class FilterForker(Forker):
    def __init__(self, forker: Forker, func: typing.Callable[[ForkContext, typing.Any], bool], *,
                 desc=None):
        self._forker = forker
        self._func = func
        self._desc = desc

    def do_fork(self, *, ctx: ForkContext = None) -> Iterator[typing.Tuple[ForkContext, typing.Any]]:
        return filter(
            lambda new_ctx, entity: self._func(new_ctx, entity),
            self._forker.do_fork(ctx=ctx)
        )

    def __str__(self) -> str:
        return self._desc or f'filter{str(self._forker)}'


class IterableForker(Forker):
    def __init__(self, entities: Iterable, *,
                 desc=None, update_ctx: typing.Callable[[ForkContext, typing.Any], ForkContext] = None):
        self._entities = entities
        self._update_ctx = update_ctx
        self._desc = desc

    def do_fork(self, *, ctx: ForkContext = None) -> Iterator[typing.Tuple[ForkContext, typing.Any]]:
        def _map(entity):
            nonlocal ctx
            if self._update_ctx:
                ctx = self._update_ctx(ctx, entity)
            return ctx, entity

        return iter(map(_map, self._entities))

    def __str__(self) -> str:
        return self._desc or f"[{', '.join([str(e) for e in self._entities])}]"


class RecursionEntityBuilder(abc.ABC):
    @abc.abstractmethod
    def update(self, entity) -> RecursionEntityBuilder:
        pass

    @abc.abstractmethod
    def build(self, ctx: ForkContext) -> typing.Tuple[ForkContext, typing.Any]:
        pass


class RecursionForker(Forker):
    class _FuncBuilder(RecursionEntityBuilder):
        def __init__(self, build: typing.Callable[[ForkContext], typing.Tuple[ForkContext, typing.Any]]):
            self._build = build

        def update(self, entity) -> RecursionEntityBuilder:
            return self

        def build(self, ctx: ForkContext) -> typing.Tuple[ForkContext, typing.Any]:
            return self._build(ctx)

    def __init__(self, *, forkers: Iterable[Forker],
                 builder: RecursionEntityBuilder = None,
                 build: typing.Callable[[ForkContext], typing.Tuple[ForkContext, typing.Any]] = None,
                 desc=None):
        if not builder and not build:
            raise TypeError("one of builder or build should be specified")

        if builder and build:
            raise TypeError("only one of builder or build should be specified")

        if build:
            builder = self._FuncBuilder(build=build)

        self._forkers = list(forkers)
        self._builder = builder
        self._desc = desc

    @property
    def forkers(self):
        return self._forkers

    def do_fork(self, *, ctx: ForkContext) -> Iterator[typing.Tuple[ForkContext, typing.Any]]:
        forkers = self._forkers
        if not forkers:
            return iter(())

        return self._do_fork(ctx.enter_recursion(self, builder=self._builder))

    def _do_fork(self, ctx: ForkContext):
        cur_forker = self._forkers[len(ctx.current_recursion.stack)]
        for new_ctx, entity in cur_forker.do_fork(ctx=ctx):
            new_ctx = new_ctx.push_entity(entity)
            if len(new_ctx.current_recursion.stack) == len(self.forkers):
                new_ctx, obj = new_ctx.current_recursion.builder.build(new_ctx)
                yield new_ctx.exit_recursion(), obj
            else:
                yield from self._do_fork(new_ctx)

    def __str__(self) -> str:
        return self._desc or f"Recursion[{', '.join([str(f) for f in self._forkers])}]"


def to_forker(obj) -> Forker:
    if isinstance(obj, Forker):
        return obj

    return IterableForker([obj], desc=str(obj))


def combine_forkers(*forkers, rtype='list', desc=None) -> Forker:
    def _build(ctx: ForkContext):
        ret = ctx.current_recursion.stack_items
        if rtype == 'tuple':
            ret = tuple(ret)
        return ctx, ret

    return RecursionForker(
        forkers=forkers,
        build=_build,
        desc=desc
    )
