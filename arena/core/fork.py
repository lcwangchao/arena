from __future__ import annotations

import abc
import typing

from typing import Any
from collections import Iterator, Iterable


class ForkContext:
    def __init__(self, *, variables: typing.Dict = None):
        self._vars = variables.copy() if variables else {}

    def new_context_with_vars(self, variables: typing.Dict) -> ForkContext:
        new_vars = self._vars.copy()
        new_vars.update(variables)
        return ForkContext(variables=new_vars)

    def get_var(self, name: str, *, default=None):
        if name in self._vars:
            return self._vars[name]
        return default


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
    def __init__(self, forker: Forker, func: typing.Callable[[ForkContext, Any], Any], *,
                 update_ctx: typing.Callable[[ForkContext, Any], ForkContext] = None):
        self._forker = forker
        self._func = func
        self._update_ctx = update_ctx

    def do_fork(self, *, ctx: ForkContext = None) -> Iterator[typing.Tuple[ForkContext, typing.Any]]:
        def _map(args):
            new_ctx = args[0]
            entity = self._func(new_ctx, args[1])
            if self._update_ctx:
                new_ctx = self._update_ctx(new_ctx, entity)
            return new_ctx, entity

        return map(_map, self._forker.do_fork(ctx=ctx))


class FlatMapForker(Forker):
    def __init__(self, forker: Forker, func: typing.Callable[[ForkContext, Any], Iterable], *,
                 update_ctx: typing.Callable[[ForkContext, Any], ForkContext] = None):
        self._forker = forker
        self._func = func
        self._update_ctx = update_ctx

    def do_fork(self, *, ctx: ForkContext = None) -> Iterator[typing.Tuple[ForkContext, typing.Any]]:
        def _generator():
            for entity_ctx, entity in self._forker.do_fork(ctx=ctx):
                for new_entity in self._func(entity_ctx, entity):
                    new_ctx = entity_ctx
                    if self._update_ctx:
                        new_ctx = self._update_ctx(new_ctx, new_entity)
                    yield new_ctx, new_entity

        return _generator()


class FilterForker(Forker):
    def __init__(self, forker: Forker, func: typing.Callable[[ForkContext, Any], bool]):
        self._forker = forker
        self._func = func

    def do_fork(self, *, ctx: ForkContext = None) -> Iterator[typing.Tuple[ForkContext, typing.Any]]:
        def _filter(args):
            return args[0], self._func(args[0], args[1])

        return filter(_filter, self._forker.do_fork(ctx=ctx))


class IterableForker(Forker):
    def __init__(self, entities: Iterable, *,
                 update_ctx: typing.Callable[[ForkContext, Any], ForkContext] = None):
        self._entities = entities
        self._update_ctx = update_ctx

    def do_fork(self, *, ctx: ForkContext = None) -> Iterator[typing.Tuple[ForkContext, typing.Any]]:
        def _map(entity):
            nonlocal ctx
            if self._update_ctx:
                ctx = self._update_ctx(ctx, entity)
            return ctx, entity

        return iter(map(_map, self._entities))


class RecursionForkState:
    def __init__(self, *, forkers):
        self._forkers = forkers
        self._entities = tuple()

    @property
    def forkers(self):
        return self._forkers

    @property
    def entities(self):
        return self._entities

    def add_entity(self, entity) -> RecursionForkState:
        state = RecursionForkState(forkers=self.forkers)
        state._entities = self._entities + (entity,)
        return state


class RecursionForker(Forker):
    def __init__(self, *, forkers: Iterable[Forker],
                 build: typing.Callable[[ForkContext, RecursionForkState], typing.Tuple[ForkContext, Any]]):
        self._forkers = list(forkers)
        self._build = build

    @property
    def forkers(self):
        return self._forkers

    def do_fork(self, *, ctx: ForkContext = None) -> Iterator[typing.Tuple[ForkContext, typing.Any]]:
        forkers = self._forkers
        if not forkers:
            return iter(())

        return self._do_fork(
            ctx if ctx else ForkContext(),
            RecursionForkState(forkers=forkers)
        )

    def _do_fork(self, ctx: ForkContext, state: RecursionForkState):
        cur_forker = state.forkers[len(state.entities)]
        for new_ctx, entity in cur_forker.do_fork(ctx=ctx):
            new_state = state.add_entity(entity)
            if len(new_state.entities) == len(new_state.forkers):
                yield self._build(new_ctx, new_state)
            else:
                yield from self._do_fork(new_ctx, new_state)


def to_forker(obj) -> Forker:
    if isinstance(obj, Forker):
        return obj
    return IterableForker([obj])


def combine_forkers(*forkers, rtype='list') -> Forker:
    def _build(ctx: ForkContext, state: RecursionForkState):
        ret = state.entities
        if rtype == 'list':
            ret = list(ret)
        return ctx, ret

    return RecursionForker(
        forkers=forkers,
        build=_build
    )
