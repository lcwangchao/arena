from __future__ import annotations

import collections

import typing

import abc
from dataclasses import dataclass

from .fork import *

__all__ = ['EventDrivenState', 'cond', 'action', 'current_action_index']


class _Cond:
    def __init__(self, conditions, op='match', name=None):
        if isinstance(conditions, list):
            conditions = tuple(conditions)

        if not isinstance(conditions, tuple):
            conditions = (conditions,)

        if op not in ('match', 'and', 'or', 'not'):
            raise ValueError('invalid op: ' + op)

        if not conditions:
            raise ValueError('conditions must not be empty')

        if op in ('match', 'not') and len(conditions) != 1:
            raise ValueError("only one condition is allowed when the op is 'match' or 'not'")

        if op in ('and', 'or') and len(conditions) == 1:
            op = 'match'

        self._op = op
        self._conditions = conditions
        self._name = name

    def __call__(self, state):
        if self._op == 'match':
            return self._evaluate(state, self._conditions[0])
        elif self._op == 'not':
            return not self._evaluate(state, self._conditions[0])
        else:
            for cond in self._conditions:
                if self._op == 'and' and not self._evaluate(state, cond):
                    return False

                if self._op == 'or' and self._evaluate(state, cond):
                    return True

            return self._op == 'and'

    def __and__(self, other):
        return self._binary_op('and', other)

    def __or__(self, other):
        return self._binary_op('or', other)

    def __invert__(self):
        return self.__class__((self,), op='not')

    def __bool__(self):
        raise ValueError('illegal operation')

    def _binary_op(self, op, other):
        if not isinstance(other, self.__class__):
            raise ValueError('not a condition')

        conditions = []
        for cond in [self, other]:
            if cond._op == op:
                conditions.extend(cond._conditions)
            else:
                conditions.append(cond)

        return self.__class__(conditions, op=op)

    def __str__(self):
        if self._name:
            return self._name

        if self._op == 'match':
            return str(self._conditions[0].__name__)
        if self._op == 'not':
            return "!" + str(self._conditions[0])

        connector = ' && ' if self._op == 'and' else ' || '
        return f'({connector.join([str(c) for c in self._conditions])})'

    @staticmethod
    def _evaluate(state, cond):
        if callable(cond):
            return cond(state)
        return bool(state)


def cond(func=None, *, name=None):
    def _decorator(_func):
        return _Cond(_func, name=name or func.__name__)

    if func:
        return _decorator(func)

    return _decorator


@dataclass
class Action:
    name: str
    func: typing.Callable
    cond: typing.Callable
    args: typing.Any

    def __call__(self, state: EventDrivenState, *, index):
        if self.args is None:
            args = tuple()
            kwargs = dict()
        elif isinstance(self.args, (list, tuple)):
            args = self.args
            kwargs = dict()
        elif isinstance(self.args, dict):
            args = tuple()
            kwargs = self.args
        else:
            args = (self.args,)
            kwargs = dict()

        try:
            setattr(state, '_current_action_index', index)
            return self.func(state, *args, **kwargs)
        finally:
            delattr(state, '_current_action_index')

    @classmethod
    def get_attached(cls, obj):
        return getattr(obj, '_actions', None)

    @classmethod
    def decorate_func(cls, func, *, cond=None, name=None, args=None):
        if not name:
            name = func.__name__

        actions = getattr(func, '_actions', None)
        if not actions:
            actions = collections.OrderedDict()
            setattr(func, '_actions', actions)

        if name in actions:
            raise ValueError('duplicated name for multi actions in one func')

        if cond is None:
            cond = cls.always_true_cond

        actions[name] = Action(name=name, func=func, cond=cond, args=args)
        return func

    @classmethod
    def always_true_cond(cls, *_, **__):
        return True


def current_action_index(state):
    idx = getattr(state, '_current_action_index', None)
    if idx is None:
        raise ValueError('Not in a action call')
    return idx


def action(func=None, *, name=None, cond=None, args=None, generate=None):
    def _wrapper(_func):
        if generate:
            if name or args:
                raise ValueError('generate is mutually exclusive with other options')

            for info in generate():
                _func = Action.decorate_func(
                    _func,
                    name=info.get('name', None),
                    cond=info.get('cond', cond),
                    args=info.get('args', None)
                )

            return _func

        return Action.decorate_func(_func, cond=cond, name=name, args=args)

    if func:
        return _wrapper(func)

    return _wrapper


class EventDrivenStateMeta(abc.ABCMeta):
    def __new__(mcs, name, bases, attrs):
        new_attrs = attrs.copy()
        actions = collections.OrderedDict()
        for name, attr in attrs.items():
            acts = Action.get_attached(attr)
            if acts:
                for name, act in acts.items():
                    if name in actions:
                        raise ValueError('duplicated action name: ' + name)
                    actions[name] = act

        mcs.ACTIONS: typing.Dict[Action] = actions
        return type.__new__(mcs, name, bases, new_attrs)


class EventDrivenState(metaclass=EventDrivenStateMeta):
    @abc.abstractmethod
    def signature(self):
        """
        :return: the signature for the state to dedup
        """

    def setup(self):
        pass

    def close(self):
        pass

    @property
    def action_records(self) -> typing.List[Action]:
        return getattr(self, '_action_records', [])

    @classmethod
    def run(cls, *args, **kwargs):
        yield from StateDriver(cls).run(*args, **kwargs)


class StateDriver:
    class _DedupForker(Forker):
        def __init__(self, driver: StateDriver, state):
            self._dedup = driver._dedup
            self._actions = [act for act in driver._actions if act.cond(state)]
            self._state_sig = state.signature()

        def do_fork(self, context: ForkContext) -> ForkResult[Action]:
            return context.new_fork_result(self._generator())

        def _generator(self):
            empty = True
            for act in self._actions:
                sig = (self._state_sig, act.name)
                if sig not in self._dedup:
                    empty = False
                    yield act

            if empty:
                yield None

    def __init__(self, cls):
        self._dedup = set()
        self._actions = cls.ACTIONS.values()
        self._cls = cls

    def run(self, *args, **kwargs):
        forker = GeneratorForker(self._run, args=args, kwargs=kwargs)
        for finalState in forker:
            yield finalState

    def _pick(self, state):
        next_action = yield self._DedupForker(self, state)
        return next_action

    def _run(self, *args, **kwargs):
        state = self._cls(*args, **kwargs)
        state.setup()
        while True:
            next_action = yield from self._pick(state)
            if next_action is None:
                return state

            sig = (state.signature(), next_action.name)
            self._dedup.add(sig)
            self._record_action(state, next_action)
            next_action(state, index=len(state.action_records) - 1)

    @staticmethod
    def _record_action(state, record):
        records = getattr(state, '_action_records', None)
        if not records:
            records = []
            setattr(state, '_action_records', records)

        records.append(record)
