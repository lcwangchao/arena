from __future__ import annotations

import collections

import typing

import abc
from dataclasses import dataclass

from .fork import *

__all__ = ['EventDrivenState', 'cond', 'action']


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
class _Action:
    name: str
    func: typing.Callable
    cond: typing.Callable
    args: typing.Any

    def __call__(self, state):
        if isinstance(self.args, (list, tuple)):
            args = self.args
            kwargs = dict()
        elif isinstance(self.args, dict):
            args = tuple()
            kwargs = self.args
        else:
            args = (self.args,)
            kwargs = dict()

        return self.func(state, *args, **kwargs)

    @staticmethod
    def get_attached(obj):
        return getattr(obj, '_actions', None)

    @staticmethod
    def decorate_func(func, *, cond=None, name=None, args=None):
        if not name:
            name = func.__name__

        actions = getattr(func, '_actions', None)
        if not actions:
            actions = collections.OrderedDict()
            setattr(func, '_actions', actions)

        if name in actions:
            raise ValueError('duplicated name for multi actions in one func')

        actions[name] = _Action(name=name, func=func, cond=cond, args=args)
        return func

    @staticmethod
    def always_true_cond(*_, **__):
        return True


def action(func=None, *, name=None, cond=None, args=None):
    def _wrapper(_func):
        return _Action.decorate_func(_func, cond=cond, name=name, args=args)

    if func:
        return _wrapper(func)

    return _wrapper


class EventDrivenStateMeta(abc.ABCMeta):
    def __new__(mcs, name, bases, attrs):
        new_attrs = attrs.copy()
        actions = collections.OrderedDict()
        for name, attr in attrs.items():
            acts = _Action.get_attached(attr)
            if acts:
                for name, act in acts.items():
                    if name in actions:
                        raise ValueError('duplicated action name: ' + name)
                    actions[name] = act

        mcs.ACTIONS: typing.Dict[_Action] = actions
        return type.__new__(mcs, name, bases, new_attrs)


class EventDrivenState(metaclass=EventDrivenStateMeta):
    @abc.abstractmethod
    def signature(self):
        """
        :return: the signature for the state to dedup
        """

    def _pick(self, actions, dedup):
        next_actions = [act for act in actions if act.cond(self)]
        if not next_actions:
            return None

        state_sig = self.signature()
        next_actions = [act for act in next_actions if (state_sig, act.name) not in dedup]
        if not next_actions:
            next_actions = [None]

        next_action = yield FlatForker(next_actions)
        return next_action

    @classmethod
    def run(cls, args=None, kwargs=None):
        args = args or tuple()
        kwargs = kwargs or dict()
        dedup = set()

        def _run_func():
            state = cls(*args, **kwargs)
            while True:
                next_action = yield from state._pick(cls.ACTIONS.values(), dedup)
                if next_action is None:
                    return state

                dedup.add((state.signature(), next_action.name))
                next_action(state)

        forker = GeneratorForker(_run_func, args=args, kwargs=kwargs)
        for finalState in forker:
            yield finalState
