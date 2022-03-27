import operator
import unittest
from arena.core.event_driven import *


class TestEventDriven(unittest.TestCase):
    def test_even_cond(self):
        @cond
        def zero(v):
            return v == 0

        @cond
        def positive(v):
            return v > 0

        @cond
        def negative(v):
            return v < 0

        @cond
        def odd(v):
            return v % 2 == 1

        @cond
        def even(v):
            return v % 2 == 0

        self.assertTrue(positive(1))
        self.assertFalse(negative(1))
        self.assertTrue(odd(1))
        self.assertFalse(even(1))

        self.assertFalse(positive(-2))
        self.assertTrue(negative(-2))
        self.assertFalse(odd(-2))
        self.assertTrue(even(-2))

        self.assertTrue((positive & odd)(1))
        self.assertFalse((positive & even)(1))
        self.assertFalse((negative & odd)(1))
        self.assertFalse((negative & even)(1))

        self.assertFalse((negative | even)(1))
        self.assertTrue((positive | even)(1))
        self.assertTrue((negative | odd)(1))
        self.assertTrue((positive | odd)(1))

        self.assertTrue((~negative)(1))
        self.assertFalse((~positive)(1))

        self.assertTrue((positive & odd & ~zero)(1))
        self.assertFalse((zero & positive & odd)(1))
        self.assertFalse((positive & odd & zero)(1))
        self.assertFalse((positive & zero & odd)(1))

        self.assertFalse((negative | odd | ~zero)(0))
        self.assertTrue((zero | negative | odd)(0))
        self.assertTrue((negative | zero | odd)(0))
        self.assertTrue((negative | odd | zero)(0))

        with self.assertRaises(ValueError):
            bool(positive)

    def test_event_driven_state(self):
        def dist(op, v):
            op_map = {
                '>': operator.gt,
                '<': operator.lt,
                '>=': operator.ge,
                '<=': operator.gt,
                '==': operator.eq,
                '!=': operator.ne
            }

            @cond(name=f'distance {op} {v}')
            def _compare(state):
                if op not in op_map:
                    raise ValueError('invalid op: ' + op)

                return op_map[op](state.distance, v)
            return _compare

        class State(EventDrivenState):
            TARGET = 12

            def __init__(self):
                self.pos = 0
                self.target = self.TARGET
                self.last_move = 0
                self.path = []
                self.covers = []

            def signature(self):
                return self.pos

            @property
            def distance(self):
                return self.target - self.pos

            @action(name='move1', cond=dist('>=', 1), args=1)
            @action(name='move2', cond=dist('>=', 2), args=2)
            @action(name='move3', cond=dist('>=', 3), args=3)
            def move(self, v):
                self.covers.append((self.pos, v))
                self.pos += v
                self.last_move = v
                self.path.append(v)

        covers = set()
        for state in State.run():
            state_covers = state.covers
            has_uncovered = False
            for cover in state_covers:
                if cover not in covers:
                    has_uncovered = True
                    covers.add(cover)
            self.assertTrue(has_uncovered)

        expected_covers = set()
        for move in [1, 2, 3]:
            for pos in range(0, State.TARGET - move + 1):
                expected_covers.add((pos, move))

        self.assertEqual(expected_covers, covers)
