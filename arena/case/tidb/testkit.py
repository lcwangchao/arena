import typing
import unittest

from arena.core.mysql import Database
from arena.tidb.testkit import Case, TestKit


class TestManager:
    def __init__(self):
        self._cases = []
        self._runner = unittest.TextTestRunner()

    def add_case(self, func):
        self._cases.append(Case(func=func))

    def run_tests(self, *, db: Database = None):
        for case in self._cases:
            case.run_test(db=db)


def test_manager() -> TestManager:
    manager = getattr(TestManager, '_default_inst', None)
    if not manager:
        manager = TestManager()
        setattr(TestManager, '_default_inst', manager)
    return manager


def tidb_test(func: typing.Callable[[TestKit], None]):
    test_manager().add_case(func)
