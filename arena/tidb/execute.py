import io
import unittest

from mysql.connector import MySQLConnection

from arena.core.mysql import Database


class CaseExecContext:
    def __init__(self, *, name: str, t: unittest.TestCase, db: Database):
        self._name = name
        self._db_name = f'_tc_{self._name}'
        self._t = t
        self._db = db
        self._conn = None
        self._vars = {}
        self._logs = []

    @property
    def name(self):
        return self._name

    @property
    def db_name(self):
        return self._db_name

    @property
    def t(self):
        return self._t

    @property
    def db(self):
        return self._db

    @property
    def conn(self) -> MySQLConnection:
        if not self._conn:
            self._conn = self._db.conn()
        return self._conn

    @property
    def vars(self):
        return self._vars

    def append_log(self, log):
        self._logs.append(log)

    def dump_detail(self):
        w = io.StringIO()
        w.write(f"'{self.name}' execute details:\n")
        w.write(f'  > Logs:\n')
        for log in self._logs:
            w.write('    ')
            w.write(log)
            w.write('\n')
        return w.getvalue()

    def __enter__(self):
        self._setup_case()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._teardown_case()

    def _setup_case(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"DROP DATABASE IF EXISTS `{self.db_name}`")
                cur.execute(f"CREATE DATABASE `{self.db_name}`")
                cur.execute(f"USE `{self.db_name}`")
        except BaseException:
            self._teardown_case()
            raise

    def _teardown_case(self):
        with self.conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS `{self.db_name}`")

        self._conn.close()
        self._conn = None


class Execute:
    def __init__(self, func):
        self._func = func if func else self._empty_func

    def __call__(self, ctx: CaseExecContext):
        self._func(ctx)

    @staticmethod
    def _empty_func(*args, **kwargs):
        """
        Do nothing
        """
