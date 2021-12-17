from __future__ import annotations

import mysql.connector

from arena.core.testkit import TestKit, testkit, fork_exec


class Rows:
    def __init__(self, tk: TestKit, *, rows=None):
        self._tk = tk
        self._rows = rows

    @fork_exec
    def check(self, rows):
        ut = self._tk.ut
        ut.assertListEqual(self._rows, rows)

    @fork_exec
    def print(self):
        print(self._rows)


class TidbConnection:
    def __init__(self, tk: TestKit, *, conn=None):
        self._tk = tk
        self._conn = conn

    @fork_exec(return_class=Rows)
    def query(self, sql, *args):
        sql = sql.format(*args)
        self._tk.record_path(f'[query] {sql}')
        with self._conn.cursor() as cur:
            cur.execute(sql)
            return Rows(self._tk, rows=cur.fetchall())


class TidbTestKit:
    def __init__(self, tk: TestKit):
        self._tk = tk

    @fork_exec(return_class=TidbConnection)
    def connect(self, *, host='localhost', port=4000, database='test', user=None, **kwargs):
        self.record_path(f'[connect] host: {host}, port: {port}, database: {database}, user: {user}')
        conn = mysql.connector.connect(host=host, port=port, database=database, user=user, **kwargs)
        self._tk.defer(lambda: conn.close())
        return TidbConnection(self._tk, conn=conn)

    def __getattr__(self, item):
        if not hasattr(self._tk, item):
            raise AttributeError(f"'{self.__class__.__name__}' object object has no attribute '{item}'")
        return getattr(self._tk, item)


def tidb_testkit() -> TidbTestKit:
    return TidbTestKit(testkit())
