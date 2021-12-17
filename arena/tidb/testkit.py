from __future__ import annotations

import mysql.connector

from arena.core.testkit import TestKit, testkit, fork_exec


class ResultSet:
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
    def __init__(self, tk: TestKit, *, conn=None, conn_id=None):
        self._tk = tk
        self._conn = conn
        self._conn_id = conn_id

    @fork_exec(return_class=ResultSet)
    def execute(self, sql, *args, fetch_rs=False):
        sql = sql.format(*args)
        self._tk.record_path(f'sql@conn#{self._conn_id}', sql)
        with self._conn.cursor() as cur:
            cur.execute(sql)
            if fetch_rs:
                return ResultSet(self._tk, rows=cur.fetchall())

    def query(self, sql, *args):
        return self.execute(sql, *args, fetch_rs=True)


class TidbTestKit:
    def __init__(self, tk: TestKit):
        self._tk = tk
        self._last_conn_id = 0

    @fork_exec(return_class=TidbConnection)
    def connect(self, *, host='localhost', port=4000, database='test', user=None, password=None, **kwargs):
        self._last_conn_id += 1
        self._tk.record_path(f'new_conn#{self._last_conn_id}',
                             f'host: {host}, port: {port}, database: {database}, user: {user}, '
                             f'password: {"*yes*" if password else "N/A"}')
        conn = mysql.connector.connect(host=host, port=port, database=database, user=user, password=password, **kwargs)
        self._tk.defer(lambda: conn.close())
        return TidbConnection(self._tk, conn=conn, conn_id=self._last_conn_id)

    def __getattr__(self, item):
        if not hasattr(self._tk, item):
            raise AttributeError(f"'{self.__class__.__name__}' object object has no attribute '{item}'")
        return getattr(self._tk, item)


def tidb_testkit() -> TidbTestKit:
    tk = testkit()
    tidb_tk = tk.state.get('tidb_tk')
    if tidb_tk is None:
        tidb_tk = TidbTestKit(testkit())
        tk.state['tidb_tk'] = tidb_tk
    return tidb_tk
