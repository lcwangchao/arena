from __future__ import annotations

import mysql.connector

from arena.core.testkit import TestKit, testkit, execute


class ResultSet:
    def __init__(self, tk: TestKit, *, rows=None):
        self._tk = tk
        self._rows = rows

    def check(self, rows):
        ut = self._tk.ut
        ut.assertListEqual(self._rows, rows)

    def print(self):
        print(self._rows)


class TidbConnection:
    def __init__(self, tk: TestKit, *, conn=None, conn_id=None):
        self._tk = tk
        self._conn = conn
        self._conn_id = conn_id

    def execute(self, sql, *params, multi=False, fetch_rs=False):
        msg = sql
        if params:
            msg = '{}, ({})'.format(sql, ', '.join([str(p) for p in params]))
        if multi:
            msg += " multi=True"

        self._tk.log_path(f'sql@conn#{self._conn_id}', msg)
        with self._conn.cursor() as cur:
            cur.execute(sql, params=params, multi=multi)
            if fetch_rs:
                return ResultSet(self._tk, rows=cur.fetchall())

    def query(self, *args, **kwargs):
        return self.execute(*args, **kwargs, fetch_rs=True)


class TidbTestKit(TestKit):
    def __init__(self, tk: TestKit):
        super().__init__(tk.ut)
        self._tk = tk
        self._last_conn_id = 0

    @execute
    def connect(self, *, host='localhost', port=4000, database='test', user=None, password=None, **kwargs):
        self._last_conn_id += 1
        self._tk.log_path(f'new_conn#{self._last_conn_id}',
                          f'host: {host}, port: {port}, database: {database}, user: {user}, '
                          f'password: {"*yes*" if password else "N/A"}')
        conn = mysql.connector.connect(host=host, port=port, database=database, user=user, password=password, **kwargs)
        self._tk.defer(lambda: conn.close())
        return TidbConnection(self._tk, conn=conn, conn_id=self._last_conn_id)

    def __getattr__(self, item):
        if not hasattr(self._tk, item):
            raise AttributeError(f"'{self.__class__.__name__}' object object has no attribute '{item}'")
        return getattr(self._tk, item)

    def pick(self, *args, **kwargs):
        return self._tk.pick(*args, **kwargs)

    def execute(self, *args, **kwargs):
        return self._tk.execute(*args, **kwargs)


def tidb_testkit() -> TidbTestKit:
    tk = testkit()
    tidb_tk = tk.state.get('tidb_tk')
    if tidb_tk is None:
        tidb_tk = TidbTestKit(testkit())
        tk.state['tidb_tk'] = tidb_tk
    return tidb_tk
