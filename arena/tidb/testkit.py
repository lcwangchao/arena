from __future__ import annotations

import typing

import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursorPrepared

from arena.core.testkit import TestKit, testkit, execute


class ResultSet:
    def __init__(self, tk: TestKit, *, rows=None):
        self._tk = tk
        self._rows = rows

    def check(self, rows):
        self._tk.log_path('rs.check', f'expected: {rows}')
        ut = self._tk.ut
        ut.assertListEqual(self._rows, rows)

    @property
    def rows(self):
        return self._rows

    def print(self):
        print(self._rows)


class PreparedStmt:
    def __init__(self, tk, stmt, cursor, conn_id):
        self._tk = tk
        self._stmt = stmt
        self._cursor: MySQLCursorPrepared = cursor
        self._conn_id = conn_id

    def execute(self, *, params=(), multi=False, fetch_rs=False):
        self._tk.log_path(f'exe@conn#{self._conn_id}', self._stmt)
        self._cursor.execute(self._stmt, params=params, multi=multi)
        if fetch_rs:
            return ResultSet(self._tk, rows=self._cursor.fetchall())

    def query(self, *args, **kwargs):
        return self.execute(*args, **kwargs, fetch_rs=True)

    def close(self):
        self._cursor.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class TidbConnection:
    def __init__(self, tk: TestKit, *, conn=None, conn_id=None):
        self._tk = tk
        self._conn: MySQLConnection = conn
        self._conn_id = conn_id

    def exec_sql(self, sql, *, params=(), multi=False, fetch_rs=False, prepared=False):
        sql = sql.strip()
        if sql[-1] != ';':
            sql += ';'
        msg = sql
        if params:
            msg = '{} ({})'.format(sql, ', '.join([str(p) for p in params]))

        options = []
        if prepared:
            options.append('prepared=True')
        if multi:
            options.append('params=True')
        if options:
            msg += f" [{','.join(options)}]"

        self._tk.log_path(f'sql@conn#{self._conn_id}', msg)
        with self._conn.cursor(prepared=prepared) as cur:
            cur.execute(sql, params=params, multi=multi)
            if fetch_rs:
                return ResultSet(self._tk, rows=cur.fetchall())

    def query(self, *args, **kwargs):
        return self.exec_sql(*args, **kwargs, fetch_rs=True)

    def prepare(self, stmt) -> PreparedStmt:
        cursor = self._conn.cursor(prepared=True)
        try:
            self._tk.log_path(f'pre@conn#{self._conn_id}', stmt)
            cursor.execute(stmt)
            cursor.fetchall()
            return PreparedStmt(self._tk, stmt, cursor, self._conn_id)
        except Exception:
            cursor.close()
            raise

    def close(self):
        self._tk.log_path(f'sql@conn#{self._conn_id}', "close connection")
        self._conn.close()


class TidbTestKit:
    def __init__(self, tk: TestKit):
        self._tk = tk
        self._last_conn_id = 0

    @execute
    def connect(self, *, host='localhost', port=4000, database='test',
                user=None, password=None, **kwargs) -> TidbConnection:
        self._last_conn_id += 1
        self._tk.log_path(f'new_conn#{self._last_conn_id}',
                          f'host: {host}, port: {port}, database: {database}, user: {user}, '
                          f'password: {"*yes*" if password else "N/A"}')
        conn = mysql.connector.connect(host=host, port=port, database=database, user=user, password=password, **kwargs)
        tidb_conn = TidbConnection(self._tk, conn=conn, conn_id=self._last_conn_id)
        self._tk.defer(lambda: tidb_conn.close())
        conn.autocommit = True
        return tidb_conn

    def __getattr__(self, item):
        if not hasattr(self._tk, item):
            raise AttributeError(f"'{self.__class__.__name__}' object object has no attribute '{item}'")
        return getattr(self._tk, item)


def tidb_testkit() -> typing.Union[TestKit, TidbTestKit]:
    tk = testkit()
    tidb_tk = tk.state.get('tidb_tk')
    if tidb_tk is None:
        tidb_tk = TidbTestKit(testkit())
        tk.state['tidb_tk'] = tidb_tk
    return tidb_tk
