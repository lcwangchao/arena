import unittest

import mysql.connector

from arena.core.event_driven import *
from arena.tidb.testkit import *
from arena.core.fork import *


class SimpleTxnTestState(EventDrivenState):
    def __init__(self, ut: unittest.TestCase, connect):
        self.ut = ut
        self.connect = connect
        self.conn = None

        self.in_txn = False
        self.inserted = False
        self.dirty = False

    def setup(self):
        self.conn = self.connect()
        self.conn.exec_sql('use test')
        self.conn.exec_sql('drop table if exists t')
        self.conn.exec_sql('create table t(id int primary key)')

    def close(self):
        self.conn.exec_sql('drop table if exists t')
        self.conn = None

    def signature(self):
        return (
            self.in_txn,
            self.inserted,
            self.dirty
        )

    @action
    def start_transaction(self):
        self.conn.exec_sql('start transaction')
        self.dirty = False
        self.in_txn = True

    @action
    def commit(self):
        self.conn.exec_sql('commit')
        self.dirty = False
        self.in_txn = False

    @action
    def rollback(self):
        self.conn.exec_sql('rollback')
        if self.dirty:
            self.inserted = False
        self.dirty = False
        self.in_txn = False

    @action
    def do_insert(self):
        try:
            self.conn.exec_sql('insert into t values(1)')
            self.ut.assertFalse(self.inserted)
            self.inserted = True
            if self.in_txn:
                self.dirty = True
        except mysql.connector.DatabaseError as err:
            if self.inserted:
                self.ut.assertEqual(1062, err.errno)
            else:
                raise

    @action
    def do_select(self):
        rows = self.conn.query('select * from t').rows
        if self.inserted:
            self.ut.assertEqual([(1,)], rows)
        else:
            self.ut.assertEqual([], rows)


class TestTiDB(unittest.TestCase):
    @fork_test(debug=True)
    def test_demo(self):
        tk = tidb_testkit()
        conn: TidbConnection = tk.connect(host='localhost', port=4000, user='root', database='test')
        conn.exec_sql('drop table if exists t')
        conn.exec_sql('create table t (id int)')
        tk.defer(conn.exec_sql, 'drop table if exists t')

        v = yield tk.pick_range(0, 2)
        conn.exec_sql(f'insert into t values({v})')
        conn.query('select * from t').check([(v,)])

    @fork_test(debug=True)
    def test_simple_txn(self):
        tk = tidb_testkit()
        yield FlatForker(SimpleTxnTestState.run(self, lambda: tk.connect(host='127.0.0.1', port=4000, user='root')))
