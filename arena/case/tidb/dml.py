import unittest

from arena.core.testkit import fork_test, execute
from arena.tidb.testkit import tidb_testkit, TidbConnection


class QueryTest(unittest.TestCase):
    @fork_test
    def test_insert_and_select(self):
        tk = tidb_testkit()

        table_name = tk.pick_enum("ta", "tb")
        pk = tk.pick_enum(1, 3, 5)

        tk.set_name(tk.format('table: {}, pk: {}', table_name, pk))
        tk.print(tk.name)

        conn: TidbConnection = tk.connect(user='root')
        conn.exec_sql(tk.format('drop table if exists {}', table_name))
        conn.exec_sql(tk.format('create table {} (id int primary key)', table_name))
        conn.exec_sql(tk.format('insert into {} values(%s)', table_name), params=(pk,))
        conn.query(tk.format('select * from {}', table_name)).check([(pk,)])
        with conn.prepare(tk.format('select * from {} where id = ?', table_name)) as prepared:
            prepared.query(params=(pk,)).check([(pk,)])


class TxnTest(unittest.TestCase):
    @fork_test(debug=True)
    def test_txn(self):
        tk = tidb_testkit()

        explicit_txn = tk.pick_bool()
        prepare = tk.pick_bool()
        tk.set_name(tk.format('ExplicateTxn({}), Prepared({})', explicit_txn, prepare))

        # prepare data
        conn: TidbConnection = tk.connect(user='root')
        conn.exec_sql('drop table if exists t1')
        conn.exec_sql('create table t1 (id int primary key, v int)')
        tk.defer(conn.exec_sql, 'drop table if exists t1')
        conn.exec_sql('insert into t1 values(1, 10)')

        # run
        tk.execute_or_not(explicit_txn, conn.exec_sql, 'begin')
        conn.query('select * from t1 where id = 1', prepared=prepare).check([(1, 10)])
        conn.exec_sql('insert into t1 values(2, 10)')
        tk.execute_or_not(explicit_txn, conn.exec_sql, 'commit')
