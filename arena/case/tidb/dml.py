import unittest

from mysql.connector import DatabaseError

from arena.core.testkit import fork_test
from arena.tidb.testkit import tidb_testkit, TidbConnection


class BaseTxnTest(unittest.TestCase):
    def run(self, result=None):
        result.failfast = True
        super().run(result=result)


class TxnTest(BaseTxnTest):
    @fork_test
    def test_stale_read(self):
        tk = tidb_testkit()

        stale_ts = '@a'
        prepare = tk.pick_bool()
        # stale read in read committed has some bug, temporary skip it
        read_committed = tk.pick(False)
        autocommit = tk.pick_bool()
        # tidb_read_staleness's behavior is strange, temporary skip it
        set_sys_var = tk.pick_enum('', 'set tx_read_ts=@a')
        start_txn = tk.if_(set_sys_var) \
            .then(lambda: tk.pick_enum('', 'begin', 'begin pessimistic', 'begin optimistic')) \
            .else_then(lambda: tk.pick_enum('', f'start transaction read only as of timestamp {stale_ts}')) \
            .end(evaluate_safe=True)
        end_txn = tk.if_(start_txn)\
            .then(lambda: tk.pick_enum('commit', 'rollback')) \
            .else_then(lambda: tk.pick_enum('', 'commit', 'rollback')) \
            .end(evaluate_safe=True)

        sql = tk.if_(tk.or_(set_sys_var, start_txn)) \
            .then_return('select * from t1 where id=1') \
            .else_return(f'select * from t1 as of timestamp {stale_ts} where id=1') \
            .end()

        tk.set_name(tk.format('sys_var: {}, start_txn: {}, end_txn: {}, '
                              'prepare: {}, rc: {}, autocommit: {}',
                              set_sys_var, start_txn, end_txn, prepare, read_committed, autocommit))

        # prepare data
        conn: TidbConnection = tk.connect(user='root', port=4001)
        tk.if_(read_committed).then(conn.exec_sql, "set tx_isolation = 'READ-COMMITTED'").end()

        conn2: TidbConnection = tk.connect(user='root', port=4001)

        conn.exec_sql('set autocommit=1')
        conn.exec_sql('drop table if exists t1')
        conn.exec_sql('create table t1 (id int primary key, v int)')
        tk.defer(conn.exec_sql, 'drop table if exists t1')
        conn.exec_sql('insert into t1 values(1, 10)')
        tk.if_(set_sys_var.find('tidb_read_staleness') != -1, skip_safe_check=True) \
            .then(conn.exec_sql, 'do sleep(1.2)') \
            .else_then(conn.exec_sql, 'do sleep(0.1)') \
            .end()
        conn.exec_sql('set @a=now(6)')
        conn.exec_sql('do sleep(0.1)')
        conn.exec_sql('update t1 set v=20 where id=1')

        # test one statement
        tk.if_(set_sys_var).then(conn.exec_sql, set_sys_var).end()
        tk.if_not(autocommit).then(conn.exec_sql, 'set autocommit=0').end()
        tk.if_(start_txn).then(conn.exec_sql, start_txn).end()
        conn2.exec_sql('alter table t1 add column (c int)')
        conn.query(sql, prepared=prepare).check([(1, 10)])
        tk.if_(end_txn).then(conn.exec_sql, end_txn).end()
        conn.query('select @@tidb_current_ts').check([('0',)])

        # test prepare stale read statement
        conn.exec_sql('set autocommit=1')
        tk.if_(set_sys_var).then(conn.exec_sql, set_sys_var).end()
        sql = tk.if_(set_sys_var).then_return('select * from t1 where id=?') \
            .else_return(f'select * from t1 as of timestamp {stale_ts} where id=?') \
            .end()
        prepared_stmt = conn.prepare(sql)
        conn.exec_sql("set @txn_read_ts=''")
        prepared_stmt.query(params=(1,)).check([(1, 10)])

        def _assert_fail_for_sys_var():
            conn.exec_sql(set_sys_var)
            with self.assertRaises(DatabaseError):
                prepared_stmt.query(params=(1,))
        tk.if_(set_sys_var).then(_assert_fail_for_sys_var).end()
        conn.exec_sql("set @txn_read_ts=''")

        def _assert_fail_for_txn():
            conn.exec_sql(start_txn)
            with self.assertRaises(DatabaseError):
                prepared_stmt.query(params=(1,))
        tk.if_(start_txn).then(_assert_fail_for_txn).end()
        conn.exec_sql('rollback')

        # test prepare normal statement and execute in stale env
        conn.exec_sql("set tx_read_ts=''")
        prepared_stmt = conn.prepare(f'select * from t1 where id=?')
        tk.if_(set_sys_var).then(conn.exec_sql, set_sys_var).end()
        tk.if_(start_txn).then(conn.exec_sql, start_txn).end()
        with prepared_stmt:
            tk.if_(tk.or_(set_sys_var, start_txn)).then(lambda: prepared_stmt.query(params=(1,)).check([(1, 10)])).end()
