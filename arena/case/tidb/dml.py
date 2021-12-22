import unittest

from mysql.connector import DatabaseError

from arena.tidb.testkit import *


class BaseTxnTest(unittest.TestCase):
    def run(self, result=None):
        result.failfast = True
        super().run(result=result)


class TxnTest(BaseTxnTest):
    @fork_test(debug=True)
    def test_stale_read(self):
        tk = tidb_testkit()

        stale_ts = '@a'
        prepare = yield tk.pick_bool()
        autocommit = yield tk.pick_bool()
        # stale read + read committed + autocommit = 0 has bug. so skip it
        read_committed = (yield tk.pick_bool()) if autocommit else False
        # tidb_read_staleness's behavior is strange, temporary skip it
        sys_var_name, set_sys_var = yield tk.pick_enum(
            (None, None),
            ('tx_read_ts', f'set tx_read_ts={stale_ts}'),
            # ('tidb_read_staleness', 'set tidb_read_staleness=-1')
        )

        if set_sys_var:
            start_txn = yield tk.pick_enum(None, 'begin', 'begin pessimistic', 'begin optimistic')
        else:
            start_txn = yield tk.pick_enum(None, f'start transaction read only as of timestamp {stale_ts}')

        if start_txn:
            end_txn = yield tk.pick_enum('commit', 'rollback')
        else:
            end_txn = yield tk.pick_enum(None, 'commit', 'rollback')

        if set_sys_var or start_txn:
            sql = 'select * from t1 where id=1'
        else:
            sql = f'select * from t1 as of timestamp {stale_ts} where id=1'

        tk.log_path('branch',
                    f'sys_var: {sys_var_name}, start_txn: {start_txn}, end_txn: {end_txn}, '
                    f'prepare: {prepare}, rc: {read_committed}, autocommit: {autocommit}')

        # provision data
        conn: TidbConnection = tk.connect(user='root', port=4001)
        conn2: TidbConnection = tk.connect(user='root', port=4001)

        if read_committed:
            conn.exec_sql("set tx_isolation = 'READ-COMMITTED'")

        conn.exec_sql('set autocommit=1')
        conn.exec_sql('drop table if exists t1')
        conn.exec_sql('create table t1 (id int primary key, v int)')
        tk.defer(conn.exec_sql, 'drop table if exists t1')
        conn.exec_sql('insert into t1 values(1, 10)')
        if sys_var_name == 'tidb_read_staleness':
            conn.exec_sql('do sleep(1.2)')
        else:
            conn.exec_sql('do sleep(0.1)')
        conn.exec_sql('set @a=now(6)')
        conn.exec_sql('do sleep(0.1)')
        conn.exec_sql('update t1 set v=20 where id=1')

        # test one statement
        if set_sys_var:
            conn.exec_sql(set_sys_var)
        if not autocommit:
            conn.exec_sql('set autocommit=0')
        if start_txn:
            conn.exec_sql(start_txn)
        conn2.exec_sql('alter table t1 add column (c int)')
        conn.query(sql, prepared=prepare).check([(1, 10)])
        if end_txn:
            conn.exec_sql(end_txn)
        conn.query('select @@tidb_current_ts').check([('0',)])

        # test prepare stale read statement
        conn.exec_sql('set autocommit=1')
        if sys_var_name:
            conn.exec_sql(set_sys_var)
            prepared_stmt = conn.prepare('select * from t1 where id=?')
        else:
            prepared_stmt = conn.prepare(f'select * from t1 as of timestamp {stale_ts} where id=?')

        # sys var should be consumed by prepare
        conn.query('select * from t1 where id=1').check([(1, 20, None)])
        prepared_stmt.query(params=(1,)).check([(1, 10)])

        if sys_var_name:
            conn.exec_sql(set_sys_var)
            with self.assertRaises(DatabaseError):
                prepared_stmt.query(params=(1,))

        conn.exec_sql("set tx_read_ts=''")
        if start_txn:
            conn.exec_sql(start_txn)
            with self.assertRaises(DatabaseError):
                prepared_stmt.query(params=(1,))
            conn.exec_sql('rollback')

        # test prepare normal statement and execute in stale env
        prepared_stmt = conn.prepare(f'select * from t1 where id=?')
        if sys_var_name:
            conn.exec_sql(set_sys_var)
        if start_txn:
            conn.exec_sql(start_txn)
        with prepared_stmt:
            if sys_var_name or start_txn:
                prepared_stmt.query(params=(1,)).check([(1, 10)])
            else:
                prepared_stmt.query(params=(1,)).check([(1, 20, None)])
