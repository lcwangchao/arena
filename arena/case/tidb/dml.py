import unittest

from arena.core.testkit import fork_test
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
        stale_read = tk.pick_bool()
        pessimistic = tk.if_(tk.and_(explicit_txn, tk.not_(stale_read)))\
            .then(lambda: tk.pick_bool())\
            .else_return(False)\
            .end(evaluate_safe=True)
        read_committed = tk.pick_bool()
        rollback = tk.if_(tk.and_(explicit_txn, stale_read)) \
            .then(lambda: tk.pick_bool()) \
            .elif_return(tk.and_(explicit_txn, tk.not_(pessimistic)), True) \
            .else_then(lambda: tk.pick_bool()) \
            .end(evaluate_safe=True)
        tk.set_name(tk.format('ExplicateTxn: {}, Pessimistic: {}, Prepared: {}, '
                              'StaleRead: {}, RC: {}, Rollback: {}',
                              explicit_txn, pessimistic, prepare, stale_read, read_committed, rollback))

        # prepare data
        conn: TidbConnection = tk.connect(user='root')
        tk.if_(read_committed).then(conn.exec_sql, "set tx_isolation = 'READ-COMMITTED'").end()
        conn.exec_sql('drop table if exists t1')
        conn.exec_sql('create table t1 (id int primary key, v int)')
        tk.defer(conn.exec_sql, 'drop table if exists t1')
        conn.exec_sql('insert into t1 values(1, 10)')
        conn.exec_sql('do sleep(0.1)')
        conn.exec_sql('set @a=now(6)')
        conn.exec_sql('do sleep(0.1)')
        conn.exec_sql('update t1 set v=20 where id=1')

        conn2: TidbConnection = tk.connect(user='root')

        read_ts = tk.if_(stale_read)\
            .then_return('@a')\
            .else_return('')\
            .end(evaluate_safe=True)

        as_of_in_sql = tk.if_(tk.and_(stale_read, tk.not_(explicit_txn)))\
            .then_return('as of timestamp @a')\
            .else_return('')\
            .end(evaluate_safe=True)

        rs1 = tk.if_(stale_read)\
            .then_return([(1, 10)])\
            .elif_return(tk.or_(tk.not_(explicit_txn), tk.and_(read_committed, pessimistic)), [(1, 30)]) \
            .else_return([(1, 20)])\
            .end(evaluate_safe=True)

        can_update = tk.not_(tk.and_(explicit_txn, stale_read))
        rs2 = tk.if_(tk.and_(explicit_txn, tk.not_(pessimistic)))\
            .then_return([(1, 20)])\
            .else_return([(1, 30)])\
            .end()

        self.may_begin_txn(tk, conn, explicit=explicit_txn, read_ts=read_ts, pessimistic=pessimistic)
        conn2.exec_sql('update t1 set v=30 where id=1')
        # conn2.exec_sql('alter table t1 add column(v2 int)')
        conn.query(tk.format('select * from t1 {} where id = 1', as_of_in_sql), prepared=prepare).check(rs1)
        tk.if_(can_update)\
            .then(lambda: conn.query('select * from t1 where id = 1 for update', prepared=prepare).check(rs2))\
            .end()
        tk.if_(can_update)\
            .then(conn.exec_sql, 'update t1 set v=40 where id=1')\
            .end()
        self.may_finish_txn(tk, conn, explicit=explicit_txn, rollback=rollback)

        tk.if_(tk.and_(tk.not_(rollback), can_update)) \
            .then(lambda: conn.query('select * from t1 where id = 1', prepared=prepare).check([(1, 40)])) \
            .end()

    @classmethod
    def may_begin_txn(cls, tk, conn, *, explicit, read_ts, pessimistic):
        sql = tk.if_not(explicit) \
            .then_return(None) \
            .elif_then(read_ts, 'start transaction read only as of timestamp {}'.format, read_ts) \
            .elif_return(pessimistic, 'begin pessimistic') \
            .else_return('begin optimistic') \
            .end(evaluate_safe=True)

        tk.if_(sql).then(conn.exec_sql, sql).end()

    @classmethod
    def may_finish_txn(cls, tk, conn, *, explicit, rollback):
        tk.if_not(explicit) \
            .then(lambda: None) \
            .elif_then(rollback, conn.exec_sql, 'rollback') \
            .else_then(conn.exec_sql, 'commit') \
            .end()
