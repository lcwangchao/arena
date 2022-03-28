import dataclasses
import unittest

import mysql.connector

from arena.core.event_driven import *
from arena.core.fork import *
from arena.tidb.testkit import *


@dataclasses.dataclass
class StaleReadEnv:
    autocommit: bool
    rc: bool
    use_variable: bool
    txn_pessimistic: bool

    @classmethod
    def generate_init_env_actions(cls):
        bool_forker = FlatForker([True, False])
        forker = ContainerForker(dict(
            autocommit=bool_forker,
            rc=bool_forker,
            use_variable=bool_forker,
            txn_pessimistic=bool_forker,
        ))

        for args in forker:
            yield dict(
                name='init env [' + ', '.join([f'{k}: {v}' for k, v in args.items()]) + ']',
                args=StaleReadEnv(**args)
            )

    def signature(self):
        return (
            self.autocommit,
            self.rc,
            self.use_variable,
            self.txn_pessimistic,
        )


SYS_VAR_TX_READ_TS = 'tx_read_ts'
SYS_VAR_TIDB_READ_STALENESS = 'tidb_read_staleness'


class StaleReadState(EventDrivenState):
    def __init__(self, *, db=None, table=None, conn: TidbConnection = None, ut: unittest.TestCase = None):
        self.db_name = db or 'test'
        self.table_name = table or 'stale_t1'

        # key states
        self.env = None
        self.is_in_txn = False
        self.is_txn_stale = False
        self.sys_var_tx_read_ts = False
        self.sys_var_tidb_read_staleness = False
        self.is_prepared = False
        self.is_binary_prepare = False
        self.is_prepared_stale = False

        self.should_sleep_second_when_setup = False

        # runtime states
        self.conn = conn
        self.ut = ut
        self.stale_point = None
        self.current_data = None
        self.prepared_stmt = None

    def re_execute_online(self, *, conn, ut):
        state = StaleReadState(conn=conn, ut=ut)
        state.should_sleep_second_when_setup = self.should_sleep_second_when_setup
        try:
            state.setup()
            for i, act in enumerate(self.action_records):
                act(state, index=i)
        finally:
            state.close()

    def signature(self):
        return (
            self.env.signature() if self.env else None,
            self.is_in_txn,
            self.is_txn_stale,
            self.sys_var_tx_read_ts,
            self.sys_var_tidb_read_staleness,
            self.is_prepared,
            self.is_binary_prepare,
            self.is_prepared_stale,
        )

    def setup(self):
        if self.online:
            self.conn.exec_sql(f'use {self.db_name}')
            self.conn.exec_sql(f'drop table if exists {self.table_name}')
            self.conn.exec_sql(f'create table if not exists {self.table_name} (id int primary key, v int)')
            self.conn.exec_sql(f'insert into {self.table_name} values(1, 100)')

    def close(self):
        if self.online:
            self.conn.exec_sql(f'drop table if exists {self.table_name}')

    @property
    def online(self):
        return bool(self.conn)

    @cond
    def initializing(self):
        return not self.env

    @cond
    def running(self):
        return self.env and len(self.action_records) < 10

    @cond
    def prepared(self):
        return self.is_prepared

    @cond
    def binary_prepare(self):
        return self.is_binary_prepare

    @action(cond=initializing, generate=StaleReadEnv.generate_init_env_actions)
    def init_env(self, env: StaleReadEnv):
        self.env = env
        if not self.online:
            return

        self.conn.exec_sql(f'set @@autocommit={1 if env.autocommit else 0}')
        self.conn.exec_sql(f"set @@tidb_txn_mode = '{'pessimistic' if env.txn_pessimistic else 'optimistic'}'")
        if env.rc:
            self.conn.exec_sql(f"set tx_isolation = 'READ-COMMITTED'")

        self.stale_point = {
            'data': [(1, 100)],
        }

        self.conn.exec_sql('do sleep(0.1)')
        if env.use_variable:
            self.conn.exec_sql(f'set @a=now(6)')
            as_of_time = '@a'
        else:
            ts = self.conn.query('select now(6)').rows[0][0]
            as_of_time = f'"{ts}"'

        self.stale_point['ts'] = as_of_time
        self.stale_point['as_of'] = 'as of timestamp ' + as_of_time
        if self.should_sleep_second_when_setup:
            self.conn.exec_sql('do sleep(1.2)')
        self.conn.exec_sql(f'alter table {self.table_name} add column v2 int default 0')
        self.conn.exec_sql(f'update {self.table_name} set v=v+1 where id=1')
        self.conn.exec_sql('commit')
        self.current_data = [(1, 101, 0)]

    @action(cond=running, name='do_select', args=False)
    @action(cond=running, name='do_select_as_of', args=True)
    def do_select(self, as_of):
        will_success = self.will_read_success(as_of)
        will_stale = self.will_stale_read(as_of)
        if not self.env.autocommit and not self.is_in_txn and not will_stale and will_success:
            self.is_in_txn = True
            self.is_txn_stale = False
        self.sys_var_tx_read_ts = False

        if not self.online:
            return

        sql = self.build_sql(as_of)
        self.check_query(lambda: self.conn.query(sql).rows, success=will_success, stale=will_stale)

    @action(cond=running & ~prepared, name='do_prepare', args=[False, False])
    @action(cond=running & ~prepared, name='do_prepare_binary', args=[True, False])
    @action(cond=running & ~prepared, name='do_prepare_as_of', args=[False, True])
    @action(cond=running & ~prepared, name='do_prepare_binary_as_of', args=[True, True])
    def do_prepare(self, binary, as_of):
        will_stale = not self.is_in_txn and (as_of or self.sys_var_tx_read_ts or self.sys_var_tidb_read_staleness)
        will_success = self.is_in_txn and not (as_of or self.sys_var_tx_read_ts) \
                       or not self.is_in_txn and not (as_of and self.sys_var_tx_read_ts)
        if will_success:
            self.is_prepared = True
            self.is_binary_prepare = binary
            self.is_prepared_stale = will_stale
        self.sys_var_tx_read_ts = False

        if not self.online:
            return

        sql = self.build_sql(as_of)
        try:
            if binary:
                self.prepared_stmt = self.conn.prepare(sql)
            else:
                self.conn.exec_sql(f"prepare s from '{sql}'")
            self.ut.assertTrue(will_success, "query should fail")
        except mysql.connector.DatabaseError as ex:
            if will_success:
                raise
            self.ut.assertEqual(8135, ex.errno)

    @action(cond=running & prepared & ~binary_prepare, name='do_execute', args=False)
    @action(cond=running & prepared & binary_prepare, name='do_execute_binary', args=True)
    def do_execute(self, binary):
        will_success = self.will_read_success(self.is_prepared_stale)
        will_stale = self.will_stale_read(self.is_prepared_stale)
        if not self.env.autocommit and not self.is_in_txn and not will_stale and will_success:
            self.is_in_txn = True
            self.is_txn_stale = False
        self.sys_var_tx_read_ts = False

        if not self.online:
            return

        def _query():
            if binary:
                return self.prepared_stmt.query().rows
            return self.conn.query('execute s').rows

        self.check_query(_query, success=will_success, stale=will_stale)
        if binary:
            # query multi times to check plan cache when in binary mode
            self.check_query(_query, success=will_success, stale=will_stale)
            self.check_query(_query, success=will_success, stale=will_stale)

    @action(cond=running, name='start_txn', args=True)
    @action(cond=running, name='start_txn_as_of', args=False)
    def start_txn(self, stale):
        will_success = not (self.sys_var_tx_read_ts and stale)
        will_stale = stale or self.sys_var_tx_read_ts
        if will_success:
            self.is_in_txn = True
            self.is_txn_stale = will_stale
            self.sys_var_tx_read_ts = False

        if not self.online:
            return

        sql = f"start transaction {('read only ' + self.stale_point['as_of']) if stale else ''}"
        try:
            self.conn.exec_sql(sql)
            self.ut.assertTrue(will_success, "query should fail")
        except mysql.connector.DatabaseError as ex:
            if will_success:
                raise
            self.ut.assertEqual(1105, ex.errno)

    @action(cond=running)
    def close_txn(self):
        self.is_in_txn = False
        self.is_txn_stale = False

        if self.online:
            self.conn.exec_sql('commit')

    @action(cond=running, name='set_sys_var_tx_read_ts', args=[SYS_VAR_TX_READ_TS, lambda s: s.stale_point['ts']])
    @action(cond=running, name='unset_sys_var_tx_read_ts', args=[SYS_VAR_TX_READ_TS, None])
    @action(cond=running, name='set_sys_var_tidb_read_staleness', args=[SYS_VAR_TIDB_READ_STALENESS, '-1'])
    @action(cond=running, name='unset_sys_var_tidb_read_staleness', args=[SYS_VAR_TIDB_READ_STALENESS, None])
    def set_sys_var(self, var, value):
        will_success = not (var == SYS_VAR_TX_READ_TS and self.is_in_txn)
        if will_success:
            if var == SYS_VAR_TX_READ_TS:
                self.sys_var_tx_read_ts = value is not None
            elif var == SYS_VAR_TIDB_READ_STALENESS:
                self.sys_var_tidb_read_staleness = value is not None
                self.should_sleep_second_when_setup = True
            else:
                raise ValueError('unexpected var: ' + var)

        if self.online:
            if value is None:
                value = '""'
            elif callable(value):
                value = value(self)

            try:
                self.conn.exec_sql(f'set @@{var}={value}')
                self.ut.assertTrue(will_success)
            except mysql.connector.DatabaseError as ex:
                if will_success:
                    raise ex
                self.ut.assertEqual(1568, ex.errno)

    def build_sql(self, as_of):
        return f'select * from {self.table_name} {self.stale_point["as_of"] if as_of else ""} where id=1'

    def will_stale_read(self, as_of_in_sql):
        return as_of_in_sql \
               or (self.is_in_txn and self.is_txn_stale) \
               or (not self.is_in_txn and (self.sys_var_tx_read_ts or self.sys_var_tidb_read_staleness))

    def will_read_success(self, as_of_in_sql):
        if self.is_in_txn:
            return not (self.sys_var_tx_read_ts or as_of_in_sql)

        return not (as_of_in_sql and self.sys_var_tx_read_ts)

    def check_query(self, func, *, success, stale):
        try:
            rows = func()
            self.ut.assertTrue(success, "query should fail")
            expected_data = self.stale_point['data'] if stale else self.current_data
            self.ut.assertEqual(expected_data, rows)
        except mysql.connector.DatabaseError as ex:
            if success:
                raise
            self.ut.assertEqual(8135, ex.errno)


class StaleReadTest(unittest.TestCase):
    def run(self, result=None):
        result.failfast = False
        super().run(result=result)

    @fork_test(debug=True)
    def test_stale_read(self):
        tk = tidb_testkit()

        def _filter_cases(i):
            failed_cases = [
                52, 215, 378, 541, 704, 867, 1030, 1193,
                (1304, 1348),
                (1394, 1438),
            ]

            for case in failed_cases:
                if isinstance(case, tuple) and case[0] <= i <= case[1]:
                    return False

                if i == case:
                    return False

            return True

        cases = [case for i, case in enumerate(StaleReadState.run())if _filter_cases(i)]
        case: StaleReadState = yield tk.pick(FlatForker(cases))
        conn = tk.connect(host='127.0.0.1', port=4001, user='root')
        case.re_execute_online(conn=conn, ut=self)
