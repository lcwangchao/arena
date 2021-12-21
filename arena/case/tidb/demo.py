import unittest

from arena.core.testkit import fork_test
from arena.tidb.testkit import tidb_testkit, TidbConnection


class TestTiDB(unittest.TestCase):
    @fork_test(debug=True)
    def test_demo(self):
        tk = tidb_testkit()
        conn: TidbConnection = tk.connect(host='localhost', port=4000, user='root', database='test')
        conn.exec_sql('drop table if exists t')
        conn.exec_sql('create table t (id int)')
        tk.defer(conn.exec_sql, 'drop table if exists t')

        v = tk.pick_range(0, 2)
        conn.exec_sql(tk.format('insert into t values({})', v))
        conn.query('select * from t').check([(v,)])
