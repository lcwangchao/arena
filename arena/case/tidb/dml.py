import unittest

from arena.core.testkit import fork_test
from arena.tidb.testkit import tidb_testkit


class QueryTest(unittest.TestCase):
    @fork_test
    def test_insert_and_select(self):
        tk = tidb_testkit()
        table_name = tk.fork_enum("ta", "tb")
        pk = tk.fork_enum(1, 3, 5)
        tk.set_fork_name('table: {}, pk: {}', table_name, pk)
        # tk.print('table: {}, pk: {}', table_name, pk)

        conn = tk.connect(user='root')
        conn.execute('drop table if exists {}', table_name)
        conn.execute('create table {} (id int primary key)', table_name)
        conn.execute('insert into {} value({})', table_name, pk)
        conn.query('select * from {}', table_name).check([(pk,)])