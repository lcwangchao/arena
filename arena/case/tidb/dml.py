import unittest

from arena.core.testkit import fork_test
from arena.tidb.testkit import tidb_testkit


class QueryTest(unittest.TestCase):
    @fork_test
    def test_insert_and_select(self):
        tk = tidb_testkit()
        table_name = tk.pick_enum("ta", "tb")
        pk = tk.pick_enum(1, 3, 5)
        tk.name = tk.format('table: {}, pk: {}', table_name, pk)
        tk.print(tk.name)

        conn = tk.connect(user='root')
        conn.execute(tk.format('drop table if exists {}', table_name))
        conn.execute(tk.format('create table {} (id int primary key)', table_name))
        conn.execute(tk.format('insert into {} values(%s)', table_name), pk)
        conn.query(tk.format('select * from {}', table_name)).check([(pk,)])
