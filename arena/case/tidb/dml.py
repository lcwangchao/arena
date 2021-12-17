import unittest

from arena.core.testkit import fork_test
from arena.tidb.testkit import tidb_testkit


class QueryTest(unittest.TestCase):
    @fork_test
    def test_query(self):
        tk = tidb_testkit()
        table_name = tk.fork_enum("t")
        conn = tk.connect(user='root')
        conn.query("select * from {}", table_name).check([])
