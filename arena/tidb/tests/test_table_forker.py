import unittest

from arena.core.fork import *
from arena.tidb.table import TableForker


class TestTableForker(unittest.TestCase):
    def test_context_table(self):
        context = ForkContext()
        forker = TableForker()
        one_table_forker = forker.context_table()
        for item in forker.do_fork(context):
            table = one_table_forker.do_fork(item.context).collect_values()
            self.assertEqual(1, len(table))
            table = table[0]
            self.assertIs(item.value, table)

            name = one_table_forker.name.do_fork(item.context).collect_values()
            self.assertListEqual(name, [item.value.name])
