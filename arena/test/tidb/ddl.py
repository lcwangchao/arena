from .testkit import tidb_test, TestKit

__all__ = ()


@tidb_test
def test_create_table(tk: TestKit):
    tb = tk.must_create_table()
    tk.must_query('show create table {}', tb.name).check([(tb.name, tb.normalized_sql_create())], "show create table")
    tk.must_query('show tables').check([(tb.name,)])
