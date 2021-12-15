from .testkit import tidb_test, TestKit

__all__ = ()


@tidb_test
def test_create_table(tk: TestKit):
    tb = tk.declare_table(name_prefix='t1_', max_points=1)
    tk.print("\n--------------\n{}", tb.sql_create)
    # tk.must_exec(tb.sql_create)
    # tk.must_query('show create table {}', tb.name).check([(tb.name, tb.normalized_sql_create)], "show create table")
