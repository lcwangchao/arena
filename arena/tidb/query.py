from __future__ import annotations

import typing

from arena.core.fork import Forker, ForkContext, RecursionForker, to_forker, combine_forkers

from .execute import Execute, CaseExecContext
from .util import AutoIDAllocator


class ResultSetEntity:
    def __init__(self, *, rows):
        self._rows = rows

    @property
    def rows(self):
        return self._rows


class ResultSet:
    def __init__(self, *, var, tk):
        self._var = var
        self._tk = tk

    @property
    def var(self):
        return self._var

    def check(self, rows, msg=None):
        row_forkers = []
        for row in rows:
            col_forkers = []
            for col in row:
                if not isinstance(col, Forker):
                    col = to_forker(col)
                col_forkers.append(col)
            row_forkers.append(combine_forkers(*col_forkers,
                                               rtype='tuple',
                                               desc=f'check row {[str(f) for f in col_forkers]}'))

        forker = combine_forkers(*row_forkers,
                                 desc=f'check rs {[str(r) for r in row_forkers]}')

        def _map(_, expected_rows):
            def _func(ectx: CaseExecContext):
                log = '[assert] check sql rs'
                if msg:
                    log += f': {msg}'
                ectx.append_log(log)
                rs = ectx.vars.get(self._var)
                ectx.t.assertEqual(expected_rows, rs.rows)
            return Execute(func=_func)

        return self._tk.fork(forker.map(_map))


class Query(RecursionForker):
    _ID_ALLOCATOR = AutoIDAllocator()

    def __init__(self, *, sql, tk=None, args=None, rs=False):
        super().__init__(
            forkers=[to_forker(sql)] + ([to_forker(arg) for arg in args] if args else []),
            build=self._build,
            desc=f"SQL '{str(sql)}'"
        )
        self._id = self._ID_ALLOCATOR.alloc()
        self._rs = ResultSet(var=f'q_{self._id}_rs', tk=tk) if rs else None

    @property
    def rs(self):
        return self._rs

    def _build(self, ctx: ForkContext) -> typing.Tuple[ForkContext, Execute]:
        items = ctx.current_recursion.stack_items
        sql = items[0]
        args = items[1:]

        def _func(ectx: CaseExecContext):
            with ectx.conn.cursor() as cur:
                full_sql = sql.format(*args)
                log_msg = f'[sql] {self._sql_for_print(full_sql)}'
                ectx.append_log(log_msg)
                cur.execute(full_sql)
                rows = cur.fetchall()
                if self._rs:
                    ectx.vars[self._rs.var] = ResultSetEntity(rows=rows)

        return ctx, Execute(_func)

    @staticmethod
    def _sql_for_print(sql):
        sql = ' '.join(sql.split()).replace(' )', ')').replace('( ', '')
        if sql[-1] != ';':
            sql = sql + ';'
        return sql
