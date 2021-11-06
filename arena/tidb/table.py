from __future__ import annotations

import io
import typing
from dataclasses import dataclass
from typing import Iterator

from arena.core.fork import RecursionForker, IterableForker, ForkContext, Forker, RecursionEntityBuilder
from .column import TableColumnsForker, TableColumns
from .util import AutoIDAllocator


@dataclass(frozen=True)
class TemporaryTableType:
    type: typing.Optional[str]
    commit: typing.Optional[str]
    points: int


class TemporaryTableTypeForker(Forker):
    TYPES = (
        None,
        TemporaryTableType('TEMPORARY', commit=None, points=1),
        TemporaryTableType('GLOBAL TEMPORARY', commit='ON COMMIT DELETE ROWS', points=1)
    )

    def __init__(self, for_table=False):
        self._for_table = for_table

    def do_fork(self, *, ctx: ForkContext) -> Iterator[typing.Tuple[ForkContext, typing.Any]]:
        tp_iter = map(lambda tp: (ctx, tp), iter(self.TYPES))
        if not self._for_table:
            return tp_iter

        builder: TableBuilder = ctx.current_recursion.builder
        if builder.max_points is None or builder.points < builder.max_points:
            return tp_iter
        else:
            return iter([(ctx, None)])


class Table:
    def __init__(self, name: str, *, columns: TableColumns, temp_type: TemporaryTableType):
        self._name = name
        self._columns = columns
        self._temp_type = temp_type

    @property
    def name(self):
        return self._name

    @property
    def columns(self):
        return self._columns

    @property
    def temp_type(self):
        return self._temp_type

    @property
    def sql_create(self):
        return self._sql_create()

    def normalized_sql_create(self):
        return self._sql_create(normalize=True)

    def _sql_create(self, *, normalize=False):
        columns = self._columns.columns
        if normalize:
            columns = [c.normalize() for c in columns]

        w = io.StringIO()
        if self.temp_type:
            w.write(f'CREATE {self.temp_type.type} TABLE')
        else:
            w.write('CREATE TABLE')

        w.write(f' `{self.name}` (\n  ')
        w.write(',\n  '.join([c.stmt for c in columns]))
        w.write('\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin')
        if self.temp_type and self.temp_type.commit:
            w.write(' ')
            w.write(self.temp_type.commit)
        return w.getvalue()


class TableBuilder(RecursionEntityBuilder):
    def __init__(self, forker: TableForker):
        self._points = 0
        self._max_points = forker.max_points
        self._forker = forker

    @property
    def points(self):
        return self._points

    @property
    def max_points(self):
        return self._max_points

    def update(self, entity) -> RecursionEntityBuilder:
        if not hasattr(entity, 'points'):
            return self

        builder = TableBuilder(self._forker)
        builder._points = self._points + entity.points
        return builder

    def build(self, ctx: ForkContext) -> typing.Tuple[ForkContext, typing.Any]:
        temp_type = None
        columns = None

        for item in ctx.current_recursion.iter_stack_reverse():
            if isinstance(item, TableColumns):
                columns = item
            elif isinstance(item, TemporaryTableType):
                temp_type = item

        entity = Table(
            self._forker.next_tbl_name(),
            columns=columns,
            temp_type=temp_type,
        )
        return ctx.set_var(self._forker.entity_ctx_var, entity), entity


class TableForker(Forker):
    _ID_ALLOCATOR = AutoIDAllocator()

    def __init__(self, *, name_prefix=None, max_points=None):
        self._id = self._ID_ALLOCATOR.alloc()
        self._name_prefix = name_prefix or f'tbl_{self._id}_'
        self._entity_idx = 0
        self._var = f'tb_{self._id}'
        self._columns = TableColumnsForker()
        self._temp_type = TemporaryTableTypeForker(for_table=True)
        self._max_points = max_points
        self._invoked = False

    @property
    def name(self) -> Forker:
        return self._attr_for_forked_table(lambda _, tb: tb.name, "name")

    @property
    def sql_create(self) -> Forker:
        return self._attr_for_forked_table(lambda _, tb: tb.sql_create, "sql_create")

    @property
    def entity_ctx_var(self):
        return self._var

    @property
    def max_points(self):
        return self._max_points

    def normalized_sql_create(self, *args, **kwargs):
        return self._attr_for_forked_table(lambda _, tb: tb.normalized_sql_create(*args, **kwargs), "normalized_sql_create")

    def do_fork(self, *, ctx: ForkContext) -> Iterator[typing.Tuple[ForkContext, typing.Any]]:
        if self._invoked:
            raise RuntimeError('already invoked')

        return RecursionForker(
            forkers=[self._columns, self._temp_type],
            builder=TableBuilder(self),
            desc=str(self)
        ).do_fork(ctx=ctx)

    def next_tbl_name(self):
        self._entity_idx += 1
        return f'{self._name_prefix}{self._entity_idx}'

    def _attr_for_forked_table(self, func, func_name) -> Forker:
        def _map(ctx: ForkContext, _):
            forked = ctx.get_var(self._var)
            return func(ctx, forked)

        return IterableForker([func_name]).map(_map, desc=f'{str(self)}.{func_name}')

    def __str__(self):
        return "Table"