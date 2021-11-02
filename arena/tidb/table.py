from __future__ import annotations

import io
import typing
from dataclasses import dataclass
from typing import Iterator

from arena.core.fork import combine_forkers, IterableForker, ForkContext, RecursionForkState, Forker
from .column import TableColumnsForker, TableColumns
from .util import AutoIDAllocator


@dataclass(frozen=True)
class TemporaryTableType:
    type: typing.Optional[str]
    commit: typing.Optional[str]

    @staticmethod
    def forker() -> Forker:
        return IterableForker([
            None,
            TemporaryTableType('TEMPORARY', commit=None),
            TemporaryTableType('GLOBAL TEMPORARY', commit='ON COMMIT DELETE ROWS')
        ])


class TableEntity:
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


class Table(Forker):
    _ID_ALLOCATOR = AutoIDAllocator()

    def __init__(self):
        self._id = self._ID_ALLOCATOR.alloc()
        self._entity_idx = 0
        self._var = f'tb_{self._id}'
        self._columns = TableColumnsForker()
        self._temp_type = TemporaryTableType.forker()

    @property
    def name(self) -> Forker:
        return self._attr_for_forked_table(lambda _, tb: tb.name)

    @property
    def sql_create(self) -> Forker:
        return self._attr_for_forked_table(lambda _, tb: tb.sql_create)

    def normalized_sql_create(self, *args, **kwargs):
        return self._attr_for_forked_table(lambda _, tb: tb.normalized_sql_create(*args, **kwargs))

    def do_fork(self, *, ctx: ForkContext) -> Iterator[typing.Tuple[ForkContext, typing.Any]]:
        return combine_forkers(self._columns, self._temp_type)\
            .map(self._build_table_entity, update_ctx=self._update_table_ctx)\
            .do_fork(ctx=ctx)

    def _build_table_entity(self, _, items: typing.List):
        self._entity_idx += 1
        tbl_name = f'tbl_{self._id}_{self._entity_idx}'
        temp_type = None
        columns = None

        for item in items:
            if isinstance(item, TableColumns):
                columns = item
            elif isinstance(item, TemporaryTableType):
                temp_type = item

        return TableEntity(
            tbl_name,
            columns=columns,
            temp_type=temp_type,
        )

    def _update_table_ctx(self, ctx: ForkContext, entity: TableEntity):
        return ctx.new_context_with_vars({self._var: entity})

    def _attr_for_forked_table(self, func) -> Forker:
        def _map(ctx: ForkContext, _):
            forked = ctx.get_var(self._var)
            return func(ctx, forked)

        return IterableForker([None]).map(_map)
