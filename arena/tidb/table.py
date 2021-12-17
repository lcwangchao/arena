from __future__ import annotations

import io

from typing import Optional

from arena.core.fork import *
from .column import TableColumnsForker, TableColumns
from .options import TemporaryTableType, TemporaryTableTypeForker
from .util import AutoIDAllocator, name_generator


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

    @property
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


class TableBuilder(Forker):
    def __init__(self, *, forkers=None, max_points=None):
        self._forkers = forkers
        self._max_points = max_points

        self._table_name: Optional[str] = None
        self._temp_type: Optional[TemporaryTableType] = None
        self._columns: Optional[TableColumns] = None
        self._table_name: Optional[str] = None

    def clone(self) -> TableBuilder:
        builder = TableBuilder()
        builder._forkers = self._forkers
        builder._max_points = self._max_points
        builder._temp_type = self._temp_type
        builder._columns = self._columns
        return builder

    def do_fork(self, context: ForkContext) -> ForkResult[T]:
        if not self._forkers:
            return context.new_fork_result([Table(
                name=self._table_name,
                temp_type=self._temp_type,
                columns=self._columns
            )])

        forker = self._forkers[0]
        if callable(forker):
            kwargs = dict(max_points=self._max_points)
            forker = forker(**kwargs)

        return forker.do_fork(context).map_value(self.to_next_builder)

    def to_next_builder(self, value):
        builder = self.clone()
        builder._forkers = self._forkers[1:]

        if value is None:
            return builder

        if builder._max_points is not None and hasattr(value, 'points'):
            points = getattr(value, 'points')
            if points > builder._max_points:
                points = builder._max_points
            builder._max_points -= points

        if isinstance(value, dict):
            table_name = value.get('table_name')
            if table_name:
                builder._table_name = table_name
        elif isinstance(value, TemporaryTableType):
            builder._temp_type = value
        elif isinstance(value, TableColumns):
            builder._columns = value

        return builder


class TableForker(Forker[Table]):
    _ID_ALLOCATOR = AutoIDAllocator()

    def __init__(self, *, name_prefix=None, max_points=None, **kwargs):
        self._forker_id = self._ID_ALLOCATOR.alloc()
        self._table_name_generator = name_generator(name_prefix or f'tbl_{self._forker_id}_')
        self._context_var = f'table_forker_{self._forker_id}'
        self._seed = TableBuilder(
            forkers=self.children_forkers(kwargs),
            max_points=max_points
        )

    def do_fork(self, context: ForkContext) -> ForkResult[T]:
        def _add_table_to_context(item: ForkItem):
            return item.set_var(self._context_var, item.value)
        return ReactionForker(self._seed).do_fork(context).map(_add_table_to_context)

    def next_tbl_name(self):
        return next(self._table_name_generator)

    def context_table(self) -> Forker[Table]:
        def _map(item: ForkItem):
            table = item.get_var(item.value)
            if table is None:
                raise RuntimeError('Table not found in context')
            return item.context.new_item(table)

        return SingleValueForker(self._context_var).map(_map)

    def __str__(self):
        return "TableForker"

    def children_forkers(self, kwargs):
        children = [
            ('temporary_table', TemporaryTableTypeForker),
            ('columns', TableColumnsForker),
        ]

        forkers = []
        for key, cls in children:
            kw = kwargs.get(key, {})
            if kw is None:
                continue

            factory = self.child_forker_factory(cls, **kw)
            if factory:
                forkers.append(factory)

        def _table_name_factory(**_):
            return SingleValueForker({'table_name': self.next_tbl_name()})

        forkers.append(_table_name_factory)
        return forkers

    @classmethod
    def child_forker_factory(cls, child_cls, **kwargs):
        max_points1 = kwargs.get('max_points')

        def _factory(**kw):
            kwargs.update(kw)
            max_points2 = kw.get('max_points')
            if max_points1 is not None and max_points2 is not None:
                max_points = min(max_points1, max_points2)
            elif max_points1 is not None:
                max_points = max_points1
            else:
                max_points = max_points2

            kwargs['max_points'] = max_points
            return child_cls(**kwargs)

        return _factory
