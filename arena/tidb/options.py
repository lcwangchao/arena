from __future__ import annotations

from dataclasses import dataclass

import typing

from arena.core.fork import *


@dataclass(frozen=True)
class TemporaryTableType:
    type: typing.Optional[str]
    commit: typing.Optional[str]
    points: int


class TemporaryTableTypeForker(Forker[TemporaryTableType]):
    TYPES = (
        None,
        TemporaryTableType('TEMPORARY', commit=None, points=1),
        TemporaryTableType('GLOBAL TEMPORARY', commit='ON COMMIT DELETE ROWS', points=1)
    )

    def __init__(self, *, max_points=None):
        self._max_points = max(0, max_points) if max_points else None

    def do_fork(self, context: ForkContext) -> ForkResult[TemporaryTableType]:
        result = context.new_fork_result(self.TYPES)
        if self._max_points is None:
            return result

        return result.filter_value(lambda tp: tp is None or tp.points <= self._max_points)
