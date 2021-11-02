from __future__ import annotations

import io
import typing
from dataclasses import dataclass


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    len: typing.Union[int, typing.Tuple[int, int], str]
    charset: str
    collate: str
    auto_inc: bool
    default: str
    unsigned: bool
    notnull: bool
    comment: str

    @classmethod
    def new(cls, name, type, *, len=0, charset=None, collate=None,
            auto_inc=False, default=None, unsigned=False, notnull=False,
            comment=None):
        return cls(
            name=name,
            type=type,
            len=len,
            charset=charset,
            collate=collate,
            auto_inc=auto_inc,
            default=default,
            unsigned=unsigned,
            notnull=notnull,
            comment=comment
        )

    def normalize(self) -> Column:
        tp, clen = self._normalize_type_len()
        default = self.default
        if not self.notnull and default is None:
            default = 'NULL'

        return Column(
            name=self.name,
            type=tp,
            len=clen,
            charset=self.charset,
            collate=self.collate,
            auto_inc=self.auto_inc,
            default=default,
            unsigned=self.unsigned,
            notnull=self.notnull,
            comment=self.comment
        )

    def _normalize_type_len(self):
        tp = self.type.lower()
        clen = self.len
        if tp == 'int':
            if not clen:
                clen = 11

        return tp, clen

    @property
    def stmt(self) -> str:
        w = io.StringIO()
        w.write(f'`{self.name}`')
        w.write(f' {self.type}')
        if self.len:
            if isinstance(self.len, tuple):
                w.write(f'({self.len[0]}, {self.len[1]})')
            else:
                w.write(f'({self.len})')

        if self.notnull:
            w.write(f' NOT NULL')

        if self.default is not None:
            w.write(f' DEFAULT {self.default}')

        return w.getvalue()
