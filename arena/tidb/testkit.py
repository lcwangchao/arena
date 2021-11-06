import typing
import unittest
from collections import Iterator

from arena.core.fork import Forker, RecursionForker, ForkContext, to_forker
from arena.core.mysql import Database
from .table import TableForker
from .query import QueryForker, ResultSet
from .execute import Execute, CaseExecContext


class TestKit:
    def __init__(self, *, exec_iter: Iterator[typing.Callable] = None):
        self._forkers = []
        self._exec_iter = exec_iter

    def fork(self, forker: Forker):
        if self._exec_iter:
            return next(self._exec_iter)()
        self._forkers.append(forker)

    def declare_table(self, *args, **kwargs) -> TableForker:
        tbl = TableForker(*args, **kwargs)
        self.fork(tbl.map(self._empty_map, desc=str(tbl)))
        return tbl

    def must_create_table(self, *args, **kwargs) -> TableForker:
        tbl = self.declare_table(*args, **kwargs)
        self.must_exec(tbl.sql_create)
        return tbl

    def must_exec(self, sql, *args):
        self.fork(QueryForker(sql=sql, args=args))

    def must_query(self, sql, *args) -> ResultSet:
        forker = QueryForker(sql=sql, args=args, rs=True, tk=self)
        self.fork(forker)
        return forker.rs

    def print(self, msg, *args):
        def _build(ctx: ForkContext) -> typing.Tuple[ForkContext, Execute]:
            items = ctx.current_recursion.stack_items

            def _func(ectx: CaseExecContext):
                full_msg = items[0].format(*items[1:])
                log_msg = f'[print] {full_msg}'
                ectx.append_log(log_msg)
                print(full_msg)

            return ctx, Execute(_func)

        self.fork(RecursionForker(
            forkers=[to_forker(msg)] + ([to_forker(arg) for arg in args] if args else []),
            build=_build,
            desc=f"Print '{str(msg)}'"
        ))

    @property
    def forkers(self) -> typing.List[Forker]:
        return self._forkers

    @classmethod
    def _empty_map(cls, *_, **__):
        return Execute(None)


class CaseEntity(unittest.TestCase):
    def __init__(self, name, *, func: typing.Callable, execs: typing.List[Execute],
                 db: Database = None):
        self._name = name
        self._case_func = func
        self._execs = execs
        self._db = db
        setattr(self, name, self._run_test)
        super().__init__(name)

    @property
    def name(self):
        return self._name

    @property
    def execs(self):
        return self._execs

    def run_test(self):
        runner = unittest.TextTestRunner()
        suite = unittest.TestSuite()
        suite.addTest(self)
        return runner.run(suite)

    def _run_test(self):
        with CaseExecContext(name=self._name, db=self._db, t=self) as ctx:
            try:
                def _map_func(execute: Execute):
                    def _exec_func():
                        return execute(ctx)
                    return _exec_func

                exec_iter = map(_map_func, iter(self._execs))
                self._case_func(TestKit(exec_iter=exec_iter))
                for _ in exec_iter:
                    raise RuntimeError('should not reach here')
                ctx.append_log('* SUCCESS *')
            except AssertionError as e:
                ctx.append_log('* FAIL *')
                args = list(e.args)
                args[0] = f'\n\n{ctx.dump_detail()}\n' + args[0]
                e.args = tuple(args)
                raise e
            except BaseException:
                ctx.append_log('* FAIL *')
                raise AssertionError(f'Errors occurs when executing case. \n\n{ctx.dump_detail()}\n')


class Case(Forker):
    def __init__(self, *, func):
        self._func: typing.Callable[[TestKit], None] = func
        self._name: str = func.__name__

        self._loaded: bool = False
        self._tk: typing.Optional[TestKit] = None

    def do_fork(self, *, ctx: ForkContext) -> Iterator[typing.Tuple[ForkContext, CaseEntity]]:
        if not self._loaded:
            self.reload()
        return RecursionForker(forkers=self._tk.forkers, build=self._build, desc=str(self)).do_fork(ctx=ctx)

    @property
    def name(self):
        return self._name

    def reload(self):
        tk = TestKit()
        self._func(tk)
        self._tk = tk

    def run_test(self, *, db: Database = None):
        runner = unittest.TextTestRunner()
        suite = unittest.TestSuite()
        ctx = ForkContext(variables=dict(
            db=db
        ))
        for _, entity in self.do_fork(ctx=ctx):
            suite.addTest(entity)
        return runner.run(suite)

    def _build(self, ctx: ForkContext) -> typing.Tuple[ForkContext, CaseEntity]:
        return ctx, CaseEntity(self.name, execs=ctx.current_recursion.stack_items, func=self._func, db=ctx.get_var("db"))

    def __str__(self):
        return f"Case '{self.name}'"
