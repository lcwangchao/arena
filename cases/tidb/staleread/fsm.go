package staleread

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/lcwangchao/arena/pkg/fork"
	"github.com/stretchr/testify/require"
)

var _ fork.FsmState = &fsmState{}

const sysVarTxReadTS = "tx_read_ts"
const sysVarTidbReadStaleness = "tidb_read_staleness"

type env struct {
	autocommit  bool
	rc          bool
	useVariable bool
	pessimistic bool
}

func (e *env) tag() string {
	if e == nil {
		return ""
	}
	return strings.Join([]string{
		fmt.Sprintf("autocommit=%v", e.autocommit),
		fmt.Sprintf("rc=%v", e.rc),
		fmt.Sprintf("useVariable=%v", e.useVariable),
		fmt.Sprintf("pessimistic=%v", e.pessimistic),
	}, "&")
}

type fsmState struct {
	dbName    string
	tableName string

	// key states
	env                     *env
	inTxn                   bool
	txnStale                bool
	sysVarTxReadTS          bool
	sysVarTidbReadStaleness bool
	stmtPrepared            bool
	binaryPrepare           bool
	preparedStale           bool

	// runtime states
	online             bool
	t                  *testing.T
	conn               *sql.Conn
	preparedBinaryStmt *sql.Stmt
	stalePoint         struct {
		asOf     string
		asOfStmt string
		data     [][]string
	}
	currentData [][]string
	longSleep   bool
	logs        []string
}

func newInitialFsmState() *fsmState {
	return &fsmState{dbName: "test", tableName: "t_stale"}
}

func (s *fsmState) Signature() string {
	sb := &strings.Builder{}
	s.writeSignatureItem(sb, "env", s.env.tag())
	s.writeSignatureItem(sb, "inTxn", s.inTxn)
	s.writeSignatureItem(sb, "txnStale", s.txnStale)
	s.writeSignatureItem(sb, "sysVarTxReadTS", s.sysVarTxReadTS)
	s.writeSignatureItem(sb, "sysVarTidbReadStaleness", s.sysVarTidbReadStaleness)
	s.writeSignatureItem(sb, "stmtPrepared", s.stmtPrepared)
	s.writeSignatureItem(sb, "binaryPrepare", s.binaryPrepare)
	s.writeSignatureItem(sb, "preparedStale", s.preparedStale)
	return sb.String()
}

func (s *fsmState) Clone() (fork.FsmState, error) {
	if s.online {
		return nil, errors.New("cannot clone online state")
	}

	var cloned fsmState
	cloned = *s
	return &cloned, nil
}

func (s *fsmState) writeSignatureItem(sb *strings.Builder, key string, value interface{}) {
	_, _ = sb.WriteString(fmt.Sprintf("%s: %v,", key, value))
}

func (s *fsmState) clear(ctx context.Context) {
	if s.online {
		sqlList := []string{
			fmt.Sprintf("use `%s`", s.dbName),
			fmt.Sprintf("drop table if exists `%s`", s.tableName),
		}
		s.executeQueries(ctx, sqlList)
	}
}

func (s *fsmState) log(msg string) {
	s.logs = append(s.logs, msg)
}

func (s *fsmState) executeQueries(ctx context.Context, queries []string) {
	for _, q := range queries {
		_ = s.executeQuery(ctx, q)
	}
}

func (s *fsmState) executeQuery(ctx context.Context, query string) sql.Result {
	s.log("[SQL] " + query + ";")
	r, err := s.conn.ExecContext(ctx, query)
	if err != nil {
		s.log("    > FAILED: " + err.Error())
		panic(err)
	}
	return r
}

func (s *fsmState) binaryPrepareStmt(ctx context.Context, query string) *sql.Stmt {
	s.log("[PREPARE] " + query + ";")
	stmt, err := s.conn.PrepareContext(ctx, query)
	if err != nil {
		s.log("    > FAILED: " + err.Error())
		panic(err)
	}
	return stmt
}

func (s *fsmState) binaryExecuteQueryStmt(stmt *sql.Stmt) [][]string {
	s.log("[EXECUTE] prepared")
	rows, err := stmt.Query()
	if err != nil {
		s.log("    > FAILED: " + err.Error())
		panic(err)
	}
	return s.toStringRows(rows)
}

func (s *fsmState) queryRows(ctx context.Context, query string) [][]string {
	s.log("[SQL] " + query + ";")
	rows, err := s.conn.QueryContext(ctx, query)
	if err != nil {
		s.log("    > FAILED: " + err.Error())
		panic(err)
	}
	return s.toStringRows(rows)
}

func (s *fsmState) toStringRows(rows *sql.Rows) [][]string {
	columns, err := rows.Columns()
	if err != nil {
		panic(err)
	}

	ret := make([][]string, 0)
	for rows.Next() {
		row := make([]string, len(columns))
		pointers := make([]interface{}, len(columns))
		for i := 0; i < len(row); i++ {
			pointers[i] = &row[i]
		}
		err = rows.Scan(pointers...)
		if err != nil {
			panic(err)
		}

		ret = append(ret, row)
	}

	err = rows.Err()
	if err != nil {
		panic(err)
	}
	return ret
}

func (s *fsmState) expectMysqlErr(errCode uint16) {
	e := recover()
	require.NotNil(s.t, e)
	err, ok := e.(error)
	require.True(s.t, ok)
	mysqlErr, ok := err.(*mysql.MySQLError)
	require.True(s.t, ok)
	require.Equal(s.t, errCode, mysqlErr.Number)
}

func (s *fsmState) initEnv(ctx context.Context, env *env) error {
	s.env = env
	if s.online {
		var autocommit, tidbTxnMode, txIsolation string
		if s.env.autocommit {
			autocommit = "1"
		} else {
			autocommit = "0"
		}

		if s.env.pessimistic {
			tidbTxnMode = "pessimistic"
		} else {
			tidbTxnMode = "optimistic"
		}

		if s.env.rc {
			txIsolation = "READ-COMMITTED"
		} else {
			txIsolation = "REPEATABLE-READ"
		}

		secondSleep := "0.1"
		if s.longSleep {
			secondSleep = "2.5"
		}

		sqlList := []string{
			fmt.Sprintf("use `%s`", s.dbName),
			fmt.Sprintf("drop table if exists `%s`", s.tableName),
			fmt.Sprintf("create table if not exists `%s`(id int primary key, v int)", s.tableName),
			fmt.Sprintf("insert into `%s` values(1, 100)", s.tableName),
			"commit",
			"do sleep(0.1)",
			"set @a=now(6)",
			fmt.Sprintf("do sleep(%s)", secondSleep),
			fmt.Sprintf("alter table `%s` add column v2 int default 0", s.tableName),
			fmt.Sprintf("update `%s` set v=v+1 where id=1", s.tableName),
			fmt.Sprintf("set autocommit=%s", autocommit),
			fmt.Sprintf("set tidb_txn_mode='%s'", tidbTxnMode),
			fmt.Sprintf("set tx_isolation='%s'", txIsolation),
		}

		s.executeQueries(ctx, sqlList)

		asOf := fmt.Sprintf(`"%s"`, s.queryRows(ctx, "select @a")[0][0])
		s.stalePoint = struct {
			asOf     string
			asOfStmt string
			data     [][]string
		}{
			asOf:     asOf,
			asOfStmt: "as of timestamp " + asOf,
			data: [][]string{
				{"1", "100"},
			},
		}
		s.currentData = [][]string{
			{"1", "101", "0"},
		}
		s.log("[INIT] init env finished")
	}
	return nil
}

func (s *fsmState) doQuery(ctx context.Context, executePrepared, selectAsOf bool) error {
	selectAsOf = selectAsOf || (executePrepared && s.preparedStale)
	willSuccess := s.inTxn && !(s.sysVarTxReadTS || selectAsOf) ||
		!s.inTxn && !(s.sysVarTxReadTS && selectAsOf)
	willStale := selectAsOf ||
		(s.inTxn && s.txnStale) ||
		(!s.inTxn && (s.sysVarTxReadTS || s.sysVarTidbReadStaleness))

	if !s.env.autocommit && !s.inTxn && !willStale && willSuccess {
		s.inTxn = true
		s.txnStale = false
		s.sysVarTxReadTS = false
	}

	if !s.online {
		return nil
	}

	if !willSuccess {
		defer s.expectMysqlErr(8135)
	}

	var result [][]string
	switch {
	case executePrepared && s.binaryPrepare:
		result = s.binaryExecuteQueryStmt(s.preparedBinaryStmt)
	case executePrepared && !s.binaryPrepare:
		result = s.queryRows(ctx, "execute s")
	case !executePrepared:
		query := fmt.Sprintf("select * from `%s`", s.tableName)
		if selectAsOf {
			query += " " + s.stalePoint.asOfStmt
		}
		result = s.queryRows(ctx, query)
	}

	if willSuccess {
		if willStale {
			require.Equal(s.t, s.stalePoint.data, result)
		} else {
			require.Equal(s.t, s.currentData, result)
		}
	}
	return nil
}

func (s *fsmState) doPrepare(ctx context.Context, binary bool, asOf bool) error {
	willStale := !s.inTxn && (asOf || s.sysVarTxReadTS || s.sysVarTidbReadStaleness)
	willSuccess := (s.inTxn && !(asOf || s.sysVarTxReadTS)) ||
		(!s.inTxn && !(asOf && s.sysVarTxReadTS))
	if willSuccess {
		s.stmtPrepared = true
		s.binaryPrepare = binary
		s.preparedStale = willStale
		s.sysVarTxReadTS = false
	}

	if !s.online {
		return nil
	}

	query := fmt.Sprintf("select * from `%s`", s.tableName)
	if asOf {
		query += " " + s.stalePoint.asOfStmt
	}
	prepareQuery := "prepare s from '" + query + "'"

	if !willSuccess {
		defer s.expectMysqlErr(8135)
	}

	if binary {
		s.preparedBinaryStmt = s.binaryPrepareStmt(ctx, query)
	} else {
		s.executeQuery(ctx, prepareQuery)
		s.preparedBinaryStmt = nil
	}
	return nil
}

func (s *fsmState) startTxn(ctx context.Context, asOf bool) error {
	willSuccess := !(s.sysVarTxReadTS && asOf)
	willStale := asOf || s.sysVarTxReadTS
	if willSuccess {
		s.inTxn = true
		s.txnStale = willStale
		s.sysVarTxReadTS = false
	}

	if !s.online {
		return nil
	}

	query := "start transaction"
	if asOf {
		query += " read only " + s.stalePoint.asOfStmt
	}
	if !willSuccess {
		defer s.expectMysqlErr(1105)
	}
	s.executeQuery(ctx, query)
	return nil
}

func (s *fsmState) closeTxn(ctx context.Context) error {
	s.inTxn = false
	s.txnStale = false
	if s.online {
		s.executeQuery(ctx, "commit")
	}
	return nil
}

func (s *fsmState) setSysVar(ctx context.Context, name string, clear bool) error {
	willSuccess := !(s.inTxn && name == sysVarTxReadTS)
	if willSuccess {
		switch name {
		case sysVarTxReadTS:
			s.sysVarTxReadTS = !clear
		case sysVarTidbReadStaleness:
			s.sysVarTidbReadStaleness = !clear
			if !clear {
				s.longSleep = true
			}
		default:
			return errors.New("unexpected var: " + name)
		}
	}

	if !s.online {
		return nil
	}

	value := "''"
	if !clear {
		switch name {
		case sysVarTxReadTS:
			value = s.stalePoint.asOf
		case sysVarTidbReadStaleness:
			value = "-2"
		}
	}
	query := fmt.Sprintf("set %s=%s", name, value)
	if !willSuccess {
		defer s.expectMysqlErr(1568)
	}
	s.executeQuery(ctx, query)
	return nil
}

func condition(fn func(state *fsmState) bool) fork.Condition {
	return fork.FnCondition(func(obj interface{}) (bool, error) {
		state, ok := obj.(*fsmState)
		if !ok {
			return false, errors.New(fmt.Sprintf("Cannot cast to *StaleReadFsmState for type: %T", obj))
		}

		return fn(state), nil
	})
}

func actionDo(fn func(context.Context, *fsmState) error) fork.FsmActionDo {
	return func(ctx context.Context, obj fork.FsmState) error {
		state, ok := obj.(*fsmState)
		if !ok {
			return errors.New(fmt.Sprintf("Cannot cast to *StaleReadFsmState for type: %T", obj))
		}

		return fn(ctx, state)
	}
}

// conditions
var initializing = condition(func(state *fsmState) bool { return state.env == nil })
var initialized = condition(func(state *fsmState) bool { return state.env != nil })
var prepared = condition(func(state *fsmState) bool { return state.stmtPrepared })

func buildForker() (*fork.FsmForker, error) {
	return fork.NewFsmForkerBuilder(func() (fork.FsmState, error) { return newInitialFsmState(), nil }).
		// init env
		When(initializing).
		ForkAction(fork.NewGenerationForker(func(ctx *fork.GenerateContext) (interface{}, error) {
			env := &env{}
			env.rc = ctx.PickEnum(true, false).(bool)
			env.pessimistic = ctx.PickEnum(true, false).(bool)
			env.autocommit = ctx.PickEnum(true, false).(bool)
			env.useVariable = ctx.PickEnum(true, false).(bool)
			return []interface{}{
				"initEnv-" + env.tag(),
				actionDo(func(ctx context.Context, state *fsmState) error {
					return state.initEnv(ctx, env)
				}),
			}, nil
		})).
		EndWhen().
		// txn
		When(initialized).
		Action("startTxn", actionDo(func(ctx context.Context, state *fsmState) error {
			return state.startTxn(ctx, false)
		})).
		Action("startTxnAsOf", actionDo(func(ctx context.Context, state *fsmState) error {
			return state.startTxn(ctx, true)
		})).
		Action("closeTxn", actionDo(func(ctx context.Context, state *fsmState) error {
			return state.closeTxn(ctx)
		})).
		EndWhen().
		// set sys var
		When(initialized).
		Action("setSysVarTxReadTS", actionDo(func(ctx context.Context, state *fsmState) error {
			return state.setSysVar(ctx, sysVarTxReadTS, false)
		})).
		Action("clearSysVarTxReadTS", actionDo(func(ctx context.Context, state *fsmState) error {
			return state.setSysVar(ctx, sysVarTxReadTS, true)
		})).
		Action("setSysTidbReadStaleness", actionDo(func(ctx context.Context, state *fsmState) error {
			return state.setSysVar(ctx, sysVarTidbReadStaleness, false)
		})).
		Action("clearSysTidbReadStaleness", actionDo(func(ctx context.Context, state *fsmState) error {
			return state.setSysVar(ctx, sysVarTidbReadStaleness, true)
		})).
		EndWhen().
		// direct select
		When(initialized).
		Action("doSelectNormal", actionDo(func(ctx context.Context, state *fsmState) error {
			return state.doQuery(ctx, false, false)
		})).
		Action("doSelectAsOf", actionDo(func(ctx context.Context, state *fsmState) error {
			return state.doQuery(ctx, false, true)
		})).
		EndWhen().
		// prepare
		When(initialized).
		Action("doSQLPrepare", actionDo(func(ctx context.Context, state *fsmState) error {
			return state.doPrepare(ctx, false, false)
		})).
		Action("doBinaryPrepare", actionDo(func(ctx context.Context, state *fsmState) error {
			return state.doPrepare(ctx, true, false)
		})).
		Action("doSQLPrepareAsOf", actionDo(func(ctx context.Context, state *fsmState) error {
			return state.doPrepare(ctx, false, true)
		})).
		Action("doBinaryPrepareAsOf", actionDo(func(ctx context.Context, state *fsmState) error {
			return state.doPrepare(ctx, true, true)
		})).
		EndWhen().
		// execute
		When(fork.And(initialized, prepared)).
		Action("doExecute", actionDo(func(ctx context.Context, state *fsmState) error {
			return state.doQuery(ctx, true, false)
		})).
		EndWhen().
		Build()
}

type testCase struct {
	state   *fsmState
	actions []*fork.FsmAction
}

func NewCase(result *fork.FsmForkResult) *testCase {
	state := newInitialFsmState()
	state.longSleep = result.GetFinalState().(*fsmState).longSleep
	return &testCase{
		state:   state,
		actions: result.GetActionPath(),
	}
}

func (c *testCase) Run(t *testing.T, index int, dns string) {
	var success = false
	defer func() {
		if !success {
			fmt.Println()
			fmt.Printf("[%d] Case failed! Logs below:\n", index)
			for _, log := range c.state.logs {
				fmt.Print("  ")
				fmt.Println(log)
			}
			fmt.Println()
		}
	}()

	db, err := sql.Open("mysql", dns)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)
	c.state.t = t
	c.state.conn = conn
	c.state.tableName = fmt.Sprintf("t_stale_%d", index)
	c.state.online = true
	defer func() {
		require.NoError(t, conn.Close())
		c.state.clear(context.TODO())
	}()

	for _, action := range c.actions {
		require.NoError(t, action.Do(context.TODO(), c.state))
	}
	success = true
}
