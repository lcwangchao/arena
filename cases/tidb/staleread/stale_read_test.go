package staleread

import (
	"context"
	"flag"
	"fmt"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/lcwangchao/arena/pkg/fork"
	"github.com/stretchr/testify/require"
)

var testTidbHost = flag.String("test-tidb-host", "127.0.0.1", "host of tidb")
var testTidbPort = flag.Int("test-tidb-port", 4000, "port of tidb")
var testTidbDatabase = flag.String("test-tidb-db", "test", "test database of tidb")
var testTidbUser = flag.String("test-tidb-user", "root", "user of tidb")
var testStartIndex = flag.Int("test-start-index", 0, "test start index")
var testLimit = flag.Int("test-limit", 0, "test limit")

func TestStaleRead(t *testing.T) {
	require.NoError(t, flag.Set("test.parallel", "1"))
	forker, err := buildForker()
	require.NoError(t, err)
	iter, err := forker.DoFork(context.TODO())
	require.NoError(t, err)
	cases := make([]*testCase, 0, 1000)
	for iter.Valid() {
		result := iter.Value().(*fork.FsmForkResult)
		cases = append(cases, NewCase(result))
		require.NoError(t, iter.Next())
	}

	start := *testStartIndex
	dsn := fmt.Sprintf("%s@tcp(%s:%d)/%s", *testTidbUser, *testTidbHost, *testTidbPort, *testTidbDatabase)
	for i, c := range cases {
		if *testLimit > 0 && i >= *testLimit {
			break
		}

		index := start + i
		t.Run(fmt.Sprintf("%d/%d (%d actions)", index, len(cases), len(c.actions)), func(t *testing.T) {
			c.Run(t, index, dsn)
		})
	}
}
