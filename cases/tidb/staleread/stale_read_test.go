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
var testConcurrency = flag.Int("test-concurrency", 10, "test concurrency")

func TestStaleRead(t *testing.T) {
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

	dsn := fmt.Sprintf("%s@tcp(%s:%d)/%s", *testTidbUser, *testTidbHost, *testTidbPort, *testTidbDatabase)
	runOneCase := func(c *testCase, index int, parallel bool) {
		t.Run(fmt.Sprintf("%d/%d (%d actions)", index, len(cases), len(c.actions)), func(t *testing.T) {
			if parallel {
				t.Parallel()
			}
			c.Run(t, index, dsn)
		})
	}

	batchCases := make([]*testCase, 0, *testConcurrency)
	for i, c := range cases {
		if *testConcurrency <= 1 {
			runOneCase(c, i, false)
			continue
		}

		batchCases = append(batchCases, c)
		if len(batchCases) >= *testConcurrency {
			start := i - len(batchCases) + 1
			end := i
			t.Run(fmt.Sprintf("from %d to %d", start, end), func(t *testing.T) {
				for offset, cas := range batchCases {
					runOneCase(cas, start+offset, true)
				}
			})
			batchCases = batchCases[:0]
		}
	}
}
