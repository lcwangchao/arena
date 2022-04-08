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

var tidbHost = flag.String("tidb-host", "127.0.0.1", "host of tidb")
var tidbPort = flag.Int("tidb-port", 4000, "port of tidb")
var tidbDatabase = flag.String("tidb-db", "test", "test database of tidb")
var tidbUser = flag.String("tidb-user", "root", "user of tidb")

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

	dsn := fmt.Sprintf("%s@tcp(%s:%d)/%s", *tidbUser, *tidbHost, *tidbPort, *tidbDatabase)
	for idx, c := range cases {
		index := idx
		cas := c
		t.Run(fmt.Sprintf("%d/%d (%d actions)", index, len(cases), len(cas.actions)), func(t *testing.T) {
			cas.Run(t, index, dsn)
		})
	}
}
