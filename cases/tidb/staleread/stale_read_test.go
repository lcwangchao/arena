package staleread

import (
	"context"
	"fmt"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/lcwangchao/arena/pkg/fork"
	"github.com/stretchr/testify/require"
)

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

	for idx, c := range cases[0:100] {
		index := idx
		cas := c
		t.Run(fmt.Sprintf("%d: %d actions", index, len(cas.actions)), func(t *testing.T) {
			cas.Run(t, index)
		})
	}
}
