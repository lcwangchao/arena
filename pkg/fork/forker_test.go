package fork

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerationForker(t *testing.T) {
	forker := NewGenerationForker(func(ctx *GenerateContext) (interface{}, error) {
		a := ctx.PickEnum(1, 2, 3).(int)
		var b int
		if a == 1 {
			b = ctx.PickEnum(11, 12, 13).(int)
		} else {
			b = ctx.PickEnum(100).(int)
		}
		return []int{a, b}, nil
	})

	checkDoFork(t, forker, []interface{}{
		[]int{1, 11}, []int{1, 12}, []int{1, 13},
		[]int{2, 100},
		[]int{3, 100},
	})

	forker = NewGenerationForker(func(ctx *GenerateContext) (interface{}, error) { return 1, nil })
	checkDoFork(t, forker, []interface{}{1})
}

func checkDoFork(t *testing.T, forker Forker, expects []interface{}) {
	iter, err := forker.DoFork(context.TODO())
	require.NoError(t, err)
	for _, expect := range expects {
		require.True(t, iter.Valid())
		got := iter.Value()
		require.Equal(t, expect, got)
		require.NoError(t, iter.Next())
	}
	require.False(t, iter.Valid())
}
