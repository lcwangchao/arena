package fork

import (
	"context"
	"errors"
	"fmt"
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

type fsmState struct {
	pos int
	len int
}

func (s *fsmState) Signature() string {
	return fmt.Sprintf("%d", s.pos)
}

func (s *fsmState) Clone() (FsmState, error) {
	return &fsmState{
		pos: s.pos,
		len: s.len,
	}, nil
}

func TestFsmForker(t *testing.T) {
	move := func(n int) func(ctx context.Context, state FsmState) error {
		return func(ctx context.Context, state FsmState) error {
			s := state.(*fsmState)
			next := s.pos + n
			if next >= s.len || next < 0 {
				return errors.New(fmt.Sprintf("invalid move %d, %d", s.pos, n))
			}
			s.pos = next
			return nil
		}
	}

	posGte := func(v int) Condition {
		return FnCondition(func(state interface{}) (bool, error) {
			s := state.(*fsmState)
			return s.pos >= v, nil
		})
	}

	distGte := func(v int) Condition {
		return FnCondition(func(state interface{}) (bool, error) {
			s := state.(*fsmState)
			dist := s.len - s.pos - 1
			return dist >= v, nil
		})
	}

	stateLen := 10
	allStates := make([]FsmState, 0)
	for i := 0; i < stateLen; i++ {
		allStates = append(allStates, &fsmState{
			pos: i,
			len: stateLen,
		})
	}

	builder := NewFsmForkerBuilder(func() (FsmState, error) { return &fsmState{pos: 0, len: stateLen}, nil }).
		Action("forward_1", move(1), distGte(1)).
		Action("forward_2", move(2), distGte(2)).
		Action("forward_3", move(2), distGte(3)).
		Action("backward_1", move(-1), posGte(1)).
		Action("backward_2", move(-2), posGte(2))
	checkFsmForker(t, builder, allStates, func(a, b FsmState) {
		s1 := a.(*fsmState)
		s2 := b.(*fsmState)
		require.Equal(t, s1.pos, s2.pos)
	})
}

func checkFsmForker(t *testing.T, builder *FsmForkerBuilder, allStates []FsmState, equals func(a, b FsmState)) {
	forker, err := builder.Build()
	require.NoError(t, err)

	iter, err := forker.DoFork(context.TODO())
	require.NoError(t, err)
	records := make(map[string]bool)
	for iter.Valid() {
		result := iter.Value().(*FsmForkResult)
		state, err := forker.InitialStateFunc()()
		require.NoError(t, err)
		for _, r := range result.path {
			sig := state.Signature() + r.Name()
			records[sig] = true
			require.NoError(t, r.Do(context.TODO(), state))
		}
		equals(result.GetFinalState(), state)
		require.NoError(t, iter.Next())
	}

	expectedRecords := make(map[string]bool)
	for _, state := range allStates {
		for _, act := range forker.Actions() {
			ok, err := act.Condition().Evaluate(state)
			require.NoError(t, err)
			if ok {
				sig := state.Signature() + act.Name()
				expectedRecords[sig] = true
			}
		}
	}
	require.Equal(t, expectedRecords, records)
}
