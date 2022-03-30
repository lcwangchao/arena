package fork

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func intCondition(fn func(i int) bool) Condition {
	return FnCondition(func(obj interface{}) (bool, error) {
		i, ok := obj.(int)
		if !ok {
			return false, errors.New("int required")
		}
		return fn(i), nil
	})
}

var positive = intCondition(func(i int) bool { return i > 0 })
var negative = intCondition(func(i int) bool { return i < 0 })
var odd = intCondition(func(i int) bool { return i%2 == 1 })
var even = intCondition(func(i int) bool { return i%2 == 0 })
var zero = intCondition(func(i int) bool { return i == 0 })

func TestCondition(t *testing.T) {
	checkTrue(t, positive, 1)
	checkFalse(t, negative, 1)
	checkTrue(t, odd, 1)
	checkFalse(t, even, 1)

	checkFalse(t, positive, -2)
	checkTrue(t, negative, -2)
	checkFalse(t, odd, -2)
	checkTrue(t, even, -2)

	checkTrue(t, And(positive, odd), 1)
	checkFalse(t, And(positive, even), 1)
	checkFalse(t, And(negative, odd), 1)
	checkFalse(t, And(negative, even), 1)

	checkFalse(t, Or(negative, even), 1)
	checkTrue(t, Or(positive, even), 1)
	checkTrue(t, Or(negative, odd), 1)
	checkTrue(t, Or(positive, odd), 1)

	checkTrue(t, Not(negative), 1)
	checkFalse(t, Not(positive), 1)

	checkTrue(t, And(positive, odd, Not(zero)), 1)
	checkFalse(t, And(zero, positive, odd), 1)
	checkFalse(t, And(positive, odd, zero), 1)
	checkFalse(t, And(positive, zero, odd), 1)

	checkFalse(t, Or(negative, odd, Not(zero)), 0)
	checkTrue(t, Or(zero, negative, odd), 0)
	checkTrue(t, Or(negative, zero, odd), 0)
	checkTrue(t, Or(negative, odd, zero), 0)

}

func checkCondition(t *testing.T, cond Condition, obj interface{}, r bool) {
	ok, err := cond.Evaluate(obj)
	require.NoError(t, err)
	require.Equal(t, r, ok)
}

func checkTrue(t *testing.T, cond Condition, obj interface{}) {
	checkCondition(t, cond, obj, true)
}

func checkFalse(t *testing.T, cond Condition, obj interface{}) {
	checkCondition(t, cond, obj, false)
}
