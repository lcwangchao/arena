package fork

import (
	"context"
)

// Forker is an interface to produce forked values
type Forker interface {
	DoFork(ctx context.Context) (Iterator, error)
}

// SimpleForker implements Forker, it is a forker with iterator
type SimpleForker struct {
	createIter func(ctx context.Context) (Iterator, error)
}

// NewSimpleForker creates a new SimpleForker
func NewSimpleForker(createIter func(ctx context.Context) (Iterator, error)) *SimpleForker {
	return &SimpleForker{createIter: createIter}
}

// NewFixedForker creates a new SimpleForker with fixed items
func NewFixedForker(items []interface{}) *SimpleForker {
	return NewSimpleForker(func(_ context.Context) (Iterator, error) {
		return NewFixedIterator(items), nil
	})
}

func (f *SimpleForker) DoFork(ctx context.Context) (Iterator, error) {
	return f.createIter(ctx)
}
