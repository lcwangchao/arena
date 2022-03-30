package fork

import (
	"context"
	"errors"
	"fmt"
)

type Generator func(ctx *GenerateContext) (interface{}, error)

type GenerateContext struct {
	context.Context
	pick     func(forker Forker) (interface{}, error)
	pickEnum func(value interface{}, values ...interface{}) interface{}
}

func (g *GenerateContext) Pick(from interface{}) (interface{}, error) {
	switch f := from.(type) {
	case Forker:
		return g.pick(f)
	case Iterator:
		used := false
		return g.pick(NewSimpleForker(func(ctx context.Context) (Iterator, error) {
			if used {
				return nil, errors.New("iter used")
			}
			return f, nil
		}))
	default:
		return nil, errors.New(fmt.Sprintf("Unsupported type: %t", f))
	}
}

func (g *GenerateContext) PickEnum(value interface{}, values ...interface{}) interface{} {
	return g.pickEnum(value, values...)
}

func (g *GenerateContext) PickBool() bool {
	return g.pickEnum(false, true).(bool)
}

// GenerationForker implements Forker
type GenerationForker struct {
	fn func(ctx *GenerateContext) (interface{}, error)
}

func NewGenerationForker(fn func(ctx *GenerateContext) (interface{}, error)) *GenerationForker {
	return &GenerationForker{fn: fn}
}

func (f *GenerationForker) DoFork(ctx context.Context) (Iterator, error) {
	return newGenerationIterator(ctx, f.fn)
}

type generationIterator struct {
	ctx       context.Context
	generator Generator
	valid     bool
	value     interface{}
	stack     []Iterator
}

func newGenerationIterator(ctx context.Context, generator Generator) (*generationIterator, error) {
	iter := &generationIterator{
		ctx:       ctx,
		generator: generator,
		value:     nil,
		stack:     make([]Iterator, 0),
		valid:     true,
	}

	if err := iter.next(); err != nil {
		return nil, err
	}
	return iter, nil
}

func (i *generationIterator) Valid() bool {
	return i.valid
}

func (i *generationIterator) Value() interface{} {
	if i.valid {
		return i.value
	}
	return nil
}

func (i *generationIterator) Next() error {
	if len(i.stack) == 0 {
		i.Close()
		return nil
	}
	return i.next()
}

func (i *generationIterator) Close() {
	if i.valid {
		i.valid = false
		for _, iter := range i.stack {
			iter.Close()
		}
		i.stack = nil
		i.value = nil
	}
}

func (i *generationIterator) stackNext() error {
	for len(i.stack) > 0 {
		last := len(i.stack) - 1
		iter := i.stack[last]
		if err := iter.Next(); err != nil {
			return err
		}

		if iter.Valid() {
			break
		} else {
			iter.Close()
			i.stack = i.stack[:last]
		}
	}
	return nil
}

func (i *generationIterator) generateNext() (interface{}, error) {
	idx := 0
	pick := func(forker Forker) (interface{}, error) {
		defer func() {
			idx++
		}()

		if idx < len(i.stack) {
			v := i.stack[idx].Value()
			return v, nil
		}

		iter, err := forker.DoFork(i.ctx)
		if err != nil {
			return nil, err
		}

		if !iter.Valid() {
			iter.Close()
			return nil, errors.New("forker is empty")
		}

		i.stack = append(i.stack, iter)
		return iter.Value(), nil
	}

	pickEnum := func(value interface{}, values ...interface{}) interface{} {
		values = append([]interface{}{value}, values...)
		v, _ := pick(NewFixedForker(values))
		return v
	}

	return i.generator(&GenerateContext{Context: i.ctx, pick: pick, pickEnum: pickEnum})
}

func (i *generationIterator) next() (err error) {
	if !i.valid {
		return errors.New("not valid")
	}

	if i.value, err = i.generateNext(); err != nil {
		i.Close()
		return err
	}

	if err = i.stackNext(); err != nil {
		i.Close()
		return err
	}

	return nil
}
