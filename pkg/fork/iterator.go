package fork

import "errors"

// Iterator is an interface to iterate values
type Iterator interface {
	Valid() bool
	Value() interface{}
	Next() error
	Close()
}

// FixedIterator implements Iterator, it is an iterator with fixed values
type FixedIterator struct {
	items []interface{}
	cur   int
}

// NewFixedIterator create a new FixedIterator
func NewFixedIterator(items []interface{}) *FixedIterator {
	return &FixedIterator{
		items: items,
		cur:   0,
	}
}

func (i *FixedIterator) Valid() bool {
	return i.cur >= 0 && i.cur < len(i.items)
}

func (i *FixedIterator) Value() interface{} {
	if i.Valid() {
		return i.items[i.cur]
	}
	return nil
}

func (i *FixedIterator) Next() error {
	if !i.Valid() {
		return errors.New("iter is invalid")
	}
	i.cur++
	return nil
}

func (i *FixedIterator) Close() {
	i.cur = -1
}
