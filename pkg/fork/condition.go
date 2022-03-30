package fork

type Condition interface {
	Evaluate(obj interface{}) (bool, error)
}

type FnCondition func(obj interface{}) (bool, error)

func (fn FnCondition) Evaluate(obj interface{}) (bool, error) {
	return fn(obj)
}

type AndCondition struct {
	children []Condition
}

func And(cond Condition, conditions ...Condition) *AndCondition {
	return &AndCondition{children: append([]Condition{cond}, conditions...)}
}

func (c *AndCondition) Evaluate(obj interface{}) (bool, error) {
	for _, child := range c.children {
		ok, err := child.Evaluate(obj)
		if err != nil {
			return false, err
		}

		if !ok {
			return false, nil
		}
	}
	return true, nil
}

type OrCondition struct {
	children []Condition
}

func Or(cond Condition, conditions ...Condition) *OrCondition {
	return &OrCondition{children: append([]Condition{cond}, conditions...)}
}

func (c *OrCondition) Evaluate(obj interface{}) (bool, error) {
	for _, child := range c.children {
		ok, err := child.Evaluate(obj)
		if err != nil {
			return false, err
		}

		if ok {
			return true, nil
		}
	}
	return false, nil
}

type NotCondition struct {
	child Condition
}

func Not(cond Condition) *NotCondition {
	return &NotCondition{child: cond}
}

func (c *NotCondition) Evaluate(obj interface{}) (bool, error) {
	ok, err := c.child.Evaluate(obj)
	if err != nil {
		return false, err
	}
	return !ok, nil
}
