package fork

import (
	"context"
	"errors"
	"fmt"
)

type FsmState interface {
	Signature(nextAction string) string
}

type FsmActionDo func(ctx context.Context, state FsmState) error

type FsmAction struct {
	name string
	when Condition
	do   FsmActionDo
}

func (f *FsmAction) Name() string {
	return f.name
}

func (f *FsmAction) Condition() Condition {
	return f.when
}

func (f *FsmAction) Do(ctx context.Context, state FsmState) error {
	return f.do(ctx, state)
}

type FsmForkResult struct {
	final FsmState
	path  []*FsmAction
}

func (r *FsmForkResult) GetFinalState() FsmState {
	return r.final
}

func (r *FsmForkResult) GetActionPath() []*FsmAction {
	return r.path
}

type FsmForker struct {
	initialState func() (FsmState, error)
	actions      []*FsmAction
}

func (f *FsmForker) InitialStateFunc() func() (FsmState, error) {
	return f.initialState
}

func (f *FsmForker) Actions() []*FsmAction {
	return f.actions
}

func (f *FsmForker) DoFork(ctx context.Context) (Iterator, error) {
	return f.newGenerationForker().DoFork(ctx)
}

func (f *FsmForker) newGenerationForker() Forker {
	dedup := make(map[string]struct{})
	return NewGenerationForker(func(ctx *GenerateContext) (interface{}, error) {
		state, err := f.initialState()
		path := make([]*FsmAction, 0)
		if err != nil {
			return nil, err
		}

		for {
			action, err := ctx.pick(NewSimpleForker(f.createActionIterFunc(state, dedup)))
			if err != nil {
				return nil, err
			}

			if action == nil {
				break
			}

			act, ok := action.(*FsmAction)
			if !ok {
				return nil, errors.New(fmt.Sprintf("invalid action type: %T", action))
			}

			path = append(path, act)
			if err := act.Do(ctx, state); err != nil {
				return nil, err
			}
		}

		return &FsmForkResult{final: state, path: path}, nil
	})
}

func (f *FsmForker) createActionIterFunc(state FsmState, dedup map[string]struct{}) func(ctx context.Context) (Iterator, error) {
	return func(ctx context.Context) (Iterator, error) {
		return newFsmActionIterator(state, f.actions, dedup)
	}
}

type fsmActionIterator struct {
	Iterator
	value *FsmAction
	dedup map[string]struct{}
}

func newFsmActionIterator(state FsmState, actions []*FsmAction, dedup map[string]struct{}) (Iterator, error) {
	allowedActions := make([]interface{}, 0, len(actions))
	for _, action := range actions {
		ok, err := action.when.Evaluate(state)
		if err != nil {
			return nil, err
		}

		if ok {
			allowedActions = append(allowedActions, []interface{}{action, state.Signature(action.name)})
		}
	}

	i := &fsmActionIterator{
		Iterator: NewFixedIterator(allowedActions),
		dedup:    dedup,
	}

	if err := i.update(); err != nil {
		return nil, err
	}

	if !i.Valid() {
		return NewFixedIterator([]interface{}{nil}), nil
	}

	return i, nil
}

func (i *fsmActionIterator) Next() error {
	if err := i.Iterator.Next(); err != nil {
		return err
	}

	if err := i.update(); err != nil {
		return err
	}

	return nil
}

func (i *fsmActionIterator) Value() interface{} {
	if !i.Valid() {
		return errors.New("not valid")
	}
	return i.value
}

func (i *fsmActionIterator) update() error {
	for i.Valid() {
		todo := i.Iterator.Value().([]interface{})
		action, signature := todo[0].(*FsmAction), todo[1].(string)
		if _, ok := i.dedup[signature]; !ok {
			i.value = action
			i.dedup[signature] = struct{}{}
			break
		}

		if err := i.Iterator.Next(); err != nil {
			return err
		}
	}
	return nil
}

type FsmForkerBuilder struct {
	forker *FsmForker
}

type WhenBuilder struct {
	cond    Condition
	builder *FsmForkerBuilder
	err     error
}

func (b *WhenBuilder) Action(name string, do FsmActionDo) *WhenBuilder {
	forker := b.builder.forker
	forker.actions = append(forker.actions, &FsmAction{
		name: name,
		do:   do,
		when: b.cond,
	})
	return b
}

func (b *WhenBuilder) ForkAction(forker Forker) (builder *WhenBuilder) {
	builder = b
	iter, err := forker.DoFork(context.TODO())
	if err != nil {
		b.err = err
		return
	}

	for iter.Valid() {
		v := iter.Value()
		item, ok := v.([]interface{})
		if !ok {
			b.err = errors.New(fmt.Sprintf("invalid value type %T, must be []interface{}", item))
			return
		}

		if len(item) != 2 {
			b.err = errors.New(fmt.Sprintf("invalid value sized %d", len(item)))
		}

		name, ok := item[0].(string)
		if !ok {
			b.err = errors.New(fmt.Sprintf("invalid name type %T", item))
			return
		}

		do, ok := item[1].(FsmActionDo)
		if !ok {
			b.err = errors.New(fmt.Sprintf("invalid name do %T", item))
			return
		}

		b.Action(name, do)
		b.err = iter.Next()
		if b.err != nil {
			return
		}
	}

	return
}

func (b *WhenBuilder) EndWhen() *FsmForkerBuilder {
	return b.builder
}

func NewFsmForkerBuilder(initialState func() (FsmState, error)) *FsmForkerBuilder {
	forker := &FsmForker{
		initialState: initialState,
		actions:      make([]*FsmAction, 0),
	}
	return &FsmForkerBuilder{forker: forker}
}

func (b *FsmForkerBuilder) When(cond Condition) *WhenBuilder {
	return &WhenBuilder{builder: b, cond: cond}
}

func (b *FsmForkerBuilder) Action(name string, do FsmActionDo, cond Condition) *FsmForkerBuilder {
	return b.When(cond).Action(name, do).EndWhen()
}

func (b *FsmForkerBuilder) Build() (*FsmForker, error) {
	actionsMap := make(map[string]*FsmAction)
	for _, action := range b.forker.actions {
		if action.name == "" {
			return nil, errors.New("action name should not be empty")
		}

		if action.when == nil {
			return nil, errors.New("condition is empty for action: " + action.name)
		}

		if action.do == nil {
			return nil, errors.New("do is empty for action: " + action.name)
		}

		if _, ok := actionsMap[action.name]; ok {
			return nil, errors.New("duplicated action name: " + action.name)
		}
	}
	return b.forker, nil
}
