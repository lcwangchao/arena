package fork

import (
	"context"
	"errors"
	"fmt"
)

type FsmState interface {
	Signature() string
	Clone() (FsmState, error)
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
	return NewSimpleForker(f.getIter).DoFork(ctx)
}

func (f *FsmForker) getIter(ctx context.Context) (Iterator, error) {
	initialState, err := f.initialState()
	if err != nil {
		return nil, err
	}

	results := make([]interface{}, 0)
	pathMap := map[string][]*FsmAction{initialState.Signature(): nil}
	currentStates := []FsmState{initialState}
	for len(currentStates) > 0 {
		nextStates := make([]FsmState, 0)
		for _, state := range currentStates {
			parentPath, ok := pathMap[state.Signature()]
			if !ok {
				return nil, errors.New("cannot find exist state")
			}

			allowedActions, err := f.getAllowedActions(state)
			if err != nil {
				return nil, err
			}

			for _, action := range allowedActions {
				nextState, err := state.Clone()
				nextStatePath := append(append([]*FsmAction{}, parentPath...), action)
				if err != nil {
					return nil, err
				}

				if err = action.Do(ctx, nextState); err != nil {
					return nil, err
				}

				if _, ok := pathMap[nextState.Signature()]; ok {
					results = append(results, &FsmForkResult{final: nextState, path: nextStatePath})
					continue
				}

				pathMap[nextState.Signature()] = nextStatePath
				nextStates = append(nextStates, nextState)
			}
		}
		currentStates = nextStates
	}

	return NewFixedIterator(results), nil
}

func (f *FsmForker) getAllowedActions(state FsmState) ([]*FsmAction, error) {
	actions := make([]*FsmAction, 0, len(f.actions))
	for _, action := range f.actions {
		ok, err := action.when.Evaluate(state)
		if err != nil {
			return nil, err
		}

		if ok {
			actions = append(actions, action)
		}
	}
	return actions, nil
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
