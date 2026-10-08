// Package flowtest tests workflows through the production engine.
package flowtest

import (
	"context"
	"sync"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
)

type Options struct {
	Mocks map[string]Mock
}

type Mock struct {
	Base   node.Any
	Output any
	Error  error
}

type Result struct {
	result engine.Result
	err    error
}

func Run(ctx context.Context, program contract.InternalProgram, nodes map[string]node.Any, input any, options Options) Result {
	registered := make(map[string]node.Any, len(nodes))
	for name, definition := range nodes {
		registered[name] = definition
	}
	for name, mock := range options.Mocks {
		definition, err := mock.Base.Mock(mock.Output, mock.Error)
		if err != nil {
			return Result{err: err}
		}
		registered[name] = definition
	}
	result, err := engine.New(registered).Run(ctx, program, input)
	return Result{result: result, err: err}
}

func (r Result) OK() bool                    { return r.err == nil }
func (r Result) Err() error                  { return r.err }
func (r Result) Response() any               { return r.result.Output }
func (r Result) State(id string) (any, bool) { value, ok := r.result.State[id]; return value, ok }
func (r Result) Steps() []engine.StepResult {
	return append([]engine.StepResult(nil), r.result.Steps...)
}

// Step returns the first execution of id, in completion order. A step inside
// an Each runs once per item: read Steps and select by IterationPath.
func (r Result) Step(id string) (engine.StepResult, bool) {
	for _, step := range r.result.Steps {
		if step.ID == id {
			return step, true
		}
	}
	return engine.StepResult{}, false
}

type Clock interface {
	Now() time.Time
	Sleep(context.Context, time.Duration) error
}

type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }
func (RealClock) Sleep(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func NewFakeClock(now time.Time) *FakeClock { return &FakeClock{now: now} }
func (c *FakeClock) Now() time.Time         { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *FakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}
func (c *FakeClock) Sleep(ctx context.Context, duration time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.Advance(duration)
	return nil
}
