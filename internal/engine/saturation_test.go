package engine_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/capacity"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
)

type item struct {
	ID string `json:"id"`
}

var itemSchema = []byte(`{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}`)

// TestSaturationIsHiddenAfterACommittedEffect: a step that runs out of
// capacity reports saturation, which tells the caller to retry the whole
// workflow, unless an earlier step that declared effects has completed:
// the retry would repeat that effect (#190). A step without declared
// effects before it changes nothing.
func TestSaturationIsHiddenAfterACommittedEffect(t *testing.T) {
	define := func(name string, err error, options ...node.Option) node.Any {
		t.Helper()
		options = append(options, node.Description(name), node.Schemas(itemSchema, itemSchema))
		definition, defineErr := node.Define(name, "1.0.0", func(_ context.Context, in item) (item, error) { return in, err }, options...)
		if defineErr != nil {
			t.Fatal(defineErr)
		}
		return definition.Any()
	}
	nodes := map[string]node.Any{
		"test/write": define("test/write", nil, node.Effects("database:write")),
		"test/read":  define("test/read", nil),
		"test/busy":  define("test/busy", fmt.Errorf("store busy: %w", capacity.ErrSaturated), node.Effects("database:write")),
		"test/late":  define("test/late", fmt.Errorf("store busy: %w: %w", capacity.ErrSaturated, context.DeadlineExceeded), node.Effects("database:write")),
	}
	program := func(steps ...string) contract.InternalProgram {
		instructions := make([]contract.InternalInstruction, len(steps))
		for index, step := range steps {
			instructions[index] = contract.InternalInstruction{Index: index, ID: strings.TrimPrefix(step, "test/"), Kind: "call", Node: step}
		}
		return contract.InternalProgram{WorkflowID: "saturation", Instructions: instructions}
	}
	for _, test := range []struct {
		name      string
		steps     []string
		saturated bool
	}{
		{"alone", []string{"test/busy"}, true},
		{"after a step without effects", []string{"test/read", "test/busy"}, true},
		{"after a committed effect", []string{"test/write", "test/busy"}, false},
		{"after a committed effect and a read", []string{"test/write", "test/read", "test/busy"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := engine.New(nodes).Run(context.Background(), program(test.steps...), item{ID: "a"})
			var failure *engine.Error
			if !errors.As(err, &failure) || failure.Code != "node_error" || failure.Step != "busy" {
				t.Fatalf("error %v; want node_error at step busy", err)
			}
			if got := errors.Is(err, capacity.ErrSaturated); got != test.saturated {
				t.Fatalf("saturated=%v; want %v (error %v)", got, test.saturated, err)
			}
			if !test.saturated && !strings.Contains(err.Error(), `after step "write" committed its effects`) {
				t.Fatalf("error %q does not name the committed step", err)
			}
		})
	}
	// Only saturation is hidden: a deadline the failure also carries still
	// matches, so the trigger answers it as a timeout.
	_, err := engine.New(nodes).Run(context.Background(), program("test/write", "test/late"), item{ID: "a"})
	if errors.Is(err, capacity.ErrSaturated) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("after an effect, a busy store past its deadline: saturated=%v deadline=%v; want false, true", errors.Is(err, capacity.ErrSaturated), errors.Is(err, context.DeadlineExceeded))
	}
}
