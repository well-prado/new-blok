// Package nodetest runs a node directly through its production invocation boundary.
package nodetest

import (
	"context"

	"github.com/well-prado/new-blok/node"
)

func Run[I, O any](ctx context.Context, definition node.Definition[I, O], input I) (O, error) {
	return definition.Invoke(ctx, input)
}
