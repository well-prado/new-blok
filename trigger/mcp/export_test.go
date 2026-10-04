package mcp

import (
	"context"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// HoldAnswers holds every tools/call answer for d after its tool handler
// returns, on each view built so far: it widens the window between a call's
// end and the write of its answer, in which #197 lost answers.
func HoldAnswers(s *Server, d time.Duration) {
	s.mu.Lock()
	views := make([]*view, 0, len(s.views))
	for _, v := range s.views {
		views = append(views, v)
	}
	s.mu.Unlock()
	for _, v := range views {
		v.server.AddReceivingMiddleware(func(next sdk.MethodHandler) sdk.MethodHandler {
			return func(ctx context.Context, method string, request sdk.Request) (sdk.Result, error) {
				result, err := next(ctx, method, request)
				if method == "tools/call" {
					time.Sleep(d)
				}
				return result, err
			}
		})
	}
}
