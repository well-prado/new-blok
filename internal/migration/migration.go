// Package migration runs idempotent schema migrations that race other
// openers of the same SQLite database.
package migration

import (
	"context"
	"errors"
	"time"

	"github.com/well-prado/new-blok/store"
)

// Budget bounds how long Retry keeps starting new attempts. An attempt that
// starts within it may still wait up to the store's busy timeout for the
// write lock, so Retry can take the budget plus one busy timeout.
const Budget = 10 * time.Second

// Retry runs a schema transaction, running it again while it fails busy
// (#233, #235). Adding a column must read the schema before it writes, and
// SQLite cannot make such a transaction wait for another writer: when
// several processes open a store that needs a column, all but one fail busy
// at once instead of waiting their turn. run must be idempotent and commit
// nothing when it fails; it is then run again, with a short growing pause,
// until it succeeds, fails otherwise, ctx ends or the budget is spent.
func Retry(ctx context.Context, run func() error) error {
	deadline := time.Now().Add(Budget)
	pause := 5 * time.Millisecond
	for {
		err := run()
		if err == nil || !errors.Is(err, store.ErrBusy) || time.Now().Add(pause).After(deadline) {
			return err
		}
		timer := time.NewTimer(pause)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(err, ctx.Err())
		case <-timer.C:
		}
		pause = min(2*pause, 200*time.Millisecond)
	}
}
