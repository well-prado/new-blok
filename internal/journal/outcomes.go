package journal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/inspection"
)

// TerminalOutcomes returns the application-boundary adapter for this journal.
// Callers must construct the Invocation from authenticated adapter context;
// workflow input is never an identity source.
func (j *Journal) TerminalOutcomes() app.RunOutcomePort {
	return terminalOutcomes{journal: j}
}

type terminalOutcomes struct{ journal *Journal }

var _ app.RunOutcomePort = terminalOutcomes{}

func (o terminalOutcomes) CompleteRun(ctx context.Context, invocation inspection.Invocation, output json.RawMessage) error {
	if err := o.authorize(ctx, invocation); err != nil {
		return err
	}
	return o.journal.CompleteRun(ctx, invocation.RunID, output)
}

func (o terminalOutcomes) FailRun(ctx context.Context, invocation inspection.Invocation, errorCode, errorClass string) error {
	if err := o.authorize(ctx, invocation); err != nil {
		return err
	}
	return o.journal.FailRun(ctx, invocation.RunID, errorCode, errorClass)
}

func (o terminalOutcomes) CancelRun(ctx context.Context, invocation inspection.Invocation, reason string) error {
	if err := o.authorize(ctx, invocation); err != nil {
		return err
	}
	return o.journal.CancelRun(ctx, invocation.RunID, reason)
}

func (o terminalOutcomes) MarkRunUncertain(ctx context.Context, invocation inspection.Invocation, errorCode, errorClass string) error {
	if err := o.authorize(ctx, invocation); err != nil {
		return err
	}
	return o.journal.MarkRunUncertain(ctx, invocation.RunID, errorCode, errorClass)
}

func (o terminalOutcomes) authorize(ctx context.Context, invocation inspection.Invocation) error {
	if o.journal == nil || invocation.RunID == "" || invocation.Principal == "" {
		return ErrNotFound
	}
	err := o.journal.withRead(ctx, func(tx *sql.Tx) error {
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT 1 FROM journal_runs WHERE run_id=? AND principal=?`, invocation.RunID, invocation.Principal).Scan(&exists); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		return nil
	})
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	return err
}
