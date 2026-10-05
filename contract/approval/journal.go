package approval

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/well-prado/new-blok/store"
)

// JournalStore shares the journal's transactional database port and volume.
// Its owned tables never modify journal_runs/operations/attempts. Decisions
// and their audit payload commit atomically under SQLite WAL/FULL barriers.
type JournalStore struct {
	database     store.Database
	authorizer   ReviewAuthorizer
	clock        func() time.Time
	maxDecisions int
}

type Config struct {
	Authorizer   ReviewAuthorizer
	Clock        func() time.Time
	MaxDecisions int
}

func NewJournalStore(ctx context.Context, database store.Database, cfg Config) (*JournalStore, error) {
	if database == nil || cfg.Authorizer == nil || cfg.MaxDecisions <= 0 || cfg.MaxDecisions > 1000000 {
		return nil, ErrDenied
	}
	if err := database.Integrity(ctx); err != nil {
		return nil, err
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	s := &JournalStore{database: database, authorizer: cfg.Authorizer, clock: cfg.Clock, maxDecisions: cfg.MaxDecisions}
	err := database.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS approval_decisions_v1 (
			id TEXT PRIMARY KEY, decision BLOB NOT NULL CHECK(length(decision) <= 16384),
			proposal BLOB NOT NULL CHECK(length(proposal) <= 16384),
			binding TEXT NOT NULL, recorded_at TEXT NOT NULL)`)
		return err
	})
	return s, err
}

// Record is immutable even for rejection/expiry. Changed proposals and replay
// runs require a fresh ID and authorized review. Duplicate exact requests are
// idempotent; all conflicting reuse is rejected.
func (s *JournalStore) Record(ctx context.Context, id string, p Proposal, grant []string, expires time.Time, approved bool) (Decision, error) {
	if !validSet(grant) {
		return Decision{}, ErrDenied
	}
	if _, err := Digest(p); err != nil {
		return Decision{}, err
	}
	p = CloneProposal(p)
	grant = sortedUnique(grant)
	digest, err := Digest(p)
	if err != nil {
		return Decision{}, err
	}
	now := s.clock().UTC()
	if !validText(id) || !validSet(grant) || !Subset(p.Scope, grant) ||
		now.IsZero() || !expires.After(now) || expires.After(now.Add(24*time.Hour)) {
		return Decision{}, ErrDenied
	}
	reviewer, err := s.authorizer.AuthorizeReview(ctx, CloneProposal(p), append([]string(nil), grant...))
	if err != nil {
		return Decision{}, err
	}
	if !validText(reviewer) {
		return Decision{}, ErrDenied
	}
	d := Decision{ID: id, ProposalDigest: digest, Reviewer: reviewer, Scope: grant, ExpiresAt: expires.UTC(), RecordedAt: now, Approved: approved}
	encoded, err := json.Marshal(d)
	if err != nil {
		return Decision{}, err
	}
	proposalJSON, err := json.Marshal(p)
	if err != nil {
		return Decision{}, err
	}
	if len(encoded) > MaxRecordBytes || len(proposalJSON) > MaxRecordBytes {
		return Decision{}, ErrCapacity
	}
	err = s.database.WithTx(ctx, func(tx *sql.Tx) error {
		// Reserve the write lock before reading limits or detecting duplicates;
		// concurrent writers cannot both consume the last capacity slot.
		if _, err := tx.ExecContext(ctx, `UPDATE approval_decisions_v1 SET id=id WHERE id=?`, id); err != nil {
			return err
		}
		var old []byte
		err := tx.QueryRowContext(ctx, `SELECT decision FROM approval_decisions_v1 WHERE id=?`, id).Scan(&old)
		if err == nil {
			var existing Decision
			if err := json.Unmarshal(old, &existing); err != nil {
				return err
			}
			d.RecordedAt = existing.RecordedAt
			check, _ := json.Marshal(d)
			if !bytes.Equal(old, check) {
				return ErrConflict
			}
			d = existing
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM approval_decisions_v1`).Scan(&count); err != nil {
			return err
		}
		if count >= s.maxDecisions {
			return ErrCapacity
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO approval_decisions_v1 (id,decision,proposal,binding,recorded_at) VALUES (?,?,?,?,?)`, id, encoded, proposalJSON, digest, now.Format(time.RFC3339Nano))
		return err
	})
	if err != nil {
		return Decision{}, err
	}
	return d, nil
}

func (s *JournalStore) Get(ctx context.Context, id string) (Decision, bool, error) {
	if !validText(id) {
		return Decision{}, false, ErrDenied
	}
	var d Decision
	found := false
	err := s.database.WithTx(store.ReadOnly(ctx), func(tx *sql.Tx) error {
		var encoded, proposalJSON []byte
		var binding, recorded string
		err := tx.QueryRowContext(ctx, `SELECT decision,proposal,binding,recorded_at FROM approval_decisions_v1 WHERE id=?`, id).Scan(&encoded, &proposalJSON, &binding, &recorded)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if len(encoded) > MaxRecordBytes || len(proposalJSON) > MaxRecordBytes {
			return ErrCapacity
		}
		if err := json.Unmarshal(encoded, &d); err != nil {
			return err
		}
		var p Proposal
		if err := json.Unmarshal(proposalJSON, &p); err != nil {
			return err
		}
		digest, err := Digest(p)
		if err != nil {
			return err
		}
		if d.ID != id || d.ProposalDigest != digest || binding != digest || recorded != d.RecordedAt.UTC().Format(time.RFC3339Nano) || !validText(d.Reviewer) || !validSet(d.Scope) || !Subset(p.Scope, d.Scope) {
			return ErrStale
		}
		found = true
		return nil
	})
	return d, found && err == nil, err
}
