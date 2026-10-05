package audit

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/well-prado/new-blok/store"
)

const pruneBatch = 256

// Config composes the durable audit journal.
type Config struct {
	// Clock stamps records written without a time. Defaults to time.Now.
	Clock func() time.Time
	// MaxRecords bounds the store (1..MaxRecords). A full store refuses the
	// next decision with ErrCapacity; audit is never silently discarded.
	MaxRecords int
	// Readers authorizes List. Required.
	Readers ReadAuthorizer
	// Mirror optionally receives committed records (logs, telemetry).
	Mirror Mirror
	// MinRetention is the application's legal minimum: Prune never deletes
	// a record younger than this, whatever cutoff it is given.
	MinRetention time.Duration
	// Hold is the application's legal-hold policy: a record for which it
	// returns true is kept by Prune.
	Hold func(Record) bool
}

// Journal stores audit records in the application's durable store. Its
// owned tables never modify another component's tables; owners write
// records inside their own transactions on the same database.
type Journal struct {
	database store.Database
	cfg      Config
	mirrored atomic.Uint64
	dropped  atomic.Uint64
}

// Stats reports optional mirror delivery. Durable records are counted by the
// store, never by these counters.
type Stats struct{ Mirrored, MirrorDropped uint64 }

// NewJournal creates the audit tables if needed. It refuses a configuration
// without a capacity bound or a read authorizer.
func NewJournal(ctx context.Context, database store.Database, cfg Config) (*Journal, error) {
	if database == nil || cfg.Readers == nil || cfg.MaxRecords <= 0 || cfg.MaxRecords > MaxRecords || cfg.MinRetention < 0 {
		return nil, ErrInvalid
	}
	if err := database.Integrity(ctx); err != nil {
		return nil, err
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	j := &Journal{database: database, cfg: cfg}
	err := database.WithTx(ctx, func(tx *sql.Tx) error {
		for _, statement := range []string{
			`CREATE TABLE IF NOT EXISTS audit_records_v1 (
				seq INTEGER PRIMARY KEY AUTOINCREMENT,
				id TEXT NOT NULL UNIQUE,
				kind TEXT NOT NULL,
				tenant TEXT NOT NULL,
				run_id TEXT NOT NULL,
				recorded_at INTEGER NOT NULL,
				record BLOB NOT NULL CHECK(length(record) <= 16384),
				digest TEXT NOT NULL)`,
			`CREATE INDEX IF NOT EXISTS audit_records_v1_tenant ON audit_records_v1(tenant, seq)`,
			`CREATE INDEX IF NOT EXISTS audit_records_v1_time ON audit_records_v1(recorded_at, seq)`,
			`CREATE TABLE IF NOT EXISTS audit_meta_v1 (name TEXT PRIMARY KEY, value INTEGER NOT NULL)`,
			`INSERT INTO audit_meta_v1 (name, value) SELECT 'records', COUNT(*) FROM audit_records_v1 WHERE true ON CONFLICT(name) DO NOTHING`,
		} {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return j, nil
}

// Shares reports whether the journal writes to database. An owner that
// writes records inside its own transactions must share it, or the record
// and the decision would not commit together.
func (j *Journal) Shares(database store.Database) bool {
	return j != nil && database != nil && j.database == database
}

// Append writes r inside tx, the transaction of the decision it records:
// both commit or neither does. A record without a time is stamped. The
// same record appended again is a no-op; a different record under the same
// id is ErrConflict. Every store failure is ErrUnavailable, and the caller
// must return it so its transaction rolls back.
func (j *Journal) Append(ctx context.Context, tx *sql.Tx, r Record) (Record, error) {
	if j == nil || tx == nil {
		return Record{}, ErrRequired
	}
	if r.At.IsZero() {
		r.At = j.cfg.Clock()
	}
	r.At = r.At.UTC()
	r = clone(r)
	if err := r.Validate(); err != nil {
		return Record{}, err
	}
	encoded, err := r.encode()
	if err != nil {
		return Record{}, ErrInvalid
	}
	var existing []byte
	err = tx.QueryRowContext(ctx, `SELECT record FROM audit_records_v1 WHERE id = ?`, r.ID).Scan(&existing)
	switch {
	case err == nil:
		if !bytes.Equal(existing, encoded) {
			return Record{}, ErrConflict
		}
		return r, nil
	case !errors.Is(err, sql.ErrNoRows):
		return Record{}, unavailableErr(err)
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT value FROM audit_meta_v1 WHERE name = 'records'`).Scan(&count); err != nil {
		return Record{}, unavailableErr(err)
	}
	if count >= j.cfg.MaxRecords {
		return Record{}, unavailableErr(ErrCapacity)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO audit_records_v1 (id, kind, tenant, run_id, recorded_at, record, digest) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		r.ID, string(r.Kind), r.Tenant, r.RunID, r.At.UnixNano(), encoded, Digest(encoded)); err != nil {
		return Record{}, unavailableErr(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE audit_meta_v1 SET value = value + 1 WHERE name = 'records'`); err != nil {
		return Record{}, unavailableErr(err)
	}
	return r, nil
}

// Notify offers committed records to the optional Mirror. Owners call it
// after their transaction committed. It never blocks on, fails because of,
// or changes durable audit; a refused or panicking Mirror counts a drop.
func (j *Journal) Notify(records ...Record) {
	if j == nil || j.cfg.Mirror == nil {
		return
	}
	for _, r := range records {
		if j.offer(clone(r)) {
			j.mirrored.Add(1)
		} else {
			j.dropped.Add(1)
		}
	}
}

func (j *Journal) offer(r Record) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	return j.cfg.Mirror.Offer(r)
}

// Stats reports the optional mirror counters.
func (j *Journal) Stats() Stats {
	if j == nil {
		return Stats{}
	}
	return Stats{Mirrored: j.mirrored.Load(), MirrorDropped: j.dropped.Load()}
}

// List returns one tenant's records in commit order after cursor ("" is the
// beginning), at most limit (capped at MaxPage). The reader is authorized
// before anything is read; an unauthorized reader gets ErrDenied and no
// records. Every returned record is verified against its stored digest.
func (j *Journal) List(ctx context.Context, tenant, cursor string, limit int) ([]Record, string, error) {
	if j == nil {
		return nil, "", ErrRequired
	}
	if tenant != "" && !validTenant(tenant) {
		return nil, "", ErrDenied
	}
	if err := j.cfg.Readers.AuthorizeAuditRead(ctx, tenant); err != nil {
		return nil, "", ErrDenied
	}
	if limit <= 0 || limit > MaxPage {
		limit = MaxPage
	}
	after := int64(0)
	if cursor != "" {
		parsed, err := strconv.ParseInt(cursor, 10, 64)
		if err != nil || parsed < 0 {
			return nil, "", ErrInvalid
		}
		after = parsed
	}
	var out []Record
	next := ""
	err := j.database.WithTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT seq, id, kind, tenant, run_id, record, digest FROM audit_records_v1 WHERE tenant = ? AND seq > ? ORDER BY seq LIMIT ?`, tenant, after, limit+1)
		if err != nil {
			return err
		}
		defer rows.Close()
		last := int64(0)
		for rows.Next() {
			seq, record, err := scanVerified(rows)
			if err != nil {
				return err
			}
			if len(out) == limit {
				next = strconv.FormatInt(last, 10)
				break
			}
			out = append(out, record)
			last = seq
		}
		return rows.Err()
	})
	if err != nil {
		return nil, "", err
	}
	return out, next, nil
}

// Verify reads every record and checks it against its digest, its indexed
// columns and the record count. Run it after a restore: a restored store
// with a damaged or partial audit table fails with ErrCorrupt.
func (j *Journal) Verify(ctx context.Context) (int, error) {
	if j == nil {
		return 0, ErrRequired
	}
	verified := 0
	err := j.database.WithTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT seq, id, kind, tenant, run_id, record, digest FROM audit_records_v1 ORDER BY seq`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			if _, _, err := scanVerified(rows); err != nil {
				return err
			}
			verified++
		}
		if err := rows.Err(); err != nil {
			return err
		}
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT value FROM audit_meta_v1 WHERE name = 'records'`).Scan(&count); err != nil {
			return err
		}
		if count != verified {
			return ErrCorrupt
		}
		return nil
	})
	return verified, err
}

type scanner interface{ Scan(...any) error }

func scanVerified(row scanner) (int64, Record, error) {
	var seq int64
	var id, kind, tenant, runID, digest string
	var encoded []byte
	if err := row.Scan(&seq, &id, &kind, &tenant, &runID, &encoded, &digest); err != nil {
		return 0, Record{}, err
	}
	if len(encoded) > MaxRecordBytes || Digest(encoded) != digest {
		return 0, Record{}, ErrCorrupt
	}
	var r Record
	if err := json.Unmarshal(encoded, &r); err != nil || r.Validate() != nil || r.ID != id || string(r.Kind) != kind || r.Tenant != tenant || r.RunID != runID {
		return 0, Record{}, ErrCorrupt
	}
	if again, err := r.encode(); err != nil || !bytes.Equal(again, encoded) {
		return 0, Record{}, ErrCorrupt
	}
	return seq, r, nil
}

// RunActivity reports which runs still have non-terminal durable state. It
// reads in the audit transaction, so the answer cannot change between the
// check and the deletion; it must therefore be backed by the same database.
type RunActivity interface {
	ActiveRuns(ctx context.Context, tx *sql.Tx, runIDs []string) (map[string]bool, error)
}

// PruneReport counts one Prune pass.
type PruneReport struct {
	Removed    int
	KeptActive int
	KeptHeld   int
}

// Prune deletes records committed before cutoff, never one younger than
// Config.MinRetention, never one whose run activity reports active, and
// never one the application's Hold keeps. activity is required: without it
// a run's state is unknown, so nothing is deleted. Each batch is its own
// bounded transaction; a failure leaves earlier batches committed and
// deletes nothing in the failed one.
func (j *Journal) Prune(ctx context.Context, cutoff time.Time, activity RunActivity) (PruneReport, error) {
	var report PruneReport
	if j == nil || activity == nil {
		return report, ErrRequired
	}
	if legal := j.cfg.Clock().Add(-j.cfg.MinRetention); legal.Before(cutoff) {
		cutoff = legal
	}
	after := int64(0)
	for {
		batch, last, done, err := j.pruneBatch(ctx, cutoff.UTC().UnixNano(), after, activity)
		report.Removed += batch.Removed
		report.KeptActive += batch.KeptActive
		report.KeptHeld += batch.KeptHeld
		if err != nil || done {
			return report, err
		}
		after = last
	}
}

func (j *Journal) pruneBatch(ctx context.Context, cutoff, after int64, activity RunActivity) (PruneReport, int64, bool, error) {
	var report PruneReport
	last, done := after, false
	err := j.database.WithTx(store.Writer(ctx), func(tx *sql.Tx) error {
		report = PruneReport{}
		// Reserve the writer before reading, as every journal transition does.
		if _, err := tx.ExecContext(ctx, `UPDATE audit_meta_v1 SET value = value WHERE 0`); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT seq, id, kind, tenant, run_id, record, digest FROM audit_records_v1 WHERE recorded_at < ? AND seq > ? ORDER BY seq LIMIT ?`, cutoff, after, pruneBatch)
		if err != nil {
			return err
		}
		type candidate struct {
			seq    int64
			record Record
		}
		var candidates []candidate
		var runIDs []string
		for rows.Next() {
			seq, record, err := scanVerified(rows)
			if err != nil {
				rows.Close()
				return err
			}
			candidates = append(candidates, candidate{seq, record})
			if record.RunID != "" {
				runIDs = append(runIDs, record.RunID)
			}
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if len(candidates) < pruneBatch {
			done = true
		}
		if len(candidates) == 0 {
			return nil
		}
		last = candidates[len(candidates)-1].seq
		active := map[string]bool{}
		if len(runIDs) > 0 {
			if active, err = activity.ActiveRuns(ctx, tx, runIDs); err != nil {
				return err
			}
		}
		for _, item := range candidates {
			switch {
			case item.record.RunID != "" && active[item.record.RunID]:
				report.KeptActive++
				continue
			case j.cfg.Hold != nil && j.held(item.record):
				report.KeptHeld++
				continue
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM audit_records_v1 WHERE seq = ?`, item.seq); err != nil {
				return err
			}
			report.Removed++
		}
		_, err = tx.ExecContext(ctx, `UPDATE audit_meta_v1 SET value = value - ? WHERE name = 'records'`, report.Removed)
		return err
	})
	if err != nil {
		return PruneReport{}, after, true, err
	}
	return report, last, done, nil
}

// held fails closed: a Hold that panics keeps the record.
func (j *Journal) held(r Record) (keep bool) {
	defer func() {
		if recover() != nil {
			keep = true
		}
	}()
	return j.cfg.Hold(clone(r))
}

func clone(r Record) Record {
	if r.Digests != nil {
		digests := make(map[string]string, len(r.Digests))
		for name, digest := range r.Digests {
			digests[name] = digest
		}
		r.Digests = digests
	}
	r.Refs = append([]string(nil), r.Refs...)
	if len(r.Refs) == 0 {
		r.Refs = nil
	}
	return r
}

func validTenant(tenant string) bool { return Record{Tenant: tenant}.tenantValid() }
