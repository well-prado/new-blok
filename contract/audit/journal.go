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
	database        store.Database
	cfg             Config
	mirrored        atomic.Uint64
	dropped         atomic.Uint64
	refused         atomic.Uint64
	refusedCapacity atomic.Uint64
}

// Stats reports optional mirror delivery and refused appends. Durable
// records are counted by the store, never by these counters. Refused counts
// every append that returned ErrUnavailable (so its decision was refused);
// RefusedCapacity is the subset caused by a full store, which needs
// provisioning or pruning rather than an outage response.
type Stats struct{ Mirrored, MirrorDropped, Refused, RefusedCapacity uint64 }

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
				tenant_seq INTEGER NOT NULL,
				run_id TEXT NOT NULL,
				recorded_at INTEGER NOT NULL,
				record BLOB NOT NULL CHECK(length(record) <= 16384),
				digest TEXT NOT NULL,
				UNIQUE(tenant, tenant_seq))`,
			`CREATE INDEX IF NOT EXISTS audit_records_v1_time ON audit_records_v1(recorded_at, seq)`,
			`CREATE INDEX IF NOT EXISTS audit_records_v1_kind ON audit_records_v1(kind, id)`,
			`CREATE TABLE IF NOT EXISTS audit_meta_v1 (name TEXT PRIMARY KEY, value INTEGER NOT NULL)`,
			// A pruned record leaves the sha256 of its id (ids are
			// application-chosen and may carry personal data) and its kind,
			// so Verify can tell a pruned record from a missing one.
			`CREATE TABLE IF NOT EXISTS audit_pruned_v1 (id_digest TEXT PRIMARY KEY, kind TEXT NOT NULL, pruned_at INTEGER NOT NULL)`,
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
// same record appended again is a no-op (inserted is false); a different
// record under the same id is ErrConflict. Every store failure is
// ErrUnavailable, and the caller must return it so its transaction rolls
// back. Owners pass the record to Notify after commit only when inserted, so
// a retried decision is not mirrored twice.
func (j *Journal) Append(ctx context.Context, tx *sql.Tx, r Record) (record Record, inserted bool, err error) {
	if j == nil || tx == nil {
		return Record{}, false, ErrRequired
	}
	defer func() {
		if errors.Is(err, ErrUnavailable) {
			j.refused.Add(1)
			if errors.Is(err, ErrCapacity) {
				j.refusedCapacity.Add(1)
			}
		}
	}()
	if r.At.IsZero() {
		r.At = j.cfg.Clock()
	}
	r.At = r.At.UTC()
	r = clone(r)
	if err := r.Validate(); err != nil {
		return Record{}, false, err
	}
	encoded, err := r.encode()
	if err != nil {
		return Record{}, false, ErrInvalid
	}
	var existing []byte
	err = tx.QueryRowContext(ctx, `SELECT record FROM audit_records_v1 WHERE id = ?`, r.ID).Scan(&existing)
	switch {
	case err == nil:
		if !bytes.Equal(existing, encoded) {
			return Record{}, false, ErrConflict
		}
		return r, false, nil
	case !errors.Is(err, sql.ErrNoRows):
		return Record{}, false, unavailableErr(err)
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT value FROM audit_meta_v1 WHERE name = 'records'`).Scan(&count); err != nil {
		return Record{}, false, unavailableErr(err)
	}
	if count >= j.cfg.MaxRecords {
		return Record{}, false, unavailableErr(ErrCapacity)
	}
	// Each tenant has its own monotonic sequence, so a reader's cursor
	// reveals nothing about other tenants' write volume.
	counter := "tenant:" + r.Tenant
	if _, err := tx.ExecContext(ctx, `INSERT INTO audit_meta_v1 (name, value) VALUES (?, 1) ON CONFLICT(name) DO UPDATE SET value = value + 1`, counter); err != nil {
		return Record{}, false, unavailableErr(err)
	}
	var tenantSeq int64
	if err := tx.QueryRowContext(ctx, `SELECT value FROM audit_meta_v1 WHERE name = ?`, counter).Scan(&tenantSeq); err != nil {
		return Record{}, false, unavailableErr(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO audit_records_v1 (id, kind, tenant, tenant_seq, run_id, recorded_at, record, digest) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		r.ID, string(r.Kind), r.Tenant, tenantSeq, r.RunID, r.At.UnixNano(), encoded, Digest(encoded)); err != nil {
		return Record{}, false, unavailableErr(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE audit_meta_v1 SET value = value + 1 WHERE name = 'records'`); err != nil {
		return Record{}, false, unavailableErr(err)
	}
	return r, true, nil
}

// Recorded reports, inside tx, whether a record with id exists. An owner
// uses it to backfill a missing record without re-deriving one that exists
// (whose tenant or time it may not know).
func (j *Journal) Recorded(ctx context.Context, tx *sql.Tx, id string) (bool, error) {
	if j == nil || tx == nil {
		return false, ErrRequired
	}
	var found int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit_records_v1 WHERE id = ?`, id).Scan(&found); err != nil {
		return false, unavailableErr(err)
	}
	return found > 0, nil
}

// StoredTenant reports, inside tx, the tenant of the stored record with
// id. The record is verified first (digest, shape, indexed columns), and one
// that fails is ErrCorrupt, so an owner never adopts an altered tenant. It
// needs no composed Journal, so an owner can call it while migrating; the
// audit tables must exist. An owner uses it to fix, once, the tenant of a
// decision that predates its own tenant column.
func StoredTenant(ctx context.Context, tx *sql.Tx, id string) (tenant string, found bool, err error) {
	if tx == nil {
		return "", false, ErrRequired
	}
	_, record, err := scanVerified(tx.QueryRowContext(ctx, `SELECT seq, id, kind, tenant, run_id, record, digest FROM audit_records_v1 WHERE id = ?`, id))
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case errors.Is(err, ErrCorrupt):
		return "", false, ErrCorrupt
	case err != nil:
		return "", false, unavailableErr(err)
	}
	return record.Tenant, true, nil
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
	return Stats{Mirrored: j.mirrored.Load(), MirrorDropped: j.dropped.Load(), Refused: j.refused.Load(), RefusedCapacity: j.refusedCapacity.Load()}
}

// List returns one tenant's records in commit order after cursor ("" is the
// beginning), at most limit (capped at MaxPage). The reader is authorized
// before anything is read; an unauthorized reader gets ErrDenied and no
// records. Every returned record is verified against its stored digest.
// The cursor is the tenant's own sequence number, not a store-wide one.
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
		rows, err := tx.QueryContext(ctx, `SELECT tenant_seq, id, kind, tenant, run_id, record, digest FROM audit_records_v1 WHERE tenant = ? AND tenant_seq > ? ORDER BY tenant_seq LIMIT ?`, tenant, after, limit+1)
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

// Owner is a component whose durable decisions each require an audit
// record. Verify cross-checks it: every decision it lists must have its
// record (or a tombstone from Prune), and every record of its kind must
// name a decision it still has.
type Owner interface {
	AuditKind() Kind
	// AuditedIDs calls fn with the audit record id of each durable decision,
	// read in tx.
	AuditedIDs(ctx context.Context, tx *sql.Tx, fn func(id string) error) error
}

// Verify reads every record and checks it against its digest, its indexed
// columns and the record count (ErrCorrupt), then cross-checks each owner's
// durable decisions against the records (ErrMismatch): a decision without
// its record or tombstone, or a record naming a decision the owner does not
// have. Run it after a restore with every owner composed on the store.
// Records of a kind no owner is passed for (deployment decisions have no
// owner table) are integrity-checked only. It holds one read transaction
// and, per owner, a set of its decision ids in memory.
func (j *Journal) Verify(ctx context.Context, owners ...Owner) (int, error) {
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
		for _, owner := range owners {
			if err := crossCheck(ctx, tx, owner); err != nil {
				return err
			}
		}
		return nil
	})
	return verified, err
}

func crossCheck(ctx context.Context, tx *sql.Tx, owner Owner) error {
	kind := owner.AuditKind()
	decisions := map[string]bool{}
	err := owner.AuditedIDs(ctx, tx, func(id string) error {
		decisions[id] = true
		var found int
		if err := tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM audit_records_v1 WHERE id = ? AND kind = ?) + (SELECT COUNT(*) FROM audit_pruned_v1 WHERE id_digest = ? AND kind = ?)`, id, string(kind), Digest([]byte(id)), string(kind)).Scan(&found); err != nil {
			return err
		}
		if found == 0 {
			return ErrMismatch
		}
		return nil
	})
	if err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT id FROM audit_records_v1 WHERE kind = ?`, string(kind))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		if !decisions[id] {
			return ErrMismatch
		}
	}
	return rows.Err()
}

type scanner interface{ Scan(...any) error }

// scanVerified checks a stored record's integrity only: digest, decodable
// shape, indexed columns and canonical encoding. It deliberately does not
// re-run Validate: the sensitivity heuristic and bounds may change between
// releases, and a record accepted under an older rule must stay readable,
// listable and prunable rather than lock every reader out.
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
	if err := json.Unmarshal(encoded, &r); err != nil || r.ID == "" || r.At.IsZero() || r.ID != id || string(r.Kind) != kind || r.Tenant != tenant || r.RunID != runID {
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

// Prune deletes records committed strictly before cutoff, never one younger
// than Config.MinRetention, never one whose run activity reports active (an
// activity port must report a run it cannot prove ended as active), and
// never one the application's Hold keeps; it leaves each deleted record's id
// as a digest tombstone for Verify. activity is required: without it
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
			if _, err := tx.ExecContext(ctx, `INSERT INTO audit_pruned_v1 (id_digest, kind, pruned_at) VALUES (?, ?, ?) ON CONFLICT(id_digest) DO UPDATE SET pruned_at = excluded.pruned_at`, Digest([]byte(item.record.ID)), string(item.record.Kind), j.cfg.Clock().UTC().UnixNano()); err != nil {
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
