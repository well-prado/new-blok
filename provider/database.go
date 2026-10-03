package provider

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/well-prado/new-blok/store"
)

// Records is a reference database operation, not an ORM or arbitrary SQL node.
// Business row and outbox record commit together in the injected application store.
type Records struct{ database store.Database }

func NewRecords(ctx context.Context, database store.Database) (*Records, error) {
	if Missing(database) {
		return nil, errors.New("provider: missing database")
	}
	err := database.WithTx(ctx, func(tx *sql.Tx) error {
		for _, statement := range []string{
			`CREATE TABLE IF NOT EXISTS provider_records (record_id TEXT PRIMARY KEY, operation_key TEXT NOT NULL UNIQUE, value TEXT NOT NULL)`,
			`CREATE TABLE IF NOT EXISTS provider_outbox (event_id TEXT PRIMARY KEY, record_id TEXT NOT NULL UNIQUE, payload BLOB NOT NULL, state TEXT NOT NULL)`,
		} {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, &Error{Class: Invalid, Code: "database_startup"}
	}
	return &Records{database: database}, nil
}

var errConflict = errors.New("provider: operation conflict")

func (p *Records) Execute(ctx context.Context, in DatabaseInput) (DatabaseOutput, error) {
	if in.Key == "" || in.RecordID == "" || len(in.Key) > 256 || len(in.RecordID) > 256 || len(in.Value) > 1<<20 {
		return DatabaseOutput{}, &Error{Class: Invalid, Code: "invalid_input"}
	}
	result := DatabaseOutput{RecordID: in.RecordID, EventID: "event:" + in.Key}
	err := p.database.WithTx(ctx, func(tx *sql.Tx) error {
		// Take the write reservation before idempotency reads. An empty UPDATE
		// waits for concurrent writers but never changes business records.
		if _, err := tx.ExecContext(ctx, `UPDATE provider_records SET record_id = record_id WHERE 0`); err != nil {
			return err
		}
		var recordID, value string
		err := tx.QueryRowContext(ctx, `SELECT record_id,value FROM provider_records WHERE operation_key=?`, in.Key).Scan(&recordID, &value)
		if err == nil {
			if recordID != in.RecordID || value != in.Value {
				return errConflict
			}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO provider_records(record_id,operation_key,value) VALUES(?,?,?)`, in.RecordID, in.Key, in.Value); err != nil {
			return err
		}
		payload, err := json.Marshal(in)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO provider_outbox(event_id,record_id,payload,state) VALUES(?,?,?,'pending')`, result.EventID, in.RecordID, payload)
		return err
	})
	if err != nil {
		class := Uncertain
		if errors.Is(err, errConflict) {
			class = Business
		}
		return DatabaseOutput{}, &Error{Class: class, Code: "database_failed", IdempotencyKey: in.Key}
	}
	return result, nil
}
