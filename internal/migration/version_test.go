package migration

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

func openStore(t *testing.T) store.Database {
	t.Helper()
	db, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "version.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type stamp struct{ version, from int }

func readStamp(t *testing.T, db store.Database, component string) (stamp, bool) {
	t.Helper()
	var s stamp
	found := false
	if err := db.WithTx(context.Background(), func(tx *sql.Tx) error {
		var err error
		s.version, found, err = Stamped(context.Background(), tx, component)
		if err != nil || !found {
			return err
		}
		return tx.QueryRow(`SELECT upgraded_from FROM `+VersionTable+` WHERE component = ?`, component).Scan(&s.from)
	}); err != nil {
		t.Fatal(err)
	}
	return s, found
}

func apply(db store.Database, s Schema, migrate func(int) error) error {
	return db.WithTx(context.Background(), func(tx *sql.Tx) error {
		return Apply(context.Background(), tx, s, migrate)
	})
}

func fixed(version int) func(context.Context, *sql.Tx) (int, error) {
	return func(context.Context, *sql.Tx) (int, error) { return version, nil }
}

// TestApplyRefusesANewerStampBeforeMigrating: a binary that supports
// version 2 refuses a component stamped 3, naming the component and both
// versions, without running its migration or touching the stamp.
func TestApplyRefusesANewerStampBeforeMigrating(t *testing.T) {
	db := openStore(t)
	if err := apply(db, Schema{Component: "widget", Supported: 3, Infer: fixed(0)}, func(int) error { return nil }); err != nil {
		t.Fatal(err)
	}
	ran := false
	err := apply(db, Schema{Component: "widget", Supported: 2, Infer: fixed(0)}, func(int) error { ran = true; return nil })
	var newer *store.NewerSchemaError
	if !errors.Is(err, store.ErrNewerSchema) || !errors.As(err, &newer) || *newer != (store.NewerSchemaError{Component: "widget", Version: 3, Supported: 2}) {
		t.Fatalf("err=%v; want the widget refusal naming 3 and 2", err)
	}
	for _, part := range []string{"widget", "version 3", "version 2"} {
		if !strings.Contains(err.Error(), part) {
			t.Fatalf("refusal %q does not name %q", err, part)
		}
	}
	if ran {
		t.Fatal("the migration ran against a newer schema")
	}
	if s, _ := readStamp(t, db, "widget"); s.version != 3 {
		t.Fatalf("stamp after refusal=%+v; want 3 kept", s)
	}
}

// TestApplyStampsInTheMigrationTransaction: the stamp commits with the
// migration or not at all, and is raised from what was found.
func TestApplyStampsInTheMigrationTransaction(t *testing.T) {
	db := openStore(t)
	failed := errors.New("synthetic migration failure")
	var seen []int
	if err := apply(db, Schema{Component: "widget", Supported: 2, Infer: fixed(1)}, func(from int) error {
		seen = append(seen, from)
		return failed
	}); !errors.Is(err, failed) {
		t.Fatalf("err=%v", err)
	}
	if s, found := readStamp(t, db, "widget"); found {
		t.Fatalf("a failed migration left stamp %+v", s)
	}
	if err := apply(db, Schema{Component: "widget", Supported: 2, Infer: fixed(1)}, func(from int) error {
		seen = append(seen, from)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if s, _ := readStamp(t, db, "widget"); s != (stamp{version: 2, from: 1}) {
		t.Fatalf("stamp=%+v; want version 2 upgraded from the inferred 1", s)
	}
	// Stamped now: inference is not consulted, and the same version is
	// not rewritten.
	if err := apply(db, Schema{Component: "widget", Supported: 2, Infer: fixed(9)}, func(from int) error {
		seen = append(seen, from)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if s, _ := readStamp(t, db, "widget"); s != (stamp{version: 2, from: 1}) {
		t.Fatalf("reopen rewrote the stamp: %+v", s)
	}
	// A newer binary migrates forward from the stamp.
	if err := apply(db, Schema{Component: "widget", Supported: 4, Infer: fixed(9)}, func(from int) error {
		seen = append(seen, from)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if s, _ := readStamp(t, db, "widget"); s != (stamp{version: 4, from: 2}) {
		t.Fatalf("stamp=%+v; want 4 upgraded from 2", s)
	}
	if want := []int{1, 1, 2, 2}; !slices.Equal(seen, want) {
		t.Fatalf("migrations saw %v; want %v", seen, want)
	}
	// Components are stamped independently.
	if _, found := readStamp(t, db, "gadget"); found {
		t.Fatal("an unrelated component is stamped")
	}
}

// TestApplyRefusesAnUnstampedShapeItDoesNotKnow: inference that finds a
// newer shape than the binary supports is refused like a stamp.
func TestApplyRefusesAnUnstampedShapeItDoesNotKnow(t *testing.T) {
	db := openStore(t)
	err := apply(db, Schema{Component: "widget", Supported: 1, Infer: fixed(2)}, func(int) error { return nil })
	if !errors.Is(err, store.ErrNewerSchema) {
		t.Fatalf("err=%v; want a refusal", err)
	}
}

func TestApplyRequiresItsSchema(t *testing.T) {
	db := openStore(t)
	for _, s := range []Schema{{Supported: 1, Infer: fixed(0)}, {Component: "widget", Infer: fixed(0)}, {Component: "widget", Supported: 1}} {
		if err := apply(db, s, func(int) error { return nil }); err == nil {
			t.Fatalf("schema %+v accepted", s)
		}
	}
}
