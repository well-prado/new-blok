package audit_test

import (
	"encoding/base64"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/audit"
)

func validRecord() audit.Record {
	return audit.Record{ID: "approval:decision-1", Kind: audit.KindApproval, Tenant: "tenant-a", Actor: "reviewer:alice", Subject: "decision-1",
		RunID: "run-1", Action: "payments/charge@1.0.0", Outcome: audit.OutcomeApproved, Reason: "within_policy",
		Digests: map[string]string{"proposal": audit.Digest([]byte("proposal"))}, Refs: []string{"payment:write"}, At: time.Unix(10, 0)}
}

// TestRecordRefusesSecretShapedValues: every free-text field of a record is
// checked, plainly and encoded, so a record cannot carry a secret into audit
// (and from there to a reader or a model).
func TestRecordRefusesSecretShapedValues(t *testing.T) {
	if err := validRecord().Validate(); err != nil {
		t.Fatalf("valid record refused: %v", err)
	}
	values := map[string]string{
		"plain":       "password=SYNTHETIC-record-0001",
		"bearer":      "Bearer SYNTHETIC-record-0002",
		"base64":      base64.StdEncoding.EncodeToString([]byte("api_key=SYNTHETIC-record-0003")),
		"url-encoded": url.QueryEscape("client_secret=SYNTHETIC-record-0004"),
		"json":        `{"token":"SYNTHETIC-record-0005"}`,
	}
	fields := map[string]func(*audit.Record, string){
		"id":      func(r *audit.Record, v string) { r.ID = v },
		"actor":   func(r *audit.Record, v string) { r.Actor = v },
		"subject": func(r *audit.Record, v string) { r.Subject = v },
		"run":     func(r *audit.Record, v string) { r.RunID = v },
		"action":  func(r *audit.Record, v string) { r.Action = v },
		"ref":     func(r *audit.Record, v string) { r.Refs = []string{v} },
	}
	for field, set := range fields {
		for name, value := range values {
			record := validRecord()
			set(&record, value)
			if err := record.Validate(); !errors.Is(err, audit.ErrSensitive) {
				t.Errorf("%s/%s: err=%v, want ErrSensitive", field, name, err)
			}
		}
	}
}

func TestRecordShapeAndBounds(t *testing.T) {
	for name, mutate := range map[string]func(*audit.Record){
		"unknown kind":     func(r *audit.Record) { r.Kind = "debug.log" },
		"unknown outcome":  func(r *audit.Record) { r.Outcome = "maybe" },
		"no actor":         func(r *audit.Record) { r.Actor = "" },
		"no time":          func(r *audit.Record) { r.At = time.Time{} },
		"control chars":    func(r *audit.Record) { r.Subject = "a\nb" },
		"tenant not label": func(r *audit.Record) { r.Tenant = "tenant a" },
		"digest not sha":   func(r *audit.Record) { r.Digests = map[string]string{"input": "md5:abc"} },
		"too many refs":    func(r *audit.Record) { r.Refs = make([]string, 65); fill(r.Refs) },
		"oversized":        func(r *audit.Record) { r.Refs = make([]string, 64); fillLong(r.Refs) },
	} {
		record := validRecord()
		mutate(&record)
		if err := record.Validate(); !errors.Is(err, audit.ErrInvalid) {
			t.Errorf("%s: err=%v, want ErrInvalid", name, err)
		}
	}
}

func fill(values []string) {
	for i := range values {
		values[i] = "scope"
	}
}

func fillLong(values []string) {
	for i := range values {
		values[i] = strings.Repeat("s", 500)
	}
}
