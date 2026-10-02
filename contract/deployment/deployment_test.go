package deployment

import (
	"errors"
	"testing"
	"time"
)

func config() Config {
	return Config{ListenerAddress: "127.0.0.1:8080", External: false, RequiredSecrets: []string{"BLOK_SECRET"}, StoreRequired: true, WorkerRequired: false, MaxAdmission: 1, DrainTimeout: time.Second}
}

func TestConfigReadinessDoesNotRequireUnselectedWorker(t *testing.T) {
	c := config()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	d := Dependencies{Artifact: true, Store: true, Secrets: map[string]bool{"BLOK_SECRET": true}}
	if err := c.Ready(d); err != nil {
		t.Fatal(err)
	}
	status := StatusFor(c, d, 0, false)
	if !status.Ready || !status.Health {
		t.Fatalf("status: %+v", status)
	}
}

func TestReadinessRequiresSelectedDependenciesAndSecretRefs(t *testing.T) {
	c := config()
	d := Dependencies{Artifact: true, Secrets: map[string]bool{}}
	if err := c.Ready(d); !errors.Is(err, ErrNotReady) {
		t.Fatalf("missing store/secret: %v", err)
	}
	status := StatusFor(c, d, 0, false)
	if status.Ready || len(status.Missing) != 2 {
		t.Fatalf("missing status: %+v", status)
	}
}

func TestAdmissionAndDrainAreBounded(t *testing.T) {
	l, err := NewLimiter(1)
	if err != nil {
		t.Fatal(err)
	}
	release, err := l.Admit()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Admit(); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("overload: %v", err)
	}
	l.BeginDrain()
	if _, err := l.Admit(); !errors.Is(err, ErrDraining) {
		t.Fatalf("drain: %v", err)
	}
	release()
	active, draining := l.Snapshot()
	if active != 0 || !draining {
		t.Fatalf("snapshot: %d %v", active, draining)
	}
}

func TestValidationRejectsUnsafeConfig(t *testing.T) {
	c := config()
	c.ListenerAddress = "not-an-address"
	if err := c.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("address: %v", err)
	}
	c = config()
	c.RequiredSecrets = []string{"X", "X"}
	if err := c.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("duplicate secret: %v", err)
	}
}
