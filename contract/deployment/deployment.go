// Package deployment defines application-owned deployment configuration and
// bounded operational state. It does not select a hosting provider.
package deployment

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"
)

var (
	ErrInvalid    = errors.New("deployment: invalid configuration")
	ErrNotReady   = errors.New("deployment: dependencies are not ready")
	ErrOverloaded = errors.New("deployment: admission capacity exhausted")
	ErrDraining   = errors.New("deployment: draining")
)

type Config struct {
	ListenerAddress string
	External        bool
	RequiredSecrets []string
	StoreRequired   bool
	WorkerRequired  bool
	MaxAdmission    int
	DrainTimeout    time.Duration
}

func (c Config) Validate() error {
	if c.ListenerAddress == "" {
		return fmt.Errorf("%w: listener address required", ErrInvalid)
	}
	if _, err := net.ResolveTCPAddr("tcp", c.ListenerAddress); err != nil {
		return fmt.Errorf("%w: listener address: %v", ErrInvalid, err)
	}
	if c.MaxAdmission <= 0 {
		return fmt.Errorf("%w: max admission must be positive", ErrInvalid)
	}
	if c.DrainTimeout <= 0 {
		return fmt.Errorf("%w: drain timeout must be positive", ErrInvalid)
	}
	seen := map[string]bool{}
	for _, secret := range c.RequiredSecrets {
		if secret == "" || seen[secret] {
			return fmt.Errorf("%w: duplicate secret ref", ErrInvalid)
		}
		seen[secret] = true
	}
	return nil
}

type Dependencies struct {
	Artifact bool
	Store    bool
	Worker   bool
	Secrets  map[string]bool
}

func (c Config) Ready(d Dependencies) error {
	if !d.Artifact || (c.StoreRequired && !d.Store) || (c.WorkerRequired && !d.Worker) {
		return ErrNotReady
	}
	for _, secret := range c.RequiredSecrets {
		if !d.Secrets[secret] {
			return ErrNotReady
		}
	}
	return nil
}

type Limiter struct {
	mu       sync.Mutex
	capacity int
	active   int
	draining bool
}

func NewLimiter(capacity int) (*Limiter, error) {
	if capacity <= 0 {
		return nil, ErrInvalid
	}
	return &Limiter{capacity: capacity}, nil
}
func (l *Limiter) Admit() (func(), error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.draining {
		return nil, ErrDraining
	}
	if l.active >= l.capacity {
		return nil, ErrOverloaded
	}
	l.active++
	released := false
	return func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if !released {
			released = true
			if l.active > 0 {
				l.active--
			}
		}
	}, nil
}
func (l *Limiter) BeginDrain() { l.mu.Lock(); l.draining = true; l.mu.Unlock() }
func (l *Limiter) Snapshot() (active int, draining bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.active, l.draining
}

type Status struct {
	Ready    bool     `json:"ready"`
	Health   bool     `json:"health"`
	Active   int      `json:"active"`
	Draining bool     `json:"draining"`
	Missing  []string `json:"missing,omitempty"`
}

func StatusFor(c Config, d Dependencies, active int, draining bool) Status {
	missing := []string{}
	if !d.Artifact {
		missing = append(missing, "artifact")
	}
	if c.StoreRequired && !d.Store {
		missing = append(missing, "store")
	}
	if c.WorkerRequired && !d.Worker {
		missing = append(missing, "worker")
	}
	for _, secret := range c.RequiredSecrets {
		if !d.Secrets[secret] {
			missing = append(missing, "secret:"+secret)
		}
	}
	sort.Strings(missing)
	return Status{Ready: len(missing) == 0 && !draining, Health: true, Active: active, Draining: draining, Missing: missing}
}
