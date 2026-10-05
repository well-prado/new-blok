package runtime

import (
	"context"

	"github.com/well-prado/new-blok/observe/slo"
)

// Availability reports the supervisor as the operational worker name
// (ADR 0022): ready while negotiated and accepting calls, the calls it holds
// and its concurrent-call capacity. Ready describes the lifecycle, as
// Supervisor.Ready does; it does not probe transport reachability, which
// deployment readiness checks with a real call (ADR 0009).
func (s *Supervisor) Availability(name string) slo.Worker {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slo.Worker{Name: name, Ready: s.state == stateReady, InFlight: len(s.sem), Capacity: cap(s.sem)}
}

// AvailabilitySource is Availability as an operational source named name.
func (s *Supervisor) AvailabilitySource(name string) slo.Source {
	return slo.Func(name, func(context.Context) (slo.Snapshot, error) {
		return slo.Snapshot{Workers: []slo.Worker{s.Availability(name)}}, nil
	})
}
