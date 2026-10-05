package slo

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// TextContentType is the Prometheus text exposition content type WriteText
// produces.
const TextContentType = "text/plain; version=0.0.4; charset=utf-8"

// WriteText renders s in the Prometheus text exposition format under the
// catalogue's Prometheus names, so the standard-library /metrics endpoint
// and the OpenTelemetry path through a collector expose the same series.
// Counters and censuses are written with every closed label value, zeros
// included, so a rule can tell "zero stalled" from "no census". Names were
// validated as bounded labels; an invalid snapshot is refused.
func WriteText(w io.Writer, s Snapshot) error {
	if err := s.Validate(); err != nil {
		return err
	}
	out := bufio.NewWriter(w)
	family := func(name string) {
		m, _ := Lookup(name)
		kind := string(m.Kind)
		fmt.Fprintf(out, "# HELP %s %s\n# TYPE %s %s\n", m.Prometheus, m.Description, m.Prometheus, kind)
	}
	sample := func(name string, labels []string, value float64) {
		m, _ := Lookup(name)
		out.WriteString(m.Prometheus)
		if len(labels) > 0 {
			out.WriteByte('{')
			for i := 0; i+1 < len(labels); i += 2 {
				if i > 0 {
					out.WriteByte(',')
				}
				fmt.Fprintf(out, "%s=%q", strings.ReplaceAll(labels[i], ".", "_"), labels[i+1])
			}
			out.WriteByte('}')
		}
		out.WriteByte(' ')
		out.WriteString(strconv.FormatFloat(value, 'g', -1, 64))
		out.WriteByte('\n')
	}
	if r := s.Readiness; r != nil {
		family(MetricReady)
		sample(MetricReady, nil, boolValue(r.Ready))
		family(MetricDraining)
		sample(MetricDraining, nil, boolValue(r.Draining))
		if len(r.Dependencies) > 0 {
			family(MetricDependencyReady)
			for _, d := range r.Dependencies {
				sample(MetricDependencyReady, []string{AttrDependency, d.Name}, boolValue(d.Ready))
			}
		}
	}
	if a := s.Admission; a != nil {
		family(MetricAdmissionActive)
		sample(MetricAdmissionActive, nil, float64(a.Active))
		family(MetricAdmissionCap)
		sample(MetricAdmissionCap, nil, float64(a.Capacity))
		family(MetricAdmissionReqs)
		sample(MetricAdmissionReqs, []string{AttrResult, "accepted", AttrReason, "none"}, float64(a.Accepted))
		for _, reason := range RejectReasons {
			sample(MetricAdmissionReqs, []string{AttrResult, "rejected", AttrReason, string(reason)}, float64(a.Rejected[reason]))
		}
	}
	if len(s.Work) > 0 {
		family(MetricWorkItems)
		for _, work := range s.Work {
			for _, l := range Livenesses {
				sample(MetricWorkItems, []string{AttrSource, work.Source, AttrLiveness, string(l)}, float64(work.Count(l)))
			}
		}
		family(MetricWorkOldest)
		for _, work := range s.Work {
			sample(MetricWorkOldest, []string{AttrSource, work.Source}, seconds(work.OldestPending))
		}
		family(MetricWorkDead)
		for _, work := range s.Work {
			sample(MetricWorkDead, []string{AttrSource, work.Source}, float64(work.DeadLetters))
		}
	}
	if len(s.Work) > 0 || len(s.Timers) > 0 {
		family(MetricCensusTruncated)
		for _, work := range s.Work {
			sample(MetricCensusTruncated, []string{AttrSource, work.Source}, boolValue(work.Truncated))
		}
		for _, timers := range s.Timers {
			if !hasWork(s.Work, timers.Source) {
				sample(MetricCensusTruncated, []string{AttrSource, timers.Source}, boolValue(timers.Truncated))
			}
		}
	}
	if len(s.Timers) > 0 {
		family(MetricTimersOverdue)
		for _, timers := range s.Timers {
			sample(MetricTimersOverdue, []string{AttrSource, timers.Source}, float64(timers.Overdue))
		}
		family(MetricTimerLag)
		for _, timers := range s.Timers {
			sample(MetricTimerLag, []string{AttrSource, timers.Source}, seconds(timers.Lag))
		}
	}
	if len(s.Workers) > 0 {
		family(MetricWorkerReady)
		for _, worker := range s.Workers {
			sample(MetricWorkerReady, []string{AttrWorker, worker.Name}, boolValue(worker.Ready))
		}
		family(MetricWorkerInFlight)
		for _, worker := range s.Workers {
			sample(MetricWorkerInFlight, []string{AttrWorker, worker.Name}, float64(worker.InFlight))
		}
		family(MetricWorkerCapacity)
		for _, worker := range s.Workers {
			sample(MetricWorkerCapacity, []string{AttrWorker, worker.Name}, float64(worker.Capacity))
		}
	}
	if p := s.Partitions; p != nil {
		family(MetricPartitions)
		sample(MetricPartitions, []string{AttrOwned, "true"}, float64(p.Owned))
		sample(MetricPartitions, []string{AttrOwned, "false"}, float64(p.Total-p.Owned))
	}
	if len(s.Storage) > 0 {
		family(MetricStorageUsed)
		for _, store := range s.Storage {
			sample(MetricStorageUsed, []string{AttrStore, store.Name}, float64(store.Used))
		}
		budgeted := false
		for _, store := range s.Storage {
			if store.Budget > 0 {
				if !budgeted {
					family(MetricStorageBudget)
					budgeted = true
				}
				sample(MetricStorageBudget, []string{AttrStore, store.Name}, float64(store.Budget))
			}
		}
	}
	if takeover := slicesFilter(s.Work); len(takeover) > 0 {
		family(MetricCensusTakeover)
		for _, work := range takeover {
			sample(MetricCensusTakeover, []string{AttrSource, work.Source}, seconds(work.Takeover))
		}
	}
	if len(s.Sources) > 0 {
		family(MetricSourceUp)
		for _, st := range s.Sources {
			sample(MetricSourceUp, []string{AttrSource, st.Name, AttrPages, fmt.Sprint(st.Pages)}, boolValue(st.Up))
		}
		family(MetricSourceAge)
		for _, st := range s.Sources {
			sample(MetricSourceAge, []string{AttrSource, st.Name, AttrPages, fmt.Sprint(st.Pages)}, seconds(st.Age))
		}
	}
	family(MetricSampleFailures)
	sample(MetricSampleFailures, nil, float64(s.SampleFailures))
	return out.Flush()
}

func hasWork(work []Work, source string) bool {
	for _, w := range work {
		if w.Source == source {
			return true
		}
	}
	return false
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func seconds(d time.Duration) float64 { return d.Seconds() }

// slicesFilter returns the work censuses that declare a takeover window.
func slicesFilter(work []Work) []Work {
	var out []Work
	for _, w := range work {
		if w.Takeover > 0 {
			out = append(out, w)
		}
	}
	return out
}
