package app

import (
	"math"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/contract/observe"
)

type nopObserver struct{}

func (nopObserver) Observe(inspection.Event) {}

func TestTracePolicyIsValidatedAndNeedsAnObserver(t *testing.T) {
	for _, ratio := range []float64{-1, 2, math.NaN()} {
		if _, err := New(Config{Inspection: nopObserver{}, Trace: observe.TracePolicy{Ratio: ratio}}); err == nil || !strings.Contains(err.Error(), "invalid_trace_policy") {
			t.Fatalf("ratio %v: err=%v", ratio, err)
		}
	}
	if _, err := New(Config{Trace: observe.TracePolicy{Ratio: 1}}); err == nil || !strings.Contains(err.Error(), "requires an inspection observer") {
		t.Fatalf("tracing without an observer: err=%v", err)
	}
	application, err := New(Config{Inspection: nopObserver{}, Trace: observe.TracePolicy{Ratio: 0.5}})
	if err != nil || application.TracePolicy().Ratio != 0.5 {
		t.Fatalf("valid policy: %v %+v", err, application.TracePolicy())
	}
	if application, err := New(Config{}); err != nil || application.TracePolicy().Enabled() {
		t.Fatalf("zero policy must leave tracing off: %v", err)
	}
	if (*Application)(nil).TracePolicy().Enabled() {
		t.Fatal("nil application traces")
	}
}
