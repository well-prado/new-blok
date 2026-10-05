package inspect_test

import (
	"testing"

	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/contract/observe"
	"github.com/well-prado/new-blok/inspect"
)

type declaredObserver bool

func (declaredObserver) Observe(inspection.Event) {}
func (d declaredObserver) ObservesPayloads() bool { return bool(d) }

type undeclaredObserver struct{}

func (undeclaredObserver) Observe(inspection.Event) {}

// A combined observer lets the engine skip payload serialization only when
// no member would read payloads (ADR 0020).
func TestCombinedObserversDeclarePayloadsWhenAnyMemberReadsThem(t *testing.T) {
	cases := []struct {
		name    string
		members []inspection.Observer
		want    bool
	}{
		{"all payload-free", []inspection.Observer{declaredObserver(false), declaredObserver(false)}, false},
		{"one reads payloads", []inspection.Observer{declaredObserver(false), declaredObserver(true)}, true},
		{"one undeclared", []inspection.Observer{declaredObserver(false), undeclaredObserver{}}, true},
	}
	for _, tc := range cases {
		combined := inspect.CombineObservers(tc.members...)
		declared, ok := combined.(observe.PayloadObserver)
		if !ok || declared.ObservesPayloads() != tc.want {
			t.Fatalf("%s: declared=%v ok=%v, want %v", tc.name, declared, ok, tc.want)
		}
	}
}
