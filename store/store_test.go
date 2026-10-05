package store

import (
	"errors"
	"fmt"
	"testing"

	"github.com/well-prado/new-blok/contract/capacity"
)

func TestWithWriteDomainAnnotatesWithoutChangingTheError(t *testing.T) {
	domain, other := NewWriteDomain(), NewWriteDomain()
	busy := fmt.Errorf("sqlite: begin: %w", ErrBusy)
	annotated := WithWriteDomain(busy, domain)
	if !errors.Is(annotated, ErrBusy) || !errors.Is(annotated, capacity.ErrSaturated) || annotated.Error() != busy.Error() {
		t.Fatalf("annotation changed the error: %v", annotated)
	}
	if got, named := ErrorWriteDomain(fmt.Errorf("worker: %w", annotated)); !named || got != domain {
		t.Fatalf("a wrapped annotation reported domain %v (named=%v); want %v", got, named, domain)
	}
	if got, _ := ErrorWriteDomain(WithWriteDomain(annotated, other)); got != other {
		t.Fatalf("the outermost annotation did not win: %v", got)
	}
	if WithWriteDomain(nil, domain) != nil || WithWriteDomain(busy, nil) != busy {
		t.Fatal("a nil error or domain was annotated")
	}
	if _, named := ErrorWriteDomain(busy); named {
		t.Fatal("an unannotated error named a domain")
	}
}

func TestErrorWriteDomainsReportsEveryJoinedBranch(t *testing.T) {
	first, second, outer := NewWriteDomain(), NewWriteDomain(), NewWriteDomain()
	busy := func(domain *WriteDomain) error {
		return WithWriteDomain(fmt.Errorf("sqlite: begin: %w", ErrBusy), domain)
	}
	joined := fmt.Errorf("handler: %w", errors.Join(busy(first), errors.New("plain"), busy(second)))
	if got := ErrorWriteDomains(joined); len(got) != 2 || got[0] != first || got[1] != second {
		t.Fatalf("joined domains=%v; want [%v %v]", got, first, second)
	}
	if got, _ := ErrorWriteDomain(joined); got != first {
		t.Fatalf("ErrorWriteDomain=%v; want the first branch %v", got, first)
	}
	// An annotation hides those nested inside it, as for ErrorWriteDomain.
	if got := ErrorWriteDomains(WithWriteDomain(joined, outer)); len(got) != 1 || got[0] != outer {
		t.Fatalf("outer annotation domains=%v; want [%v]", got, outer)
	}
	// A wrapper that exposes its cause only through As is still read, as
	// errors.As reads it.
	hidden := asOnlyError{cause: busy(first)}
	if got := ErrorWriteDomains(errors.Join(busy(second), hidden)); len(got) != 2 || got[0] != second || got[1] != first {
		t.Fatalf("As-only wrapper domains=%v; want [%v %v]", got, second, first)
	}
	if got := ErrorWriteDomains(errors.Join(errors.New("a"), nil)); len(got) != 0 {
		t.Fatalf("unannotated domains=%v; want none", got)
	}
	if got := ErrorWriteDomains(nil); len(got) != 0 {
		t.Fatalf("nil domains=%v; want none", got)
	}
}

// asOnlyError exposes its cause through As but not Unwrap.
type asOnlyError struct{ cause error }

func (e asOnlyError) Error() string      { return "redacted" }
func (e asOnlyError) As(target any) bool { return errors.As(e.cause, target) }
