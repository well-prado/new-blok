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
