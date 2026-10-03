package cron

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type occurrenceFixture struct {
	Cases []struct {
		Name   string     `json:"name"`
		Spec   string     `json:"spec"`
		Zone   string     `json:"zone"`
		Gap    GapPolicy  `json:"gap"`
		Fold   FoldPolicy `json:"fold"`
		After  time.Time  `json:"after"`
		Count  int        `json:"count"`
		Expect []string   `json:"expect"`
	} `json:"cases"`
	Invalid []struct {
		Spec   string `json:"spec"`
		Reason string `json:"reason"`
	} `json:"invalid"`
	Expected struct {
		Output int `json:"output"`
		Errors int `json:"errors"`
	} `json:"expected"`
}

func TestOccurrenceCorpus(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "cron", "occurrences.json"))
	if err != nil {
		t.Fatal(err)
	}
	var f occurrenceFixture
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	output := 0
	for _, tc := range f.Cases {
		spec, err := Parse(tc.Spec)
		if err != nil {
			t.Fatalf("%s: %v", tc.Name, err)
		}
		location, err := time.LoadLocation(tc.Zone)
		if err != nil {
			t.Fatalf("%s: %v", tc.Name, err)
		}
		o := Occurrences{Spec: spec, Location: location, Gap: tc.Gap, Fold: tc.Fold}
		cursor := tc.After
		var got []string
		for i := 0; i < tc.Count; i++ {
			next, ok := o.Next(cursor)
			if !ok {
				t.Fatalf("%s: no occurrence after %s", tc.Name, cursor)
			}
			got = append(got, next.Instant.Format(time.RFC3339))
			cursor = next.Instant
		}
		if len(got) != len(tc.Expect) {
			t.Fatalf("%s: got %v, want %v", tc.Name, got, tc.Expect)
		}
		for i := range got {
			if got[i] != tc.Expect[i] {
				t.Fatalf("%s: got %v, want %v", tc.Name, got, tc.Expect)
			}
		}
		output++
	}
	errors := 0
	for _, tc := range f.Invalid {
		if _, err := Parse(tc.Spec); err == nil {
			t.Fatalf("%q (%s) was accepted", tc.Spec, tc.Reason)
		}
		errors++
	}
	if output != f.Expected.Output || errors != f.Expected.Errors {
		t.Fatalf("output=%d errors=%d, want %+v", output, errors, f.Expected)
	}
}

// TestNextIsDeterministicAndMonotonic walks a year of a DST zone minute by
// minute in coarse steps: Next never returns an instant at or before its
// input and always returns the same answer.
func TestNextIsDeterministicAndMonotonic(t *testing.T) {
	spec, err := Parse("*/20 0-3 * * *")
	if err != nil {
		t.Fatal(err)
	}
	location, _ := time.LoadLocation("America/New_York")
	for _, gap := range []GapPolicy{GapSkip, GapShift} {
		for _, fold := range []FoldPolicy{FoldOnce, FoldTwice} {
			o := Occurrences{Spec: spec, Location: location, Gap: gap, Fold: fold}
			cursor := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			previous := cursor
			for i := 0; i < 5000; i++ {
				next, ok := o.Next(cursor)
				again, _ := o.Next(cursor)
				if !ok || !next.Instant.After(previous) || !again.Instant.Equal(next.Instant) {
					t.Fatalf("%s/%s step %d: next=%v after %v (again %v)", gap, fold, i, next.Instant, previous, again.Instant)
				}
				previous, cursor = next.Instant, next.Instant
			}
		}
	}
}

func TestBetweenBoundsCatchUp(t *testing.T) {
	spec, _ := Parse("* * * * *")
	o := Occurrences{Spec: spec, Location: time.UTC, Gap: GapSkip, Fold: FoldOnce}
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// A year of minutely firings missed, at the production cap: only the
	// latest three are kept, and the count of the half million others is
	// clamped to the cap instead of enumerated.
	end := start.AddDate(1, 0, 0)
	began := time.Now()
	window, missed := o.Between(start, end, 3, missedCountCap)
	elapsed := time.Since(began)
	if len(window) != 3 || !window[0].Instant.Equal(end.Add(-2*time.Minute)) || !window[2].Instant.Equal(end) || missed != missedCountCap {
		t.Fatalf("window=%v missed=%d", window, missed)
	}
	if elapsed > catchUpBound {
		t.Fatalf("catch-up over a year took %v", elapsed)
	}
	t.Logf("a year of minutely firings at the production cap: %v", elapsed)
	// Below the cap the count is exact.
	window, missed = o.Between(start, start.AddDate(0, 0, 1), 3, missedCountCap)
	if len(window) != 3 || missed != 1437 {
		t.Fatalf("one day: window=%v missed=%d", window, missed)
	}
	window, missed = o.Between(start, start.Add(10*time.Minute), 3, missedCountCap)
	if len(window) != 3 || missed != 7 || !window[0].Instant.Equal(start.Add(8*time.Minute)) {
		t.Fatalf("short window=%v missed=%d", window, missed)
	}
	if window, missed = o.Between(start, start.Add(10*time.Minute), 0, missedCountCap); len(window) != 0 || missed != 10 {
		t.Fatalf("no catch-up: window=%v missed=%d", window, missed)
	}
}
