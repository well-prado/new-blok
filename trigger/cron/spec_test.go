package cron

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
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
	began, cpu := time.Now(), ProcessCPUTime()
	window, missed := o.Between(start, end, 3, missedCountCap)
	elapsed, used := time.Since(began), ProcessCPUTime()-cpu
	if len(window) != 3 || !window[0].Instant.Equal(end.Add(-2*time.Minute)) || !window[2].Instant.Equal(end) || missed != missedCountCap {
		t.Fatalf("window=%v missed=%d", window, missed)
	}
	// The bound is on CPU time: wall time also counts a contended host's
	// scheduling delays, which made this fail under parallel suites (#214).
	if used > catchUpBound {
		t.Fatalf("catch-up over a year used %v of CPU (%v wall)", used, elapsed)
	}
	t.Logf("a year of minutely firings at the production cap: %v CPU, %v wall", used, elapsed)
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

func TestBetweenMatchesUnboundedReference(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	utc, err := Parse("*/15 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	gap, err := Parse("30 2 * * *")
	if err != nil {
		t.Fatal(err)
	}
	fold, err := Parse("30 1 * * *")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		o    Occurrences
		from time.Time
		to   time.Time
	}{
		{
			name: "utc exact boundaries",
			o:    Occurrences{Spec: utc, Location: time.UTC, Gap: GapSkip, Fold: FoldOnce},
			from: time.Date(2026, 1, 1, 0, 15, 0, 0, time.UTC),
			to:   time.Date(2026, 1, 1, 2, 0, 0, 0, time.UTC),
		},
		{
			name: "spring gap skipped",
			o:    Occurrences{Spec: gap, Location: newYork, Gap: GapSkip, Fold: FoldOnce},
			from: time.Date(2026, 3, 7, 0, 0, 0, 0, newYork),
			to:   time.Date(2026, 3, 10, 0, 0, 0, 0, newYork),
		},
		{
			name: "spring gap shifted",
			o:    Occurrences{Spec: gap, Location: newYork, Gap: GapShift, Fold: FoldOnce},
			from: time.Date(2026, 3, 7, 0, 0, 0, 0, newYork),
			to:   time.Date(2026, 3, 10, 0, 0, 0, 0, newYork),
		},
		{
			name: "fall fold once",
			o:    Occurrences{Spec: fold, Location: newYork, Gap: GapSkip, Fold: FoldOnce},
			from: time.Date(2026, 10, 31, 0, 0, 0, 0, newYork),
			to:   time.Date(2026, 11, 3, 0, 0, 0, 0, newYork),
		},
		{
			name: "fall fold twice",
			o:    Occurrences{Spec: fold, Location: newYork, Gap: GapSkip, Fold: FoldTwice},
			from: time.Date(2026, 10, 31, 0, 0, 0, 0, newYork),
			to:   time.Date(2026, 11, 3, 0, 0, 0, 0, newYork),
		},
	}
	limits := []int{0, 1, 3, math.MaxInt}
	countLimits := []int{1, 5, math.MaxInt}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, limit := range limits {
				for _, countLimit := range countLimits {
					want, wantMissed := referenceBetween(tc.o, tc.from, tc.to, limit, countLimit)
					got, gotMissed := tc.o.Between(tc.from, tc.to, limit, countLimit)
					if !reflect.DeepEqual(got, want) || gotMissed != wantMissed {
						t.Fatalf("limit=%d countLimit=%d: got (%v, %d), want (%v, %d)", limit, countLimit, got, gotMissed, want, wantMissed)
					}
				}
			}
		})
	}
}

func TestBetweenMatchesExistingOccurrenceCorpus(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "cron", "occurrences.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixtures occurrenceFixture
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	limits := []int{0, 1, 3, math.MaxInt}
	countLimits := []int{1, 2, math.MaxInt}
	for _, tc := range fixtures.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			spec, err := Parse(tc.Spec)
			if err != nil {
				t.Fatal(err)
			}
			location, err := time.LoadLocation(tc.Zone)
			if err != nil {
				t.Fatal(err)
			}
			o := Occurrences{Spec: spec, Location: location, Gap: tc.Gap, Fold: tc.Fold}
			until := tc.After
			cursor := tc.After
			for i := 0; i < tc.Count; i++ {
				next, ok := o.Next(cursor)
				if !ok {
					t.Fatalf("fixture occurrence %d missing after %s", i, cursor)
				}
				until, cursor = next.Instant, next.Instant
			}
			for _, limit := range limits {
				for _, countLimit := range countLimits {
					want, wantMissed := referenceBetween(o, tc.After, until, limit, countLimit)
					got, gotMissed := o.Between(tc.After, until, limit, countLimit)
					if !reflect.DeepEqual(got, want) || gotMissed != wantMissed {
						t.Fatalf("limit=%d countLimit=%d: got (%v, %d), want (%v, %d)", limit, countLimit, got, gotMissed, want, wantMissed)
					}
				}
			}
		})
	}
}

// referenceBetween deliberately retains every occurrence before taking its
// tail. These short fixtures make that simple implementation an oracle for
// the bounded production scan, including its count-cap behavior.
func referenceBetween(o Occurrences, after, until time.Time, limit, countLimit int) ([]Occurrence, int) {
	var all []Occurrence
	cursor := after
	for {
		next, ok := o.Next(cursor)
		if !ok || next.Instant.After(until) {
			break
		}
		all = append(all, next)
		cursor = next.Instant
	}
	if len(all) <= limit {
		return all, 0
	}
	dropped := len(all) - limit
	if dropped > countLimit {
		dropped = countLimit
	}
	return all[len(all)-limit:], dropped
}

var occurrenceAllocationSink []Occurrence

func TestBetweenWindowAllocationsDoNotGrowPerMiss(t *testing.T) {
	spec, err := Parse("* * * * *")
	if err != nil {
		t.Fatal(err)
	}
	o := Occurrences{Spec: spec, Location: time.UTC, Gap: GapSkip, Fold: FoldOnce}
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	shortEnd := start.Add(10 * time.Minute)
	longEnd := start.Add(12 * time.Hour)
	const limit = 3

	allocs := func(end time.Time, reference bool) float64 {
		return testing.AllocsPerRun(3, func() {
			if reference {
				occurrenceAllocationSink, _ = appendShiftBetween(o, start, end, limit, math.MaxInt)
				return
			}
			occurrenceAllocationSink, _ = o.Between(start, end, limit, math.MaxInt)
		})
	}
	shortReference, longReference := allocs(shortEnd, true), allocs(longEnd, true)
	shortBounded, longBounded := allocs(shortEnd, false), allocs(longEnd, false)
	referenceGrowth := longReference - shortReference
	boundedGrowth := longBounded - shortBounded
	if longBounded >= longReference || referenceGrowth-boundedGrowth < 100 {
		t.Fatalf("allocation counts (short/long): ring %.0f/%.0f, append-and-shift %.0f/%.0f; growth reduction %.0f, want at least 100",
			shortBounded, longBounded, shortReference, longReference, referenceGrowth-boundedGrowth)
	}
	t.Logf("allocations/op at retained limit %d (10m / 12h): ring %.0f / %.0f; append-and-shift reference %.0f / %.0f",
		limit, shortBounded, longBounded, shortReference, longReference)
}

func appendShiftBetween(o Occurrences, after, until time.Time, limit, countLimit int) ([]Occurrence, int) {
	var window []Occurrence
	skipped := 0
	capped := false
	cursor := after
	for {
		next, ok := o.Next(cursor)
		if !ok || next.Instant.After(until) {
			return window, skipped
		}
		window = append(window, next)
		cursor = next.Instant
		if len(window) <= limit {
			continue
		}
		window = window[1:]
		if capped {
			continue
		}
		skipped++
		if skipped < countLimit {
			continue
		}
		capped = true
		if start, ok := o.jumpBack(until, limit); ok && start.After(cursor) {
			cursor, window = start, nil
		}
	}
}

func TestOccurrenceWindowStorageIsBoundedAndReusable(t *testing.T) {
	occurrence := Occurrence{Instant: time.Unix(1, 0), Wall: "one"}
	var zeroLimit occurrenceWindow
	zeroAllocs := testing.AllocsPerRun(100, func() { zeroLimit.push(occurrence, 0) })
	if zeroAllocs != 0 || zeroLimit.size != 0 || len(zeroLimit.values) != 0 {
		t.Fatalf("limit-zero pushes allocated %.0f times or retained storage: size=%d len=%d", zeroAllocs, zeroLimit.size, len(zeroLimit.values))
	}

	const limit = 7
	var full occurrenceWindow
	for i := 0; i < limit; i++ {
		full.push(occurrence, limit)
	}
	storage := &full.values[0]
	capacity := cap(full.values)
	fullAllocs := testing.AllocsPerRun(100, func() { full.push(occurrence, limit) })
	if fullAllocs != 0 || &full.values[0] != storage || cap(full.values) != capacity || full.size != limit {
		t.Fatalf("full window grew after fill: allocs=%.0f size=%d cap=%d (was %d)", fullAllocs, full.size, cap(full.values), capacity)
	}

	var maxLimit occurrenceWindow
	for i := 0; i < 5; i++ {
		maxLimit.push(occurrence, math.MaxInt)
	}
	if maxLimit.size != 5 || len(maxLimit.values) > 8 || cap(maxLimit.values) > 8 {
		t.Fatalf("MaxInt limit preallocated beyond observed size: size=%d len=%d cap=%d", maxLimit.size, len(maxLimit.values), cap(maxLimit.values))
	}
	maxStorage, maxCapacity := &maxLimit.values[0], cap(maxLimit.values)
	maxLimit.reset()
	if maxLimit.size != 0 || maxLimit.head != 0 || &maxLimit.values[0] != maxStorage || cap(maxLimit.values) != maxCapacity {
		t.Fatalf("reset did not retain storage: size=%d head=%d cap=%d (was %d)", maxLimit.size, maxLimit.head, cap(maxLimit.values), maxCapacity)
	}
	for _, value := range maxLimit.values {
		if value != (Occurrence{}) {
			t.Fatalf("reset retained occurrence value: %+v", value)
		}
	}
	maxLimit.push(occurrence, math.MaxInt)
	if &maxLimit.values[0] != maxStorage {
		t.Fatal("push after reset replaced reusable storage")
	}
}

func TestOccurrenceWindowResultIsOrderedAndIndependent(t *testing.T) {
	var window occurrenceWindow
	for i := 1; i <= 5; i++ {
		window.push(Occurrence{Instant: time.Unix(int64(i), 0), Wall: string(rune('0' + i))}, 3)
	}
	first := window.result(true)
	if got := []string{first[0].Wall, first[1].Wall, first[2].Wall}; !reflect.DeepEqual(got, []string{"3", "4", "5"}) {
		t.Fatalf("first result is not chronological: %v", got)
	}
	first[1].Wall = "caller mutation"
	window.push(Occurrence{Instant: time.Unix(6, 0), Wall: "6"}, 3)
	second := window.result(true)
	if got := []string{second[0].Wall, second[1].Wall, second[2].Wall}; !reflect.DeepEqual(got, []string{"4", "5", "6"}) {
		t.Fatalf("second result is not chronological: %v", got)
	}
	if first[1].Wall != "caller mutation" {
		t.Fatal("subsequent push changed a previously returned result")
	}
	second[1].Wall = "second mutation"
	third := window.result(true)
	if got := []string{third[0].Wall, third[1].Wall, third[2].Wall}; !reflect.DeepEqual(got, []string{"4", "5", "6"}) {
		t.Fatalf("result slices share caller-mutable storage: %v", got)
	}
}

func TestBetweenPreservesEmptyAndNegativeLimitSliceBehavior(t *testing.T) {
	spec, err := Parse("0 0 * * *")
	if err != nil {
		t.Fatal(err)
	}
	o := Occurrences{Spec: spec, Location: time.UTC, Gap: GapSkip, Fold: FoldOnce}
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if got, missed := o.Between(start, start.Add(-time.Second), 3, 5); got != nil || missed != 0 {
		t.Fatalf("empty interval returned (%v, %d), want (nil, 0)", got, missed)
	}
	for _, tc := range []struct {
		name       string
		until      time.Time
		countLimit int
	}{
		{name: "uncapped negative limit", until: start.AddDate(0, 0, 2), countLimit: math.MaxInt},
		{name: "reset before no later occurrence", until: start.Add(24*time.Hour + 22*time.Hour), countLimit: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want, wantMissed := appendShiftBetween(o, start, tc.until, -1, tc.countLimit)
			got, gotMissed := o.Between(start, tc.until, -1, tc.countLimit)
			if !reflect.DeepEqual(got, want) || gotMissed != wantMissed {
				t.Fatalf("got (%v, %d), want (%v, %d)", got, gotMissed, want, wantMissed)
			}
		})
	}
}
