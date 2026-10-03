package cron

import (
	"archive/zip"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

// referenceNext is the exact, slow definition Next must agree with: every
// scheduled wall-clock time of consecutive local dates, mapped through
// instants (the exact mapping under the gap and fold policies), sorted by
// instant. It returns the earliest firing after t once no later date can
// hold an earlier one: a date's firings are no earlier than its midnight
// less the largest offset change (26 hours here). The scheduler's previous
// algorithm merged only two dates and missed a fold crossing midnight (Goose
// Bay fell back at 00:01 to 23:01 in 1987); this reference does not.
func (o Occurrences) referenceNext(t time.Time, memo map[time.Time][]Occurrence) (Occurrence, bool) {
	local := t.In(o.Location)
	base := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, time.UTC)
	var all []Occurrence
	for k := -3; k < searchDays; k++ {
		date := base.AddDate(0, 0, k)
		day, ok := memo[date]
		if !ok {
			day = o.referenceDate(date.Year(), date.Month(), date.Day())
			memo[date] = day
		}
		all = append(all, day...)
		if k < 1 {
			continue
		}
		sort.SliceStable(all, func(a, b int) bool { return all[a].Instant.Before(all[b].Instant) })
		bound := date.AddDate(0, 0, 1).Add(-26 * time.Hour)
		for _, candidate := range all {
			if candidate.Instant.After(t) {
				if candidate.Instant.Before(bound) {
					return candidate, true
				}
				break
			}
		}
	}
	return Occurrence{}, false
}

func (o Occurrences) referenceDate(year int, month time.Month, day int) []Occurrence {
	if !o.Spec.dateMatches(year, month, day) {
		return nil
	}
	var result []Occurrence
	for hours := o.Spec.hours; hours != 0; hours &= hours - 1 {
		hour := bits.TrailingZeros32(hours)
		for minutes := o.Spec.minutes; minutes != 0; minutes &= minutes - 1 {
			minute := bits.TrailingZeros64(minutes)
			wall := fmt.Sprintf("%04d-%02d-%02dT%02d:%02d", year, month, day, hour, minute)
			for _, instant := range o.instants(year, month, day, hour, minute) {
				result = append(result, Occurrence{Instant: instant, Wall: wall})
			}
		}
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].Instant.Before(result[j].Instant) })
	unique := result[:0]
	for _, occurrence := range result {
		if len(unique) == 0 || !unique[len(unique)-1].Instant.Equal(occurrence.Instant) {
			unique = append(unique, occurrence)
		}
	}
	return unique
}

// transitions lists a zone's period boundaries in [from, to), as period
// reports them, plus the last UTC day of every leap year, where Go's
// ZoneBounds misreports TZ-rule-extended zones.
func transitions(location *time.Location, from, to time.Time) []time.Time {
	var out []time.Time
	o := Occurrences{Location: location}
	at := from
	for at.Before(to) {
		_, end, _ := o.period(at)
		if end.IsZero() || !end.Before(to) {
			break
		}
		out = append(out, end)
		at = end
	}
	for year := from.Year(); year < to.Year(); year++ {
		if year%4 == 0 && (year%100 != 0 || year%400 == 0) {
			out = append(out, time.Date(year, 12, 31, 12, 0, 0, 0, time.UTC))
		}
	}
	return out
}

// compareAround walks Next and referenceNext side by side from 30 hours
// before a transition to 30 hours after it.
func compareAround(t *testing.T, o Occurrences, zone, spec string, transition time.Time) int {
	t.Helper()
	cursor, until := transition.Add(-30*time.Hour), transition.Add(30*time.Hour)
	compared := 0
	memo := map[time.Time][]Occurrence{}
	for cursor.Before(until) {
		got, gotOK := o.Next(cursor)
		want, wantOK := o.referenceNext(cursor, memo)
		if gotOK != wantOK || !got.Instant.Equal(want.Instant) || got.Wall != want.Wall {
			t.Fatalf("%s %q gap=%s fold=%s after %s (transition %s): got %v %s %v, want %v %s %v",
				zone, spec, o.Gap, o.Fold, cursor.Format(time.RFC3339), transition.Format(time.RFC3339),
				got.Instant.Format(time.RFC3339), got.Wall, gotOK, want.Instant.Format(time.RFC3339), want.Wall, wantOK)
		}
		if !gotOK {
			break
		}
		compared++
		cursor = got.Instant
	}
	return compared
}

var differentialSpecs = []string{"*/10 0-4,22-23 * * *", "30 2 * * *", "0,30 0-3 * * *", "45 23 * * *", "0 0 * * *", "15,45 1 * * 0"}

// TestNextMatchesExactReference checks the bitset walk against the exact
// reference around every transition of zones chosen for their shapes:
// ordinary DST, negative DST (Dublin), a 2-hour DST (Troll), 30-minute DST
// (Lord Howe), a skipped calendar day (Apia 2011), midnight transitions
// (Sao Paulo, Havana), offsets in seconds (Monrovia until 1972) and
// fractional offsets (Kolkata, Chatham, Tehran), a fold across midnight
// (Goose Bay fell back at 00:01 until 2011), and America/Fort_Wayne,
// whose TZ-rule extension from 2008 meets Go's ZoneBounds leap-year quirk.
func TestNextMatchesExactReference(t *testing.T) {
	zones := []string{"America/Goose_Bay", "America/Fort_Wayne", "America/New_York", "Europe/London", "Europe/Dublin", "Antarctica/Troll", "Australia/Lord_Howe", "Pacific/Apia", "America/Sao_Paulo", "America/Havana", "Africa/Monrovia", "Asia/Kolkata", "Pacific/Chatham", "Asia/Tehran", "Pacific/Kiritimati", "America/Santiago"}
	// The full 1970–2040 range of these zones runs with the every-zone
	// sweep; by default 2010–2040, and a shorter span under -race.
	from, to := time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2041, 1, 1, 0, 0, 0, 0, time.UTC)
	if testing.Short() || raceEnabled {
		from = time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
		to = time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	compared := differential(t, zones, from, to)
	t.Logf("%d zones, %s–%s: %d firings identical to the reference", len(zones), from.Format("2006"), to.Format("2006"), compared)
}

// TestNextMatchesExactReferenceEveryZone is the full sweep: every zone in
// Go's time zone database, every transition 1970–2040. It takes minutes, so
// it runs only with NEWBLOK_CRON_SWEEP=1.
func TestNextMatchesExactReferenceEveryZone(t *testing.T) {
	if os.Getenv("NEWBLOK_CRON_SWEEP") == "" {
		t.Skip("set NEWBLOK_CRON_SWEEP=1 to compare every zone")
	}
	archive, err := zip.OpenReader(filepath.Join(runtime.GOROOT(), "lib", "time", "zoneinfo.zip"))
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	var zones []string
	for _, file := range archive.File {
		if !strings.HasSuffix(file.Name, "/") {
			zones = append(zones, file.Name)
		}
	}
	from, to := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2040, 1, 1, 0, 0, 0, 0, time.UTC)
	compared := differential(t, zones, from, to)
	t.Logf("%d zones, 1970–2040: %d firings identical to the reference", len(zones), compared)
}

func differential(t *testing.T, zones []string, from, to time.Time) int {
	t.Helper()
	compared := 0
	for _, zone := range zones {
		location, err := time.LoadLocation(zone)
		if err != nil {
			t.Fatal(err)
		}
		for _, text := range differentialSpecs {
			spec, err := Parse(text)
			if err != nil {
				t.Fatal(err)
			}
			for _, gap := range []GapPolicy{GapSkip, GapShift} {
				for _, fold := range []FoldPolicy{FoldOnce, FoldTwice} {
					o := Occurrences{Spec: spec, Location: location, Gap: gap, Fold: fold}
					for _, transition := range transitions(location, from, to) {
						compared += compareAround(t, o, zone, text, transition)
					}
				}
			}
		}
	}
	return compared
}

// TestNextSurvivesZoneBoundsLeapYearQuirk: on the last UTC day of a leap
// year Go's ZoneBounds reports, for TZ-rule-extended zones, a period ending
// before its input. Next must neither loop on it nor fire differently from
// the reference there.
func TestNextSurvivesZoneBoundsLeapYearQuirk(t *testing.T) {
	for _, c := range []struct {
		zone string
		year int
	}{{"America/Fort_Wayne", 2008}, {"America/New_York", 2040}, {"Australia/Lord_Howe", 2040}} {
		location, err := time.LoadLocation(c.zone)
		if err != nil {
			t.Fatal(err)
		}
		lastDay := time.Date(c.year, 12, 31, 12, 0, 0, 0, time.UTC)
		if _, end := lastDay.In(location).ZoneBounds(); end.After(lastDay) {
			t.Fatalf("%s %d: ZoneBounds no longer misreports; this test no longer covers the quirk", c.zone, c.year)
		}
		for _, text := range []string{"*/15 * * * *", "30 23 * * *", "0 0 1 1 *"} {
			spec, err := Parse(text)
			if err != nil {
				t.Fatal(err)
			}
			o := Occurrences{Spec: spec, Location: location, Gap: GapShift, Fold: FoldTwice}
			done := make(chan int, 1)
			go func() { done <- compareAround(t, o, c.zone, text, lastDay) }()
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				t.Fatalf("%s %q: Next did not return around %s", c.zone, text, lastDay)
			}
		}
	}
}

// TestFoldAcrossMidnightOrdersByInstant: Goose Bay fell back from 00:01 ADT
// to 23:01 AST, so midnight's first instant (03:00Z) precedes the repeated
// 23:10 (03:10Z). The previous two-date merge returned 03:10Z.
func TestFoldAcrossMidnightOrdersByInstant(t *testing.T) {
	location, err := time.LoadLocation("America/Goose_Bay")
	if err != nil {
		t.Fatal(err)
	}
	spec, err := Parse("*/10 0-4,22-23 * * *")
	if err != nil {
		t.Fatal(err)
	}
	o := Occurrences{Spec: spec, Location: location, Gap: GapSkip, Fold: FoldTwice}
	cursor := time.Date(1987, 10, 25, 2, 50, 0, 0, time.UTC)
	var got []string
	for i := 0; i < 4; i++ {
		next, ok := o.Next(cursor)
		if !ok {
			t.Fatal("no firing")
		}
		got = append(got, next.Instant.Format("15:04Z")+"="+next.Wall[11:])
		cursor = next.Instant
	}
	if want := "[03:00Z=00:00 03:10Z=23:10 03:20Z=23:20 03:30Z=23:30]"; fmt.Sprint(got) != want {
		t.Fatalf("got %v, want %s", got, want)
	}
}
