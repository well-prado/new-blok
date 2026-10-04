package cron

import (
	"errors"
	"fmt"
	"math/bits"
	"sort"
	"strconv"
	"strings"
	"time"
)

// GapPolicy decides what happens to a wall-clock time that does not exist
// because the clocks spring forward over it.
type GapPolicy string

const (
	// GapSkip does not fire for a wall-clock time inside the gap.
	GapSkip GapPolicy = "skip"
	// GapShift fires once at the transition instant (the first instant after
	// the gap) for every scheduled wall-clock time inside it.
	GapShift GapPolicy = "shift"
)

// FoldPolicy decides what happens to a wall-clock time that occurs twice
// because the clocks fall back over it.
type FoldPolicy string

const (
	// FoldOnce fires only at the first occurrence (the earlier instant).
	FoldOnce FoldPolicy = "once"
	// FoldTwice fires at both instants.
	FoldTwice FoldPolicy = "twice"
)

// searchDays bounds how far ahead or back occurrences are searched. Eight
// years covers every valid 29 February schedule (2096 → 2104).
const searchDays = 8*366 + 2

// Spec is a parsed five-field cron expression: minute hour day-of-month
// month day-of-week. Fields accept *, numbers, ranges a-b, lists and steps
// (*/n, a-b/n, a/n); day-of-week 0 and 7 are Sunday. When both day-of-month
// and day-of-week are restricted, a day matches if either does (classic cron
// semantics). A day field is unrestricted only when it is exactly "*": unlike
// Vixie cron, which treats any day field starting with "*" (such as "*/2")
// as unrestricted and then requires both fields, "*/2" here is a
// restriction like "1-31/2". Macros: @yearly (@annually), @monthly,
// @weekly, @daily (@midnight), @hourly.
type Spec struct {
	minutes uint64
	hours   uint32
	days    uint32
	months  uint16
	weekday uint8
	daysAny bool
	wdAny   bool
	text    string
}

var macros = map[string]string{
	"@yearly": "0 0 1 1 *", "@annually": "0 0 1 1 *", "@monthly": "0 0 1 * *",
	"@weekly": "0 0 * * 0", "@daily": "0 0 * * *", "@midnight": "0 0 * * *", "@hourly": "0 * * * *",
}

// ErrNeverFires rejects an expression that matches no calendar date.
var ErrNeverFires = errors.New("cron: expression never fires")

// Parse parses a cron expression.
func Parse(text string) (Spec, error) {
	expression := strings.TrimSpace(text)
	if expanded, ok := macros[expression]; ok {
		expression = expanded
	}
	fields := strings.Fields(expression)
	if len(fields) != 5 {
		return Spec{}, fmt.Errorf("cron: %q must have five fields", text)
	}
	var s Spec
	var err error
	var field uint64
	if s.minutes, _, err = parseField(fields[0], 0, 59, false); err != nil {
		return Spec{}, fmt.Errorf("cron: minute: %w", err)
	}
	if field, _, err = parseField(fields[1], 0, 23, false); err != nil {
		return Spec{}, fmt.Errorf("cron: hour: %w", err)
	}
	s.hours = uint32(field)
	if field, s.daysAny, err = parseField(fields[2], 1, 31, false); err != nil {
		return Spec{}, fmt.Errorf("cron: day of month: %w", err)
	}
	s.days = uint32(field)
	if field, _, err = parseField(fields[3], 1, 12, false); err != nil {
		return Spec{}, fmt.Errorf("cron: month: %w", err)
	}
	s.months = uint16(field)
	if field, s.wdAny, err = parseField(fields[4], 0, 7, true); err != nil {
		return Spec{}, fmt.Errorf("cron: day of week: %w", err)
	}
	s.weekday = uint8(field)
	s.text = strings.Join(fields, " ")
	if !s.anyDate() {
		return Spec{}, fmt.Errorf("%w: %q", ErrNeverFires, text)
	}
	return s, nil
}

func (s Spec) String() string { return s.text }

// parseField returns the bitset of allowed values and whether the field is
// an unrestricted "*".
func parseField(field string, low, high int, weekday bool) (uint64, bool, error) {
	var set uint64
	unrestricted := field == "*"
	for _, part := range strings.Split(field, ",") {
		if part == "" {
			return 0, false, fmt.Errorf("empty list item in %q", field)
		}
		rangePart, stepPart, hasStep := strings.Cut(part, "/")
		step := 1
		if hasStep {
			n, err := strconv.Atoi(stepPart)
			if err != nil || n < 1 || n > high {
				return 0, false, fmt.Errorf("invalid step %q", stepPart)
			}
			step = n
		}
		from, to := low, high
		switch {
		case rangePart == "*":
		case strings.Contains(rangePart, "-"):
			a, b, _ := strings.Cut(rangePart, "-")
			var err error
			if from, err = value(a, low, high); err != nil {
				return 0, false, err
			}
			if to, err = value(b, low, high); err != nil {
				return 0, false, err
			}
			if from > to {
				return 0, false, fmt.Errorf("range %q is reversed", rangePart)
			}
		default:
			n, err := value(rangePart, low, high)
			if err != nil {
				return 0, false, err
			}
			from = n
			if !hasStep {
				to = n
			}
		}
		for v := from; v <= to; v += step {
			if weekday && v == 7 {
				v = 0
				set |= 1
				break
			}
			set |= 1 << uint(v)
		}
	}
	return set, unrestricted, nil
}

func value(text string, low, high int) (int, error) {
	n, err := strconv.Atoi(text)
	if err != nil || n < low || n > high {
		return 0, fmt.Errorf("value %q outside %d-%d", text, low, high)
	}
	return n, nil
}

// anyDate reports whether some month/day combination can ever match.
func (s Spec) anyDate() bool {
	if s.minutes == 0 || s.hours == 0 || s.months == 0 {
		return false
	}
	if !s.daysAny && !s.wdAny {
		return s.days != 0 || s.weekday != 0
	}
	if !s.wdAny {
		return s.weekday != 0
	}
	maxDay := [13]int{0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	for month := 1; month <= 12; month++ {
		if s.months&(1<<uint(month)) == 0 {
			continue
		}
		for day := 1; day <= maxDay[month]; day++ {
			if s.days&(1<<uint(day)) != 0 {
				return true
			}
		}
	}
	return false
}

func (s Spec) dateMatches(year int, month time.Month, day int) bool {
	if s.months&(1<<uint(month)) == 0 {
		return false
	}
	dayOK := s.days&(1<<uint(day)) != 0
	weekday := time.Date(year, month, day, 12, 0, 0, 0, time.UTC).Weekday()
	wdOK := s.weekday&(1<<uint(weekday)) != 0
	switch {
	case s.daysAny && s.wdAny:
		return true
	case s.daysAny:
		return wdOK
	case s.wdAny:
		return dayOK
	default:
		return dayOK || wdOK
	}
}

// Occurrence is one firing: the instant (UTC) and the wall-clock time it was
// scheduled for.
type Occurrence struct {
	Instant time.Time
	Wall    string
}

// Occurrences computes firings of a spec in a location under the DST
// policies.
type Occurrences struct {
	Spec     Spec
	Location *time.Location
	Gap      GapPolicy
	Fold     FoldPolicy
}

// instants maps one wall-clock time to the instants it denotes under the
// policies: none or the transition instant in a gap, one or two in a fold.
func (o Occurrences) instants(year int, month time.Month, day, hour, minute int) []time.Time {
	civil := time.Date(year, month, day, hour, minute, 0, 0, time.UTC)
	probe := time.Date(year, month, day, hour, minute, 0, 0, o.Location)
	offsets := map[int]bool{}
	for _, around := range []time.Time{probe.Add(-36 * time.Hour), probe, probe.Add(36 * time.Hour)} {
		_, offset := around.In(o.Location).Zone()
		offsets[offset] = true
	}
	var candidates []time.Time
	for offset := range offsets {
		instant := civil.Add(-time.Duration(offset) * time.Second)
		local := instant.In(o.Location)
		if local.Year() == year && local.Month() == month && local.Day() == day && local.Hour() == hour && local.Minute() == minute {
			candidates = append(candidates, instant.UTC())
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Before(candidates[j]) })
	switch {
	case len(candidates) == 0:
		if o.Gap != GapShift {
			return nil
		}
		return []time.Time{o.transition(civil, offsets)}
	case len(candidates) > 1 && o.Fold != FoldTwice:
		return candidates[:1]
	}
	return candidates
}

// transition finds the instant the offset changes for a wall-clock time in a
// gap: the first instant after the gap.
func (o Occurrences) transition(civil time.Time, offsets map[int]bool) time.Time {
	low, high := civil.Add(48*time.Hour), civil.Add(-48*time.Hour)
	for offset := range offsets {
		instant := civil.Add(-time.Duration(offset) * time.Second)
		if instant.Before(low) {
			low = instant
		}
		if instant.After(high) {
			high = instant
		}
	}
	_, before := low.In(o.Location).Zone()
	for high.Sub(low) > time.Second {
		middle := low.Add(high.Sub(low) / 2).Truncate(time.Second)
		if _, offset := middle.In(o.Location).Zone(); offset == before {
			low = middle
		} else {
			high = middle
		}
	}
	return high.UTC()
}

// nearTransition is how close to a zone transition a firing is confirmed
// against the exact mapping (instants). Two instants of one wall-clock time
// are never further apart than the largest offset change, well under it.
const nearTransition = 48 * time.Hour

const wallLayout = "2006-01-02T15:04"

// Next returns the first firing strictly after t, or false if none exists
// within the search bound.
//
// Between two zone transitions the offset is constant, so each wall-clock
// time is one instant and wall-clock order is instant order: the hour and
// minute bitsets are scanned forward directly, without enumerating whole
// days. A firing within nearTransition of a transition is confirmed with
// instants (the exact mapping under the gap and fold policies), and a
// spring-forward gap contributes its shifted firing at the transition.
func (o Occurrences) Next(t time.Time) (Occurrence, bool) {
	limit := t.Add(searchDays * 24 * time.Hour)
	at, after := t, t
	for at.Before(limit) {
		start, end, confirm := o.period(at)
		if start.After(t) {
			if occurrence, ok := o.shifted(start); ok {
				return occurrence, true
			}
		}
		stop := limit
		if !end.IsZero() && end.Before(limit) {
			stop = end
		}
		_, offset := at.In(o.Location).Zone()
		if occurrence, ok := o.walk(after, start, end, stop, offset, confirm); ok {
			return occurrence, true
		}
		if !stop.Before(limit) {
			break
		}
		// The next period begins at end; its first firing may be at end.
		at, after = end, end.Add(-time.Nanosecond)
	}
	return Occurrence{}, false
}

// period returns the bounds of the constant-offset period containing at,
// zero when unbounded. Go's ZoneBounds can answer with an end at or before
// its input: for a zone extended by a TZ rule, on the last UTC day of a leap
// year (America/Fort_Wayne on 2008-12-31, America/New_York on 2040-12-31).
// There the next offset change within two days is found by probing hourly
// and bisecting to the second, and confirm asks for every firing to be
// checked against the exact mapping.
func (o Occurrences) period(at time.Time) (start, end time.Time, confirm bool) {
	start, end = at.In(o.Location).ZoneBounds()
	if !start.After(at) && (end.IsZero() || end.After(at)) {
		return start, end, false
	}
	_, offset := at.In(o.Location).Zone()
	low := at
	for probe := 0; probe < 48; probe++ {
		high := low.Add(time.Hour)
		if _, changed := high.In(o.Location).Zone(); changed != offset {
			for high.Sub(low) > time.Second {
				middle := low.Add(high.Sub(low) / 2)
				if _, now := middle.In(o.Location).Zone(); now == offset {
					low = middle
				} else {
					high = middle
				}
			}
			return time.Time{}, high.Truncate(time.Second), true
		}
		low = high
	}
	return time.Time{}, low, true
}

// walk scans wall-clock times after the instant after, under one constant
// offset, for the first firing before stop. start and end bound the period
// the offset holds in (zero when unbounded); confirm checks every firing
// against the exact mapping, not only those near start or end.
func (o Occurrences) walk(after, start, end, stop time.Time, offset int, confirm bool) (Occurrence, bool) {
	shift := time.Duration(offset) * time.Second
	wall := after.UTC().Add(shift).Truncate(time.Minute).Add(time.Minute)
	day := time.Date(wall.Year(), wall.Month(), wall.Day(), 0, 0, 0, 0, time.UTC)
	hour, minute := wall.Hour(), wall.Minute()
	for day.Add(-shift).Before(stop) {
		if o.Spec.dateMatches(day.Year(), day.Month(), day.Day()) {
			for {
				h, m, ok := o.Spec.timeFrom(hour, minute)
				if !ok {
					break
				}
				candidate := day.Add(time.Duration(h)*time.Hour + time.Duration(m)*time.Minute)
				instant := candidate.Add(-shift)
				if !instant.Before(stop) {
					return Occurrence{}, false
				}
				near := confirm || (!start.IsZero() && instant.Sub(start) < nearTransition) || (!end.IsZero() && end.Sub(instant) < nearTransition)
				if !near || o.denotes(candidate, instant) {
					return Occurrence{Instant: instant, Wall: candidate.Format(wallLayout)}, true
				}
				hour, minute = h, m+1
			}
		}
		day = day.AddDate(0, 0, 1)
		hour, minute = 0, 0
	}
	return Occurrence{}, false
}

// timeFrom returns the first scheduled hour and minute at or after hour:minute
// on the same day; minute may be 60, meaning the next hour.
func (s Spec) timeFrom(hour, minute int) (int, int, bool) {
	if minute < 60 && s.hours&(1<<uint(hour)) != 0 {
		if rest := s.minutes &^ (uint64(1)<<uint(minute) - 1); rest != 0 {
			return hour, bits.TrailingZeros64(rest), true
		}
	}
	if later := s.hours &^ (uint32(1)<<uint(hour+1) - 1); later != 0 {
		return bits.TrailingZeros32(later), bits.TrailingZeros64(s.minutes), true
	}
	return 0, 0, false
}

// denotes reports whether a wall-clock time fires at instant under the
// policies: false for the later instant of a fold fired once.
func (o Occurrences) denotes(wall, instant time.Time) bool {
	for _, candidate := range o.instants(wall.Year(), wall.Month(), wall.Day(), wall.Hour(), wall.Minute()) {
		if candidate.Equal(instant) {
			return true
		}
	}
	return false
}

// shifted returns the firing a spring-forward gap ending at the transition
// at contributes under GapShift: the gap's first scheduled wall-clock time,
// fired at the transition.
func (o Occurrences) shifted(at time.Time) (Occurrence, bool) {
	if o.Gap != GapShift {
		return Occurrence{}, false
	}
	_, before := at.Add(-time.Second).In(o.Location).Zone()
	_, after := at.In(o.Location).Zone()
	if after <= before {
		return Occurrence{}, false
	}
	from := at.UTC().Add(time.Duration(before) * time.Second)
	to := at.UTC().Add(time.Duration(after) * time.Second)
	wall := from.Truncate(time.Minute)
	if wall.Before(from) {
		wall = wall.Add(time.Minute)
	}
	day := time.Date(wall.Year(), wall.Month(), wall.Day(), 0, 0, 0, 0, time.UTC)
	hour, minute := wall.Hour(), wall.Minute()
	for day.Before(to) {
		if o.Spec.dateMatches(day.Year(), day.Month(), day.Day()) {
			if h, m, ok := o.Spec.timeFrom(hour, minute); ok {
				candidate := day.Add(time.Duration(h)*time.Hour + time.Duration(m)*time.Minute)
				if !candidate.Before(to) {
					return Occurrence{}, false
				}
				if fired := o.instants(candidate.Year(), candidate.Month(), candidate.Day(), candidate.Hour(), candidate.Minute()); len(fired) == 1 {
					return Occurrence{Instant: fired[0], Wall: candidate.Format(wallLayout)}, true
				}
				return Occurrence{}, false
			}
		}
		day = day.AddDate(0, 0, 1)
		hour, minute = 0, 0
	}
	return Occurrence{}, false
}

// Between returns the firings in (after, until]: all of them when there are
// at most limit, otherwise the latest limit, with how many earlier firings
// were left out. The count is exact up to countLimit and is clamped there;
// once it is reached the scan jumps close to until instead of enumerating
// every remaining firing.
func (o Occurrences) Between(after, until time.Time, limit, countLimit int) ([]Occurrence, int) {
	var window occurrenceWindow
	skipped := 0
	capped := false
	hadOccurrence := false
	cursor := after
	for {
		next, ok := o.Next(cursor)
		if !ok || next.Instant.After(until) {
			return window.result(hadOccurrence), skipped
		}
		hadOccurrence = true
		cursor = next.Instant
		if limit > 0 && window.size < limit {
			window.push(next, limit)
			continue
		}
		if capped {
			window.push(next, limit)
			continue
		}
		window.push(next, limit)
		skipped++
		if skipped < countLimit {
			continue
		}
		capped = true
		if start, ok := o.jumpBack(until, limit); ok && start.After(cursor) {
			cursor = start
			window.reset()
			hadOccurrence = false
		}
	}
}

// occurrenceWindow keeps only the newest limit occurrences. Its storage
// grows with observed output, never from the caller-provided limit, and once
// full it reuses the oldest slot instead of shifting the window on every
// missed firing.
type occurrenceWindow struct {
	values []Occurrence
	head   int
	size   int
}

func (w *occurrenceWindow) push(occurrence Occurrence, limit int) {
	if limit <= 0 {
		return
	}
	if w.size < limit && w.size == len(w.values) {
		capacity := 4
		if len(w.values) > 0 {
			if len(w.values) > int(^uint(0)>>1)/2 {
				capacity = limit
			} else {
				capacity = len(w.values) * 2
			}
		}
		if capacity > limit {
			capacity = limit
		}
		values := make([]Occurrence, capacity)
		if w.size > 0 {
			first := w.size
			if remaining := len(w.values) - w.head; first > remaining {
				first = remaining
			}
			copy(values, w.values[w.head:w.head+first])
			copy(values[first:], w.values[:w.size-first])
		}
		w.values, w.head = values, 0
	}
	if w.size < limit {
		index := w.head + w.size
		if index >= len(w.values) {
			index -= len(w.values)
		}
		w.values[index] = occurrence
		w.size++
		return
	}
	w.values[w.head] = occurrence
	w.head++
	if w.head == len(w.values) {
		w.head = 0
	}
}

func (w *occurrenceWindow) reset() {
	clear(w.values)
	w.head, w.size = 0, 0
}

func (w *occurrenceWindow) result(hadOccurrence bool) []Occurrence {
	if w.size == 0 {
		if hadOccurrence {
			return []Occurrence{}
		}
		return nil
	}
	result := make([]Occurrence, w.size)
	first := w.size
	if remaining := len(w.values) - w.head; first > remaining {
		first = remaining
	}
	copy(result, w.values[w.head:w.head+first])
	copy(result[first:], w.values[:w.size-first])
	return result
}

// jumpBack finds an instant before until with more than limit firings
// between it and until, so a long catch-up does not enumerate every missed
// firing.
func (o Occurrences) jumpBack(until time.Time, limit int) (time.Time, bool) {
	for span := time.Hour; span <= time.Duration(searchDays)*24*time.Hour; span *= 2 {
		start := until.Add(-span)
		count := 0
		cursor := start
		for count <= limit {
			next, ok := o.Next(cursor)
			if !ok || next.Instant.After(until) {
				break
			}
			count++
			cursor = next.Instant
		}
		if count > limit {
			return start, true
		}
	}
	return time.Time{}, false
}
