package cron

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
)

type steppedClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *steppedClock) Now() time.Time                       { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *steppedClock) After(time.Duration) <-chan time.Time { return make(chan time.Time) }
func (c *steppedClock) set(at time.Time)                     { c.mu.Lock(); defer c.mu.Unlock(); c.now = at }

type acceptAll struct{}

func (acceptAll) Submit(context.Context, trigger.Submission) (bool, error) { return true, nil }

// neverSettled keeps every occurrence's work running, so coalesce keeps
// replacing the held occurrence.
type neverSettled struct{}

func (neverSettled) Settled(context.Context, string) (bool, error) { return false, nil }

// TestReadersDoNotRaceTicks reads what Run's wait and Next read (next,
// pending, retry and unwritten cursor state) in a loop that never ticks,
// while ticks change it. Under -race any unguarded write is reported.
func TestReadersDoNotRaceTicks(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "cron.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	start := time.Date(2026, 10, 3, 10, 30, 0, 0, time.UTC)
	clock := &steppedClock{now: start}
	s, err := New(context.Background(), database, acceptAll{}, neverSettled{}, clock)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		_, err := s.Add(context.Background(), Schedule{Name: fmt.Sprintf("held-%d", i), Spec: "* * * * *", TimeZone: "UTC", Kind: "report.build",
			Payload: json.RawMessage(`{}`), InputSchema: []byte(`{"type":"object"}`), Principal: trigger.Principal{ID: "schedule:reports"}, Overlap: OverlapCoalesce})
		if err != nil {
			t.Fatal(err)
		}
	}
	stop := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = s.wait()
				s.Next("held-0")
			}
		}
	}()
	held := 0
	for minute := 1; minute <= 20; minute++ {
		clock.set(start.Add(time.Duration(minute) * time.Minute))
		results, err := s.Tick(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range results {
			if r.Held != nil {
				held++
			}
		}
	}
	close(stop)
	readers.Wait()
	if held == 0 {
		t.Fatal("no occurrence was ever held; the test does not exercise pending")
	}
}

// TestHostTickWakesRun: a host Tick may change what Run waits for (a retry,
// a failed cursor write), so it wakes Run; Run's own tick does not.
func TestHostTickWakesRun(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "cron.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	s, err := New(context.Background(), database, acceptAll{}, nil, &steppedClock{now: time.Date(2026, 10, 3, 10, 30, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	drain := func() {
		select {
		case <-s.wake:
		default:
		}
	}
	drain()
	if _, err := s.tick(context.Background()); err != nil || len(s.wake) != 0 {
		t.Fatalf("Run's tick woke Run: err=%v wake=%d", err, len(s.wake))
	}
	if _, err := s.Tick(context.Background()); err != nil || len(s.wake) != 1 {
		t.Fatalf("a host Tick did not wake Run: err=%v wake=%d", err, len(s.wake))
	}
}
