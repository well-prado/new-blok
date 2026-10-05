//go:build !windows

package devtool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// awaitTimeout bounds one awaited event. The first build of a fresh
// application compiles the framework, which on a shared machine with a cold
// cache can take a while.
const awaitTimeout = 4 * time.Minute

type devFixtureFile struct {
	Cases []devCase `json:"cases"`
}

type devCase struct {
	ID      string   `json:"id"`
	Layouts []string `json:"layouts"`
	Options struct {
		BackoffMS     int64 `json:"backoffMs"`
		MaxBackoffMS  int64 `json:"maxBackoffMs"`
		StableAfterMS int64 `json:"stableAfterMs"`
		StopGraceMS   int64 `json:"stopGraceMs"`
	} `json:"options"`
	Steps    []devStep `json:"steps"`
	Expected struct {
		Builds             int     `json:"builds"`
		FailedBuilds       int     `json:"failedBuilds"`
		Starts             *int    `json:"starts"`
		MaxStarts          int     `json:"maxStarts"`
		Stops              int     `json:"stops"`
		Exits              *int    `json:"exits"`
		MinExits           int     `json:"minExits"`
		Regenerated        int     `json:"regenerated"`
		StopTimeouts       int     `json:"stopTimeouts"`
		StopTimeoutMS      []int64 `json:"stopTimeoutMs"`
		EffectRecords      int     `json:"effectRecords"`
		RestartDelaysMS    []int64 `json:"restartDelaysMs"`
		ExitOutputContains string  `json:"exitOutputContains"`
		Diagnostics        []struct {
			Code         string `json:"code"`
			Source       string `json:"source"`
			SourcePrefix string `json:"sourcePrefix"`
		} `json:"diagnostics"`
	} `json:"expected"`
}

type devStep struct {
	Await      string        `json:"await"`
	Build      int           `json:"build"`
	Running    *int          `json:"running"`
	Post       int           `json:"post"`
	Status     int           `json:"status"`
	TotalCents int64         `json:"totalCents"`
	Edits      []fixtureEdit `json:"edits"`
	Repeat     int           `json:"repeat"`
	EveryMS    int           `json:"everyMs"`
	ObserveMS  int           `json:"observeMs"`
	Outside    string        `json:"outside"`
	Link       string        `json:"link"`
	Unlink     string        `json:"unlink"`
}

// timedEvent is an event and when the harness received it.
type timedEvent struct {
	DevEvent
	at time.Time
}

// devSession is one Dev call under test.
type devSession struct {
	t      *testing.T
	mu     sync.Mutex
	events []timedEvent
	cursor int
	wake   chan struct{}
	cancel context.CancelFunc
	done   chan struct{}
	code   int
	err    error
	output bytes.Buffer
}

func startDev(t *testing.T, options DevOptions) *devSession {
	t.Helper()
	session := &devSession{t: t, wake: make(chan struct{}, 1), done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	session.cancel = cancel
	options.Emit = func(event DevEvent) error {
		session.mu.Lock()
		session.events = append(session.events, timedEvent{event, time.Now()})
		session.mu.Unlock()
		select {
		case session.wake <- struct{}{}:
		default:
		}
		return nil
	}
	if options.AppStdout == nil {
		options.AppStdout = &lockedWriter{mu: &session.mu, w: &session.output}
		options.AppStderr = options.AppStdout
	}
	go func() {
		defer close(session.done)
		session.code, session.err = Dev(ctx, options)
	}()
	t.Cleanup(func() {
		session.cancel()
		select {
		case <-session.done:
		case <-time.After(2 * time.Minute):
			t.Error("blok dev did not return after its context was canceled")
		}
	})
	return session
}

type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(data []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(data)
}

// await returns the next event, after the last one awaited, that matches.
func (s *devSession) await(match func(DevEvent) bool, what string) timedEvent {
	s.t.Helper()
	deadline := time.After(awaitTimeout)
	for {
		s.mu.Lock()
		for index := s.cursor; index < len(s.events); index++ {
			if match(s.events[index].DevEvent) {
				s.cursor = index + 1
				event := s.events[index]
				s.mu.Unlock()
				return event
			}
		}
		s.mu.Unlock()
		select {
		case <-s.wake:
		case <-s.done:
			s.t.Fatalf("blok dev ended (exit %d) before %s\n%s", s.code, what, s.dump())
		case <-deadline:
			s.t.Fatalf("no %s within %s\n%s", what, awaitTimeout, s.dump())
		}
	}
}

// stop ends the session as Ctrl+C would and returns every event.
func (s *devSession) stop() []timedEvent {
	s.t.Helper()
	s.cancel()
	select {
	case <-s.done:
	case <-time.After(time.Minute):
		s.t.Fatalf("blok dev did not stop\n%s", s.dump())
	}
	if s.code != ExitInterrupted || s.err != nil {
		s.t.Fatalf("exit %d (%v); want %d\n%s", s.code, s.err, ExitInterrupted, s.dump())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]timedEvent(nil), s.events...)
}

func (s *devSession) dump() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out bytes.Buffer
	for _, event := range s.events {
		_ = WriteDevJSON(&out, event.DevEvent)
	}
	fmt.Fprintf(&out, "--- application output ---\n%s", s.output.String())
	return out.String()
}

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().String()
}

// postQuote sends one quote request to the application.
func postQuote(t *testing.T, address string, quantity int) (int, map[string]any) {
	t.Helper()
	client := http.Client{Timeout: 30 * time.Second}
	var response *http.Response
	var err error
	// A started application may not be listening yet: retry a refused
	// connection, never a request the application received.
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		response, err = client.Post("http://"+address+"/quotes", "application/json", strings.NewReader(fmt.Sprintf(`{"sku":"coffee","quantity":%d}`, quantity)))
		if err == nil || !errors.Is(err, syscall.ECONNREFUSED) || time.Now().After(deadline) {
			break
		}
	}
	if err != nil {
		t.Fatalf("POST /quotes: %v", err)
	}
	defer response.Body.Close()
	var body map[string]any
	_ = json.NewDecoder(response.Body).Decode(&body)
	return response.StatusCode, body
}

// assertNoProcesses requires every application process blok dev started,
// and everything in its process group, to be gone.
func assertNoProcesses(t *testing.T, events []timedEvent, extra ...int) {
	t.Helper()
	pids := append([]int(nil), extra...)
	for _, event := range events {
		if event.Event == EventAppStarted {
			pids = append(pids, event.PID)
		}
	}
	if len(pids) == 0 {
		t.Fatal("no application process was started")
	}
	for _, pid := range pids {
		for deadline := time.Now().Add(10 * time.Second); alive(pid) || syscall.Kill(-pid, 0) == nil; time.Sleep(50 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("process %d or its group outlived blok dev", pid)
			}
		}
	}
}

func loadDevFixtures(t *testing.T) []devCase {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repoRoot(t), "testdata", "tooling", "dev", "fixtures.json"))
	if err != nil {
		t.Fatal(err)
	}
	var file devFixtureFile
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatal(err)
	}
	if len(file.Cases) == 0 {
		t.Fatal("no dev fixtures")
	}
	return file.Cases
}

func millis(value int64) time.Duration { return time.Duration(value) * time.Millisecond }

// TestDevFixtures runs every predeclared blok dev case against a real
// scaffolded application in each listed layout, through real builds,
// real processes and real HTTP requests.
func TestDevFixtures(t *testing.T) {
	for _, item := range loadDevFixtures(t) {
		for _, layout := range item.Layouts {
			t.Run(item.ID+"/"+layout, func(t *testing.T) { runDevCase(t, item, layout) })
		}
	}
}

func runDevCase(t *testing.T, item devCase, layout string) {
	dir := scaffolded(t, layout)
	shadow := filepath.Join(t.TempDir(), "shadow")
	copyTree(t, dir, shadow)
	outside := filepath.Join(filepath.Dir(dir), "outside.go")
	names := placeholders(layout)
	address := freeAddress(t)
	session := startDev(t, DevOptions{
		Options: Options{Root: dir}, AppEnv: append(os.Environ(), "ADDR="+address),
		Backoff: millis(item.Options.BackoffMS), MaxBackoff: millis(item.Options.MaxBackoffMS),
		StableAfter: millis(item.Options.StableAfterMS), StopGrace: millis(item.Options.StopGraceMS),
	})
	for index, step := range item.Steps {
		switch {
		case step.Await != "":
			what := fmt.Sprintf("step %d: %s of build %d", index, step.Await, step.Build)
			session.await(func(event DevEvent) bool {
				return event.Event == step.Await && event.Build == step.Build && (step.Running == nil || event.Running == *step.Running)
			}, what)
		case step.Post > 0:
			status, body := postQuote(t, address, step.Post)
			if total, _ := body["totalCents"].(float64); status != step.Status || int64(total) != step.TotalCents {
				t.Fatalf("step %d: status %d body %v; want %d totalCents %d\n%s", index, status, body, step.Status, step.TotalCents, session.dump())
			}
		case len(step.Edits) > 0:
			for repeat := range max(step.Repeat, 1) {
				edits := append([]fixtureEdit(nil), step.Edits...)
				for edit := range edits {
					edits[edit].Append = strings.ReplaceAll(edits[edit].Append, "{n}", strconv.Itoa(repeat))
				}
				applyEdits(t, dir, edits, names)
				applyEdits(t, shadow, edits, names)
				time.Sleep(millis(int64(step.EveryMS)))
			}
		case step.Outside != "":
			if err := os.WriteFile(outside, []byte(step.Outside), 0o644); err != nil {
				t.Fatal(err)
			}
		case step.Link != "":
			for _, root := range []string{dir, shadow} {
				if err := os.Symlink(outside, filepath.Join(root, filepath.FromSlash(step.Link))); err != nil {
					t.Fatal(err)
				}
			}
		case step.Unlink != "":
			for _, root := range []string{dir, shadow} {
				if err := os.Remove(filepath.Join(root, filepath.FromSlash(step.Unlink))); err != nil {
					t.Fatal(err)
				}
			}
		case step.ObserveMS > 0:
			time.Sleep(millis(int64(step.ObserveMS)))
		default:
			t.Fatalf("step %d does nothing", index)
		}
	}
	events := session.stop()
	assertNoProcesses(t, events)
	checkDevExpectations(t, item, events, names, session)
	if got := effects(treeDigest(t, shadow), treeDigest(t, dir)); got != item.Expected.EffectRecords {
		t.Errorf("effect records %d, want %d", got, item.Expected.EffectRecords)
	}
}

func checkDevExpectations(t *testing.T, item devCase, events []timedEvent, names *strings.Replacer, session *devSession) {
	t.Helper()
	want := item.Expected
	count := map[string]int{}
	regenerated, stopTimeouts := 0, 0
	var delays []int64
	var diagnostics []string
	var buildDone time.Time
	for _, event := range events {
		count[event.Event]++
		regenerated += len(event.Regenerated)
		switch event.Event {
		case EventBuildSucceeded:
			buildDone = event.at
		case EventRestartScheduled:
			delays = append(delays, event.DelayMS)
		case EventBuildFailed, EventWatchFailed:
			for _, problem := range event.Diagnostics {
				diagnostics = append(diagnostics, problem.Code+" "+problem.Source)
			}
		case EventAppStopped:
			for _, problem := range event.Diagnostics {
				if problem.Code != "dev_app_stop_timeout" {
					continue
				}
				stopTimeouts++
				// The first timeout follows a build; it must take at least
				// the grace and not much longer.
				if len(want.StopTimeoutMS) == 2 && stopTimeouts == 1 {
					took := event.at.Sub(buildDone)
					if took < millis(want.StopTimeoutMS[0]) || took > millis(want.StopTimeoutMS[1]) {
						t.Errorf("stop took %s, want within %v ms", took, want.StopTimeoutMS)
					}
				}
			}
		case EventAppExited:
			if want.ExitOutputContains != "" && !strings.Contains(strings.Join(event.Output, "\n"), want.ExitOutputContains) {
				t.Errorf("app-exited output %q lacks %q", event.Output, want.ExitOutputContains)
			}
			if len(event.Diagnostics) != 1 || event.Diagnostics[0].Code != "dev_app_exited" {
				t.Errorf("app-exited diagnostics %+v", event.Diagnostics)
			}
		}
	}
	check := func(name string, got, wanted int) {
		if got != wanted {
			t.Errorf("%s: %d, want %d", name, got, wanted)
		}
	}
	check("builds", count[EventBuildStarted], want.Builds)
	check("failed builds", count[EventBuildFailed], want.FailedBuilds)
	check("stops", count[EventAppStopped], want.Stops)
	check("regenerated", regenerated, want.Regenerated)
	check("stop timeouts", stopTimeouts, want.StopTimeouts)
	if want.Starts != nil {
		check("starts", count[EventAppStarted], *want.Starts)
	}
	if want.MaxStarts > 0 && count[EventAppStarted] > want.MaxStarts {
		t.Errorf("starts: %d, want at most %d", count[EventAppStarted], want.MaxStarts)
	}
	if want.Exits != nil {
		check("exits", count[EventAppExited], *want.Exits)
	}
	if count[EventAppExited] < want.MinExits {
		t.Errorf("exits: %d, want at least %d", count[EventAppExited], want.MinExits)
	}
	if len(delays) < len(want.RestartDelaysMS) || fmt.Sprint(delays[:len(want.RestartDelaysMS)]) != fmt.Sprint(want.RestartDelaysMS) {
		t.Errorf("restart delays %v, want prefix %v", delays, want.RestartDelaysMS)
	}
	for _, delay := range delays {
		if item.Options.MaxBackoffMS > 0 && delay > item.Options.MaxBackoffMS {
			t.Errorf("restart delay %d exceeds the bound %d", delay, item.Options.MaxBackoffMS)
		}
	}
	// Every diagnostic is expected, and every expected one appears once.
	used := make([]bool, len(diagnostics))
	for _, expected := range want.Diagnostics {
		found := false
		for index, got := range diagnostics {
			code, source, _ := strings.Cut(got, " ")
			matches := code == expected.Code && (source == names.Replace(expected.Source) || (expected.SourcePrefix != "" && strings.HasPrefix(source, names.Replace(expected.SourcePrefix))))
			if !used[index] && matches {
				used[index], found = true, true
				break
			}
		}
		if !found {
			t.Errorf("diagnostic %s %s%s not reported; got %q", expected.Code, expected.Source, expected.SourcePrefix, diagnostics)
		}
	}
	for index, got := range diagnostics {
		if !used[index] {
			t.Errorf("unexpected diagnostic %q", got)
		}
	}
	if t.Failed() {
		t.Log(session.dump())
	}
}
