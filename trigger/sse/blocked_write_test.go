package sse_test

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// writeGateListener holds the next server-side transport write after Arm.
// Because it sits beneath net/http, the tests exercise a real HTTP response
// write while controlling exactly when the transport reports that write as
// blocked, independent of the host's TCP send-buffer size.
type writeGateListener struct {
	net.Listener
	armed     atomic.Bool
	entered   chan struct{}
	exited    chan error
	enterOnce sync.Once
	exitOnce  sync.Once
}

func newWriteGateListener() (*writeGateListener, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	return &writeGateListener{Listener: listener, entered: make(chan struct{}), exited: make(chan error, 1)}, nil
}

func (l *writeGateListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &writeGateConn{Conn: conn, gate: l, changed: make(chan struct{}), closed: make(chan struct{})}, nil
}

func (l *writeGateListener) arm() { l.armed.Store(true) }

func (l *writeGateListener) waitBlocked(t *testing.T) {
	t.Helper()
	select {
	case <-l.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP response transport did not enter the blocked write")
	}
}

func (l *writeGateListener) waitWriteExit(t *testing.T) error {
	t.Helper()
	select {
	case err := <-l.exited:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("blocked HTTP transport write did not exit")
		return nil
	}
}

type writeGateConn struct {
	net.Conn
	gate     *writeGateListener
	mu       sync.Mutex
	writeAt  time.Time
	changed  chan struct{}
	closed   chan struct{}
	close    sync.Once
	closeErr error
}

func (c *writeGateConn) Close() error {
	c.close.Do(func() {
		close(c.closed)
		c.closeErr = c.Conn.Close()
	})
	return c.closeErr
}

func (c *writeGateConn) SetWriteDeadline(deadline time.Time) error {
	if err := c.Conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	c.mu.Lock()
	c.writeAt = deadline
	close(c.changed)
	c.changed = make(chan struct{})
	c.mu.Unlock()
	return nil
}

func (c *writeGateConn) Write(p []byte) (int, error) {
	if !c.gate.armed.Load() {
		return c.Conn.Write(p)
	}
	c.gate.enterOnce.Do(func() { close(c.gate.entered) })
	finish := func(err error) (int, error) {
		c.gate.exitOnce.Do(func() { c.gate.exited <- err })
		return 0, err
	}
	for {
		c.mu.Lock()
		deadline, changed := c.writeAt, c.changed
		c.mu.Unlock()
		if deadline.IsZero() {
			select {
			case <-changed:
				continue
			case <-c.closed:
				return finish(net.ErrClosed)
			}
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return finish(os.ErrDeadlineExceeded)
		}
		timer := time.NewTimer(remaining)
		select {
		case <-timer.C:
			c.mu.Lock()
			expired := !c.writeAt.IsZero() && !time.Now().Before(c.writeAt)
			c.mu.Unlock()
			if expired {
				return finish(os.ErrDeadlineExceeded)
			}
		case <-changed:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-c.closed:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return finish(net.ErrClosed)
		}
	}
}

// blockedSubscription reads the real HTTP response headers and initial SSE
// retry frame before arming the transport gate for the next response write.
func blockedSubscription(t *testing.T, f *fixture, stream string, gate *writeGateListener) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", strings.TrimPrefix(f.http.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := fmt.Fprintf(conn, "GET /orders/%s HTTP/1.1\r\nHost: test\r\nAuthorization: Bearer alice\r\nAccept: text/event-stream\r\n\r\n", stream); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatalf("read blocked subscription response: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("blocked subscription status: %s", response.Status)
	}
	reader := bufio.NewReader(response.Body)
	retry, err := reader.ReadString('\n')
	if err != nil || !strings.HasPrefix(retry, "retry: ") {
		t.Fatalf("initial SSE retry frame: %q %v", retry, err)
	}
	blank, err := reader.ReadString('\n')
	if err != nil || blank != "\n" {
		t.Fatalf("initial SSE frame terminator: %q %v", blank, err)
	}
	gate.arm()
	return conn
}
