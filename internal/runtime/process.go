package runtime

import (
	"context"
	"errors"
	contract "github.com/well-prado/new-blok/contract/runtime"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

type ProcessFactory struct {
	Command                   string
	Args, Env                 []string
	Dir                       string
	Address, Token, Principal string
	Capabilities              []contract.Capability
	StartupTimeout            time.Duration
}
type processConnection struct {
	Connection
	cmd    *exec.Cmd
	exited chan struct{}
	once   sync.Once
}

func (f ProcessFactory) Connect(ctx context.Context, h contract.Hello) (Connection, contract.Ready, error) {
	if f.Command == "" {
		return nil, contract.Ready{}, errors.New("worker command required")
	}
	if _, err := exec.LookPath(f.Command); err != nil {
		return nil, contract.Ready{}, errors.New("worker executable unavailable")
	}
	timeout := f.StartupTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	startup, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.Command(f.Command, f.Args...)
	cmd.Dir = f.Dir
	cmd.Env = append(os.Environ(), f.Env...)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		return nil, contract.Ready{}, errors.New("worker process startup failed")
	}
	p := &processConnection{cmd: cmd, exited: make(chan struct{})}
	go func() { _ = cmd.Wait(); close(p.exited) }()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		attempt, stop := context.WithTimeout(startup, 200*time.Millisecond)
		conn, ready, err := (GRPCFactory{Address: f.Address, Token: f.Token, Principal: f.Principal, Capabilities: f.Capabilities}).Connect(attempt, h)
		stop()
		if err == nil {
			p.Connection = conn
			go func() {
				<-p.exited
				if c, ok := conn.(*grpcConnection); ok {
					c.fail()
				}
			}()
			return p, ready, nil
		}
		select {
		case <-p.exited:
			return nil, contract.Ready{}, errors.New("worker exited before readiness")
		case <-startup.Done():
			_ = cmd.Process.Kill()
			<-p.exited
			return nil, contract.Ready{}, startup.Err()
		case <-ticker.C:
		}
	}
}
func (p *processConnection) Close(ctx context.Context) error {
	cleanup, cancel := context.WithTimeout(ctx, cleanupTimeout)
	defer cancel()
	// Transport drain must not prevent reaching process termination. A stuck
	// adapter cannot hold the child alive past the cleanup budget.
	closed := make(chan error, 1)
	go func() { closed <- p.Connection.Close(cleanup) }()
	var err error
	select {
	case err = <-closed:
	case <-cleanup.Done():
		err = cleanup.Err()
		if c, ok := p.Connection.(*grpcConnection); ok {
			c.fail()
		}
	}
	p.once.Do(func() { _ = p.cmd.Process.Signal(syscall.SIGTERM) })
	select {
	case <-p.exited:
	case <-cleanup.Done():
		_ = p.cmd.Process.Kill()
		<-p.exited
		return cleanup.Err()
	}
	return err
}
func (p *processConnection) PID() int { return p.cmd.Process.Pid }
