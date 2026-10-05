package runtime

import (
	"context"
	"errors"
	contract "github.com/well-prado/new-blok/contract/runtime"
	"os"
	"os/exec"
	"sync"
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

// errProcessOwnership reports a worker that started but could not be owned.
// It was ended before it ran.
var errProcessOwnership = errors.New("worker process ownership failed")

type processConnection struct {
	Connection
	cmd    *exec.Cmd
	owned  *processOwner
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
	owned, err := newProcessOwner()
	if err != nil {
		return nil, contract.Ready{}, errors.New("worker process ownership failed")
	}
	cmd := workerCommand(f.Command, f.Args, f.Dir, f.Env)
	// start owns the worker before any of its code runs. Without ownership
	// the supervisor could not end the worker's descendants, so start ends a
	// worker it could not own instead of running it.
	if err := owned.start(cmd); err != nil {
		owned.release()
		if errors.Is(err, errProcessOwnership) {
			return nil, contract.Ready{}, errors.New("worker process ownership failed")
		}
		return nil, contract.Ready{}, errors.New("worker process startup failed")
	}
	p := &processConnection{cmd: cmd, owned: owned, exited: make(chan struct{})}
	// exited closes only after the process has been reaped and, where the
	// platform tracks them, its remaining descendants have been ended.
	go func() { _ = cmd.Wait(); owned.release(); close(p.exited) }()
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
			owned.kill()
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
	p.once.Do(p.owned.stop)
	select {
	case <-p.exited:
	case <-cleanup.Done():
		p.owned.kill()
		<-p.exited
		return cleanup.Err()
	}
	return err
}
func (p *processConnection) PID() int { return p.cmd.Process.Pid }

// workerCommand builds the worker process exactly as Connect starts it.
// Standard streams go to the null device, never to pipes: with a pipe, Wait
// would not return until every descendant holding it had exited, so a
// surviving grandchild would keep a dead worker looking alive.
func workerCommand(name string, args []string, dir string, env []string) *exec.Cmd {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	isolateWorker(cmd)
	return cmd
}
