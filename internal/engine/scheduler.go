package engine

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrSaturated = errors.New("admission_saturated")
	ErrClosed    = errors.New("admission_closed")
	ErrLimit     = errors.New("admission_limit")
)

type AdmissionLimits struct {
	Workers         int
	QueueCapacity   int
	PerTenantActive int
	PerTenantQueued int
	MaxAttempts     int
	MaxChildDepth   int
}

type Request struct {
	Tenant     string
	Attempt    int
	ChildDepth int
	Run        func(context.Context) (any, error)
}

type AdmissionResult struct {
	Value any
	Err   error
}

type Scheduler struct {
	mu      sync.Mutex
	limits  AdmissionLimits
	queues  map[string][]queuedRequest
	order   []string
	next    int
	last    string
	running map[string]int
	total   int
	closed  bool
	notify  chan struct{}
	workers sync.WaitGroup
}

type queuedRequest struct {
	ctx      context.Context
	request  Request
	response chan AdmissionResult
}

func NewScheduler(limits AdmissionLimits) (*Scheduler, error) {
	if limits.Workers < 1 || limits.QueueCapacity < 1 || limits.PerTenantActive < 1 || limits.PerTenantQueued < 1 || limits.MaxAttempts < 1 || limits.MaxChildDepth < 0 {
		return nil, ErrLimit
	}
	scheduler := &Scheduler{limits: limits, queues: map[string][]queuedRequest{}, running: map[string]int{}, notify: make(chan struct{}, 1)}
	for index := 0; index < limits.Workers; index++ {
		scheduler.workers.Add(1)
		go scheduler.worker()
	}
	return scheduler, nil
}

func (s *Scheduler) Submit(ctx context.Context, request Request) (<-chan AdmissionResult, error) {
	if ctx == nil || request.Tenant == "" || request.Run == nil || request.Attempt < 1 || request.Attempt > s.limits.MaxAttempts || request.ChildDepth > s.limits.MaxChildDepth {
		return nil, ErrLimit
	}
	response := make(chan AdmissionResult, 1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	if s.total >= s.limits.QueueCapacity {
		return nil, ErrSaturated
	}
	if len(s.queues[request.Tenant]) >= s.limits.PerTenantQueued {
		return nil, ErrSaturated
	}
	if _, exists := s.queues[request.Tenant]; !exists {
		s.order = append(s.order, request.Tenant)
		if len(s.order) > 1 && s.last != "" && s.next == tenantIndex(s.order, s.last) {
			s.next = len(s.order) - 1
		}
	}
	s.queues[request.Tenant] = append(s.queues[request.Tenant], queuedRequest{ctx: ctx, request: request, response: response})
	s.total++
	s.signal()
	return response, nil
}

func (s *Scheduler) Shutdown() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.workers.Wait()
		return
	}
	s.closed = true
	for tenant, queue := range s.queues {
		for _, request := range queue {
			request.response <- AdmissionResult{Err: ErrClosed}
			close(request.response)
		}
		delete(s.queues, tenant)
	}
	s.total = 0
	s.signal()
	s.mu.Unlock()
	s.workers.Wait()
}

func (s *Scheduler) signal() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *Scheduler) worker() {
	defer s.workers.Done()
	for {
		request, ok := s.nextRequest()
		if !ok {
			return
		}
		if request.ctx.Err() != nil {
			request.response <- AdmissionResult{Err: request.ctx.Err()}
			close(request.response)
			s.finish(request.request.Tenant)
			continue
		}
		value, err := request.request.Run(request.ctx)
		if request.ctx.Err() != nil {
			err = request.ctx.Err()
			value = nil
		}
		request.response <- AdmissionResult{Value: value, Err: err}
		close(request.response)
		s.finish(request.request.Tenant)
	}
}

func (s *Scheduler) nextRequest() (queuedRequest, bool) {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return queuedRequest{}, false
		}
		for attempt := 0; attempt < len(s.order); attempt++ {
			index := (s.next + attempt) % len(s.order)
			tenant := s.order[index]
			queue := s.queues[tenant]
			if len(queue) == 0 || s.running[tenant] >= s.limits.PerTenantActive {
				continue
			}
			request := queue[0]
			s.queues[tenant] = queue[1:]
			s.running[tenant]++
			s.next = (index + 1) % len(s.order)
			s.last = tenant
			s.mu.Unlock()
			return request, true
		}
		s.mu.Unlock()
		<-s.notify
	}
}

func tenantIndex(order []string, wanted string) int {
	for index, tenant := range order {
		if tenant == wanted {
			return index
		}
	}
	return -1
}

func (s *Scheduler) finish(tenant string) {
	s.mu.Lock()
	if s.running[tenant] > 0 {
		s.running[tenant]--
	}
	s.total--
	s.signal()
	s.mu.Unlock()
}
