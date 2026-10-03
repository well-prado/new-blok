package pubsub_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/well-prado/new-blok/trigger/pubsub"
)

// memBroker models a durable broker subscription for unit tests: stored
// messages, delivery counts, ack deadlines, delayed naks, and a bound on
// outstanding unacknowledged messages. The NATS JetStream driver is tested
// against a real broker separately.
type memBroker struct {
	mu             sync.Mutex
	now            time.Time
	ackWait        time.Duration
	maxAckPending  int
	messages       []*memMessage
	deadLetters    []string
	failAck        map[string]bool
	failDLQ        bool
	maxOutstanding int
}

type memMessage struct {
	broker    *memBroker
	id        string
	data      []byte
	sequence  int
	delivered int
	state     string // available, inflight, acked, termed
	notBefore time.Time
	deadline  time.Time
	acks      int
	terms     int
	naks      int
}

func newBroker(maxAckPending int) *memBroker {
	return &memBroker{now: time.Unix(1_800_000_000, 0), ackWait: 30 * time.Second, maxAckPending: maxAckPending, failAck: map[string]bool{}}
}

func (b *memBroker) publish(id string, data []byte) *memMessage {
	b.mu.Lock()
	defer b.mu.Unlock()
	m := &memMessage{broker: b, id: id, data: append([]byte(nil), data...), sequence: len(b.messages) + 1, state: "available"}
	b.messages = append(b.messages, m)
	return m
}

func (b *memBroker) advance(by time.Duration) { b.mu.Lock(); b.now = b.now.Add(by); b.mu.Unlock() }

func (b *memBroker) outstanding() int {
	count := 0
	for _, m := range b.messages {
		if m.state == "inflight" && b.now.Before(m.deadline) {
			count++
		}
	}
	return count
}

func (b *memBroker) Fetch(_ context.Context, limit int, _ time.Duration) ([]pubsub.Message, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var batch []pubsub.Message
	for _, m := range b.messages {
		if len(batch) >= limit || b.outstanding() >= b.maxAckPending {
			break
		}
		ready := m.state == "available" && !b.now.Before(m.notBefore)
		expired := m.state == "inflight" && !b.now.Before(m.deadline)
		if ready || expired {
			m.state, m.delivered, m.deadline = "inflight", m.delivered+1, b.now.Add(b.ackWait)
			batch = append(batch, m)
		}
	}
	b.maxOutstanding = max(b.maxOutstanding, b.outstanding())
	return batch, nil
}

func (b *memBroker) DeadLetter(_ context.Context, message pubsub.Message, reason string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failDLQ {
		return errors.New("dead-letter stream unavailable")
	}
	b.deadLetters = append(b.deadLetters, message.ID()+":"+reason)
	return nil
}

func (m *memMessage) ID() string     { return m.id }
func (m *memMessage) Data() []byte   { return m.data }
func (m *memMessage) Attempt() int   { return m.delivered }
func (m *memMessage) Cursor() string { return fmt.Sprintf("mem:%d", m.sequence) }

func (m *memMessage) Ack(context.Context) error {
	m.broker.mu.Lock()
	defer m.broker.mu.Unlock()
	if m.broker.failAck[m.id] {
		return errors.New("ack lost")
	}
	m.state = "acked"
	m.acks++
	return nil
}

func (m *memMessage) Nak(_ context.Context, delay time.Duration) error {
	m.broker.mu.Lock()
	defer m.broker.mu.Unlock()
	m.state, m.notBefore = "available", m.broker.now.Add(delay)
	m.naks++
	return nil
}

func (m *memMessage) Term(context.Context, string) error {
	m.broker.mu.Lock()
	defer m.broker.mu.Unlock()
	m.state = "termed"
	m.terms++
	return nil
}
