package loopback

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/well-prado/new-blok/contract/conformance"
	"github.com/well-prado/new-blok/trigger"
)

type driver struct{ adapter *Adapter }

func (*driver) Declaration() trigger.Declaration { return Declaration }

func (d *driver) Open(_ context.Context, env conformance.TriggerEnv) error {
	adapter, err := New(env.InputSchema, env.Authenticate, func(ctx context.Context, input json.RawMessage, principal trigger.Principal) (json.RawMessage, error) {
		return env.Workflow(ctx, conformance.Call{Input: input, Principal: principal})
	})
	d.adapter = adapter
	return err
}

func (d *driver) Start(context.Context) error { d.adapter.Start(); return nil }
func (d *driver) Stop(context.Context) error  { d.adapter.Stop(); return nil }

func (d *driver) Deliver(ctx context.Context, delivery conformance.Delivery) (conformance.Outcome, error) {
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	disconnected := make(chan struct{})
	if delivery.Disconnect != nil {
		go func() {
			select {
			case <-delivery.Disconnect:
				close(disconnected)
				cancel()
			case <-callCtx.Done():
			}
		}()
	}
	output, err := d.adapter.Call(callCtx, delivery.Credential, delivery.Payload)
	select {
	case <-disconnected:
		return conformance.Outcome{Kind: conformance.Disconnected}, nil
	default:
	}
	var callErr *CallError
	if errors.As(err, &callErr) {
		return conformance.Outcome{Kind: conformance.Rejected, Code: callErr.Code}, nil
	}
	if err != nil {
		return conformance.Outcome{}, err
	}
	return conformance.Outcome{Kind: conformance.Completed, Output: output}, nil
}

func TestLoopbackAdapterPassesTriggerConformance(t *testing.T) {
	corpus, err := conformance.LoadTriggerCorpus()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conformance.RunTrigger(context.Background(), &driver{}, corpus, conformance.TriggerOptions{}); err != nil {
		t.Fatal(err)
	}
}
