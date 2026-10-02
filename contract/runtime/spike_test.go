package runtime

import (
	"context"
	"encoding/json"
	"github.com/well-prado/new-blok/contract/runtime/wire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"net"
	"os"
	"runtime"
	"testing"
	"time"
)

// This is a controlled transport comparison only, not an application benchmark.
type spikeServer struct{ wire.UnimplementedWorkerServer }

func (spikeServer) Invoke(_ context.Context, c *wire.Call) (*wire.Result, error) {
	return &wire.Result{CallId: c.CallId, AttemptId: c.AttemptId, Generation: c.Generation, Output: c.Input}, nil
}
func (s spikeServer) Connect(stream wire.Worker_ConnectServer) error {
	for {
		f, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if f.GetCall() == nil {
			return ErrIncompatibleProtocol
		}
		result, _ := s.Invoke(stream.Context(), f.GetCall())
		if err := stream.Send(&wire.Frame{Body: &wire.Frame_Result{Result: result}}); err != nil {
			return err
		}
	}
}
func TestUnaryStreamingControlledSpike(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	wire.RegisterWorkerServer(server, spikeServer{})
	go server.Serve(listener)
	defer server.Stop()
	client, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	stub := wire.NewWorkerClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := stub.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	type sample struct {
		Mode        string  `json:"mode"`
		Calls       int     `json:"calls"`
		Nanoseconds []int64 `json:"nanoseconds"`
	}
	samples := []sample{}
	for _, mode := range []string{"unary", "stream"} {
		for batch := -1; batch < 5; batch++ {
			s := sample{Mode: mode, Calls: 100}
			for i := 0; i < 100; i++ {
				call := &wire.Call{CallId: "call", AttemptId: "attempt", Generation: 1, Input: []byte(`{"n":"9223372036854775807"}`)}
				start := time.Now()
				var r *wire.Result
				if mode == "unary" {
					r, err = stub.Invoke(ctx, call)
				} else {
					err = stream.Send(&wire.Frame{Body: &wire.Frame_Call{Call: call}})
					if err == nil {
						var f *wire.Frame
						f, err = stream.Recv()
						if err == nil {
							r = f.GetResult()
						}
					}
				}
				if err != nil || r == nil || string(r.Output) != string(call.Input) {
					t.Fatalf("transport parity %s: %v", mode, err)
				}
				s.Nanoseconds = append(s.Nanoseconds, time.Since(start).Nanoseconds())
			}
			if batch >= 0 {
				samples = append(samples, s)
			}
		}
	}
	if path := os.Getenv("BLOK_SPIKE_REPORT"); path != "" {
		raw, err := json.MarshalIndent(struct {
			Toolchain string   `json:"toolchain"`
			OS        string   `json:"os"`
			Arch      string   `json:"arch"`
			Topology  string   `json:"topology"`
			Warmup    int      `json:"warmup"`
			Claim     string   `json:"claim"`
			Samples   []sample `json:"samples"`
		}{runtime.Version(), runtime.GOOS, runtime.GOARCH, "one loopback TCP channel, one server process, sequential 26-byte echo", 100, "transport spike only; no application throughput or Node parity claim", samples}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
}
