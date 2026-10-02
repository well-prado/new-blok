// Command worker selects only the durable worker trigger. It enqueues one job
// into a SQLite store, processes it and reports the committed state.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"

	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger/testdata/selection/quote"
	"github.com/well-prado/new-blok/trigger/worker"
)

func main() {
	ctx := context.Background()
	runner, err := quote.New()
	if err != nil {
		panic(err)
	}
	database, err := (sqlite.Backend{}).Open(ctx, os.Args[1])
	if err != nil {
		panic(err)
	}
	defer database.Close()
	queue, err := worker.New(ctx, database, nil)
	if err != nil {
		panic(err)
	}
	if err := queue.RegisterKind("quote.compute", quote.InputSchema); err != nil {
		panic(err)
	}
	if _, err := queue.Enqueue(ctx, worker.EnqueueRequest{RequestKey: "selection-1", Kind: "quote.compute", Payload: []byte(`{"sku":"coffee","quantity":2}`)}); err != nil {
		panic(err)
	}
	var output any
	if _, err := queue.ProcessOnce(ctx, func(ctx context.Context, _ *sql.Tx, job worker.Job) error {
		var request quote.Input
		if err := json.Unmarshal(job.Payload, &request); err != nil {
			return err
		}
		output, err = runner.Run(ctx, request)
		return err
	}); err != nil {
		panic(err)
	}
	job, err := queue.Get(ctx, "selection-1")
	if err != nil {
		panic(err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]any{"state": job.State, "output": output}); err != nil {
		panic(err)
	}
}
