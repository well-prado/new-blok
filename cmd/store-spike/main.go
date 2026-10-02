// Command store-spike records the executable backend comparison for E07-T01.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/well-prado/new-blok/store/sqlite"
	"go.etcd.io/bbolt"
)

type report struct {
	Toolchain  string        `json:"toolchain"`
	GOOS       string        `json:"goos"`
	GOARCH     string        `json:"goarch"`
	Directory  string        `json:"directory"`
	Filesystem string        `json:"filesystem"`
	SQLite     backendReport `json:"sqlite"`
	BBolt      backendReport `json:"bbolt"`
}

type backendReport struct {
	Durability string `json:"durability"`
	Single     result `json:"single"`
	Batch      result `json:"batch"`
	Concurrent result `json:"concurrent"`
	Bytes      int64  `json:"bytes"`
}

type result struct {
	Operations int64         `json:"operations"`
	Workers    int           `json:"workers"`
	Elapsed    time.Duration `json:"elapsed"`
}

func main() {
	var operations int
	var workers int
	var output string
	flag.IntVar(&operations, "operations", 1000, "operations per benchmark")
	flag.IntVar(&workers, "workers", 4, "concurrent benchmark workers")
	flag.StringVar(&output, "output", "", "optional JSON report path")
	flag.Parse()
	if operations < 1 || workers < 1 {
		fatal("operations and workers must be positive")
	}
	ctx := context.Background()
	directory, err := os.MkdirTemp("", "new-blok-store-spike-")
	if err != nil {
		fatal(err)
	}
	defer os.RemoveAll(directory)

	sqliteReport, err := benchmarkSQLite(ctx, filepath.Join(directory, "sqlite.db"), operations, workers)
	if err != nil {
		fatal(err)
	}
	bboltReport, err := benchmarkBBolt(filepath.Join(directory, "bbolt.db"), operations, workers)
	if err != nil {
		fatal(err)
	}
	report := report{
		Toolchain:  runtime.Version(),
		GOOS:       runtime.GOOS,
		GOARCH:     runtime.GOARCH,
		Directory:  os.TempDir(),
		Filesystem: "runtime temporary directory; mount type is not inferred by this tool",
		SQLite:     sqliteReport,
		BBolt:      bboltReport,
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fatal(err)
	}
	data = append(data, '\n')
	if output != "" {
		if err := os.WriteFile(output, data, 0o644); err != nil {
			fatal(err)
		}
	}
	fmt.Print(string(data))
}

func benchmarkSQLite(ctx context.Context, path string, operations, workers int) (backendReport, error) {
	backend := sqlite.Backend{}
	database, err := backend.Open(ctx, path)
	if err != nil {
		return backendReport{}, err
	}
	defer database.Close()
	if err := initializeSQLite(ctx, database); err != nil {
		return backendReport{}, err
	}
	single, err := timeSQLite(ctx, database, operations, 1, false)
	if err != nil {
		return backendReport{}, err
	}
	batch, err := timeSQLite(ctx, database, operations, 1, true)
	if err != nil {
		return backendReport{}, err
	}
	concurrent, err := timeSQLite(ctx, database, operations, workers, false)
	if err != nil {
		return backendReport{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return backendReport{}, err
	}
	return backendReport{Durability: "WAL, synchronous=FULL, busy_timeout=5000, foreign_keys=ON", Single: single, Batch: batch, Concurrent: concurrent, Bytes: info.Size()}, nil
}

func initializeSQLite(ctx context.Context, database interface {
	WithTx(context.Context, func(*sql.Tx) error) error
}) error {
	return database.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS entries (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
		return err
	})
}

func timeSQLite(ctx context.Context, database interface {
	WithTx(context.Context, func(*sql.Tx) error) error
}, operations, workers int, batch bool) (result, error) {
	start := time.Now()
	if batch {
		err := database.WithTx(ctx, func(tx *sql.Tx) error {
			for index := 0; index < operations; index++ {
				if _, err := tx.ExecContext(ctx, "INSERT INTO entries (value) VALUES (?)", "batch"); err != nil {
					return err
				}
			}
			return nil
		})
		return result{Operations: int64(operations), Workers: 1, Elapsed: time.Since(start)}, err
	}
	var group sync.WaitGroup
	errs := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := 0; index < operations; index++ {
				if err := database.WithTx(ctx, func(tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, "INSERT INTO entries (value) VALUES (?)", "single")
					return err
				}); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		return result{Operations: int64(operations * workers), Workers: workers, Elapsed: time.Since(start)}, err
	}
	return result{Operations: int64(operations * workers), Workers: workers, Elapsed: time.Since(start)}, nil
}

func benchmarkBBolt(path string, operations, workers int) (backendReport, error) {
	database, err := bbolt.Open(path, 0o600, &bbolt.Options{NoSync: false, NoGrowSync: false})
	if err != nil {
		return backendReport{}, err
	}
	defer database.Close()
	if err := database.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("entries"))
		return err
	}); err != nil {
		return backendReport{}, err
	}
	single := time.Now()
	for index := 0; index < operations; index++ {
		if err := database.Update(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket([]byte("entries"))
			return bucket.Put([]byte(fmt.Sprintf("single-%d", index)), []byte("single"))
		}); err != nil {
			return backendReport{}, err
		}
	}
	singleResult := result{Operations: int64(operations), Workers: 1, Elapsed: time.Since(single)}
	batch := time.Now()
	if err := database.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("entries"))
		for index := 0; index < operations; index++ {
			if err := bucket.Put([]byte(fmt.Sprintf("%d", index)), []byte("batch")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return backendReport{}, err
	}
	batchResult := result{Operations: int64(operations), Workers: 1, Elapsed: time.Since(batch)}
	concurrent := time.Now()
	var group sync.WaitGroup
	errs := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			for index := 0; index < operations; index++ {
				err := database.Update(func(tx *bbolt.Tx) error {
					return tx.Bucket([]byte("entries")).Put([]byte(fmt.Sprintf("%d-%d", worker, index)), []byte("concurrent"))
				})
				if err != nil {
					errs <- err
					return
				}
			}
		}(worker)
	}
	group.Wait()
	close(errs)
	for err := range errs {
		return backendReport{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return backendReport{}, err
	}
	return backendReport{Durability: "NoSync=false, NoGrowSync=false, single-writer transactions", Single: singleResult, Batch: batchResult, Concurrent: result{Operations: int64(operations * workers), Workers: workers, Elapsed: time.Since(concurrent)}, Bytes: info.Size()}, nil
}

func fatal(err any) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
