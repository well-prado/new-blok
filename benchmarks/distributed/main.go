// Command distributed records bounded raw measurements against an explicitly
// selected local spike topology. It does not start or modify the services.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/well-prado/new-blok/store/distributed"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type measurement struct {
	SamplesNS []int64 `json:"samplesNs"`
	P50NS     int64   `json:"p50Ns"`
	P95NS     int64   `json:"p95Ns"`
	P99NS     int64   `json:"p99Ns"`
	MeanNS    int64   `json:"meanNs"`
}

type report struct {
	SchemaVersion int            `json:"schemaVersion"`
	Synthetic     bool           `json:"synthetic"`
	StartedAt     time.Time      `json:"startedAt"`
	Environment   map[string]any `json:"environment"`
	Workload      map[string]any `json:"workload"`
	Topology      string         `json:"topology"`
	Dependency    map[string]any `json:"dependencyCost"`
	Commit        measurement    `json:"commitLatency"`
	Read          measurement    `json:"readLatency"`
	Concurrent    measurement    `json:"concurrentCommitLatency"`
	S3Put         measurement    `json:"s3PutAndVerifyLatency"`
	S3Get         measurement    `json:"s3GetAndVerifyLatency"`
	Snapshot      measurement    `json:"snapshotExportLatency"`
	Restore       measurement    `json:"snapshotRestoreLatency"`
	SnapshotBytes int64          `json:"snapshotBytes"`
	Errors        []string       `json:"errors"`
}

func main() {
	defer func() {
		if recovered := recover(); recovered != nil {
			if err, ok := recovered.(error); ok {
				_, _ = fmt.Fprintln(os.Stderr, err)
			} else {
				_, _ = fmt.Fprintln(os.Stderr, recovered)
			}
			os.Exit(1)
		}
	}()
	run()
}

func run() {
	var count, samples, workers, payloadBytes int
	flag.IntVar(&count, "commits", 100, "unique commits in each sequential/concurrent sample (1..10000)")
	flag.IntVar(&samples, "samples", 5, "repeat count for each measurement (1..20)")
	flag.IntVar(&workers, "workers", 4, "bounded concurrent commit workers (1..32)")
	flag.IntVar(&payloadBytes, "payload-bytes", 256, "synthetic JSON payload size (1..512 KiB)")
	flag.Parse()
	if count < 1 || count > 10000 || samples < 1 || samples > 20 || workers < 1 || workers > 32 || payloadBytes < 13 || payloadBytes > distributed.MaxPayloadBytes {
		fatal(errors.New("commits must be 1..10000, samples 1..20, workers 1..32, payload-bytes 13..512 KiB"))
	}
	endpoints := strings.Split(os.Getenv("BLOK_DISTRIBUTED_ENDPOINTS"), ",")
	incarnation := os.Getenv("BLOK_DISTRIBUTED_INCARNATION")
	s3Endpoint := os.Getenv("BLOK_DISTRIBUTED_S3_ENDPOINT")
	if endpoints[0] == "" || incarnation == "" || s3Endpoint == "" {
		fatal(errors.New("set BLOK_DISTRIBUTED_ENDPOINTS, BLOK_DISTRIBUTED_INCARNATION, and BLOK_DISTRIBUTED_S3_ENDPOINT; this command never starts services"))
	}
	etcdImage := os.Getenv("BLOK_DISTRIBUTED_ETCD_IMAGE")
	if etcdImage == "" {
		etcdImage = "quay.io/coreos/etcd:v3.6.5"
	}
	s3Image := os.Getenv("BLOK_DISTRIBUTED_S3_IMAGE")
	if s3Image == "" {
		s3Image = "chrislusf/seaweedfs:4.47"
	}
	etcdIdentity, err := dockerImageIdentity(etcdImage)
	if err != nil {
		fatal(err)
	}
	s3Identity, err := dockerImageIdentity(s3Image)
	if err != nil {
		fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 3 * time.Second})
	if err != nil {
		fatal(err)
	}
	defer client.Close()
	store, err := distributed.New(ctx, client, incarnation)
	if err != nil {
		fatal(err)
	}

	var nonceBytes [12]byte
	if _, err := rand.Read(nonceBytes[:]); err != nil {
		fatal(err)
	}
	nonce := hex.EncodeToString(nonceBytes[:])
	partition := "bench-" + nonce
	owner, err := store.Acquire(ctx, partition, "distributed-benchmark", time.Hour)
	if err != nil {
		fatal(err)
	}
	payload := []byte(`{"v":"` + strings.Repeat("x", payloadBytes-8) + `"}`)

	out := report{
		SchemaVersion: 1,
		Synthetic:     true,
		StartedAt:     time.Now().UTC(),
		Environment: map[string]any{
			"goVersion": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH,
			"gomaxprocs": runtime.GOMAXPROCS(0), "logicalCPUs": runtime.NumCPU(),
			"etcdEndpoints": endpoints, "etcdImage": etcdImage, "etcdImageIdentity": etcdIdentity,
			"s3Endpoint": s3Endpoint, "s3Image": s3Image, "s3ImageIdentity": s3Identity,
		},
		Workload: map[string]any{"sequentialCommitsPerSample": count, "concurrentCommitsPerSample": count, "concurrentWorkers": workers, "samples": samples, "payloadBytes": len(payload)},
		Topology: "one Docker host; three etcd voters with separate named volumes; one SeaweedFS S3 service with one named volume; all published service ports loopback-only",
	}
	basePackages, err := nonStandardPackages("./store")
	if err != nil {
		fatal(err)
	}
	distributedPackages, err := nonStandardPackages("./store/distributed")
	if err != nil {
		fatal(err)
	}
	out.Dependency = map[string]any{
		"etcdClient":                         "go.etcd.io/etcd/client/v3 v3.6.5",
		"s3Client":                           "github.com/minio/minio-go/v7 v7.0.95",
		"storeNonStandardPackageCount":       len(basePackages),
		"distributedNonStandardPackageCount": len(distributedPackages),
		"incrementalNonStandardPackageCount": len(distributedPackages) - len(basePackages),
	}
	out.Commit = measure(samples, func(sample int) ([]int64, error) {
		values := make([]int64, 0, count)
		for i := 0; i < count; i++ {
			started := time.Now()
			err := store.Commit(ctx, owner, fmt.Sprintf("seq-%s-%02d-%06d", nonce, sample, i), "state", payload)
			values = append(values, time.Since(started).Nanoseconds())
			if err != nil {
				return values, err
			}
		}
		return values, nil
	})
	out.Read = measure(samples, func(sample int) ([]int64, error) {
		values := make([]int64, 0, count)
		for i := 0; i < count; i++ {
			started := time.Now()
			encoded, err := store.Read(ctx, partition, fmt.Sprintf("seq-%s-%02d-%06d", nonce, sample, i))
			if err == nil && encoded == nil {
				err = errors.New("committed event missing during read measurement")
			}
			values = append(values, time.Since(started).Nanoseconds())
			if err != nil {
				return values, err
			}
		}
		return values, nil
	})
	out.Concurrent = measure(samples, func(sample int) ([]int64, error) {
		values := make([]int64, count)
		var wg sync.WaitGroup
		jobs := make(chan int)
		errCh := make(chan error, 1)
		for worker := 0; worker < workers; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range jobs {
					started := time.Now()
					err := store.Commit(ctx, owner, fmt.Sprintf("par-%s-%02d-%06d", nonce, sample, i), "state", payload)
					values[i] = time.Since(started).Nanoseconds()
					if err != nil {
						select {
						case errCh <- err:
						default:
						}
					}
				}
			}()
		}
		for i := 0; i < count; i++ {
			jobs <- i
		}
		close(jobs)
		wg.Wait()
		select {
		case err := <-errCh:
			return values, err
		default:
			return values, nil
		}
	})

	bucket := "blokbench-" + nonce
	blobs, err := distributed.NewS3BlobStore(s3Endpoint, bucket, os.Getenv("BLOK_DISTRIBUTED_S3_ACCESS_KEY"), os.Getenv("BLOK_DISTRIBUTED_S3_SECRET_KEY"), false)
	if err != nil {
		fatal(err)
	}
	if err := blobs.EnsureBucket(ctx); err != nil {
		fatal(err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cleanupCancel()
		if err := blobs.RemoveBucket(cleanupCtx); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "S3 benchmark cleanup:", err)
		}
	}()
	out.S3Put = measure(samples, func(int) ([]int64, error) {
		values := make([]int64, 0, count)
		for i := 0; i < count; i++ {
			started := time.Now()
			_, err := blobs.Put(ctx, payload)
			values = append(values, time.Since(started).Nanoseconds())
			if err != nil {
				return values, err
			}
		}
		return values, nil
	})
	ref, err := blobs.Put(ctx, payload)
	if err != nil {
		fatal(err)
	}
	out.S3Get = measure(samples, func(int) ([]int64, error) {
		values := make([]int64, 0, count)
		for i := 0; i < count; i++ {
			started := time.Now()
			_, err := blobs.Get(ctx, ref)
			values = append(values, time.Since(started).Nanoseconds())
			if err != nil {
				return values, err
			}
		}
		return values, nil
	})

	for i := 0; i < samples; i++ {
		snapshotPath, size, duration, err := exportSnapshot(ctx, client)
		if err != nil {
			fatal(err)
		}
		out.Snapshot.SamplesNS = append(out.Snapshot.SamplesNS, duration.Nanoseconds())
		out.SnapshotBytes = size
		restoreDuration, err := restoreSnapshot(ctx, snapshotPath, nonce, i, etcdImage)
		_ = os.Remove(snapshotPath)
		if err != nil {
			fatal(err)
		}
		out.Restore.SamplesNS = append(out.Restore.SamplesNS, restoreDuration.Nanoseconds())
	}
	out.Snapshot.summarize()
	out.Restore.summarize()
	if err := json.NewEncoder(os.Stdout).Encode(out); err != nil {
		fatal(err)
	}
}

func measure(samples int, run func(int) ([]int64, error)) measurement {
	var out measurement
	for i := 0; i < samples; i++ {
		values, err := run(i)
		out.SamplesNS = append(out.SamplesNS, values...)
		if err != nil {
			fatal(err)
		}
	}
	out.summarize()
	return out
}

func (m *measurement) summarize() {
	if len(m.SamplesNS) == 0 {
		return
	}
	values := append([]int64(nil), m.SamplesNS...)
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	var total int64
	for _, value := range values {
		total += value
	}
	m.MeanNS = total / int64(len(values))
	m.P50NS = percentile(values, .50)
	m.P95NS = percentile(values, .95)
	m.P99NS = percentile(values, .99)
}

func percentile(sorted []int64, p float64) int64 {
	index := int(float64(len(sorted)-1) * p)
	return sorted[index]
}

func exportSnapshot(ctx context.Context, client *clientv3.Client) (string, int64, time.Duration, error) {
	started := time.Now()
	snapshot, err := client.Snapshot(ctx)
	if err != nil {
		return "", 0, time.Since(started), err
	}
	defer snapshot.Close()
	path := filepath.Join(os.TempDir(), fmt.Sprintf("blok-distributed-bench-%d.db", time.Now().UnixNano()))
	file, err := os.Create(path)
	if err != nil {
		return "", 0, time.Since(started), err
	}
	size, copyErr := io.Copy(file, snapshot)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(copyErr, syncErr, closeErr); err != nil {
		_ = os.Remove(path)
		return "", size, time.Since(started), err
	}
	return path, size, time.Since(started), nil
}

func restoreSnapshot(ctx context.Context, path, nonce string, sample int, image string) (time.Duration, error) {
	started := time.Now()
	directory, err := os.MkdirTemp("", "blok-distributed-restore-")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(directory)
	if err := copyFile(path, filepath.Join(directory, "snapshot.db")); err != nil {
		return 0, err
	}
	name := fmt.Sprintf("restore-%s-%02d", nonce[:8], sample)
	command := exec.CommandContext(ctx, "docker", "run", "--rm", "--entrypoint=/usr/local/bin/etcdutl", "-v", directory+":/restore", image, "snapshot", "restore", "/restore/snapshot.db", "--name="+name, "--data-dir=/restore/restored", "--initial-cluster="+name+"=http://"+name+":2380", "--initial-advertise-peer-urls=http://"+name+":2380")
	output, err := command.CombinedOutput()
	if err != nil {
		return time.Since(started), fmt.Errorf("etcd snapshot restore: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return time.Since(started), nil
}

func nonStandardPackages(target string) ([]string, error) {
	command := exec.Command("go", "list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", target)
	output, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("count Go package dependencies for %s: %w", target, err)
	}
	packages := make([]string, 0)
	for _, line := range strings.Split(string(output), "\n") {
		if line != "" {
			packages = append(packages, line)
		}
	}
	return packages, nil
}

func dockerImageIdentity(image string) (string, error) {
	command := exec.Command("docker", "image", "inspect", "--format", "{{.Id}}|{{json .RepoDigests}}", image)
	output, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("inspect local container image %q: %w", image, err)
	}
	return strings.TrimSpace(string(output)), nil
}

func copyFile(source, destination string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(destination)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	syncErr := out.Sync()
	closeErr := out.Close()
	return errors.Join(copyErr, syncErr, closeErr)
}

func fatal(err error) {
	panic(err)
}
