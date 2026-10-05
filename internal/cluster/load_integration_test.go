package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store/distributed"
)

const (
	helperLimitsEnv   = "BLOK_CLUSTER_HELPER_LIMITS"
	helperEffectMSEnv = "BLOK_CLUSTER_HELPER_EFFECT_MILLIS"
)

// loadRuntime executes one counted effect per run. Every effect is appended to
// the shared ledger under "<tenant>/<sequence>", the run's unique input key.
func loadRuntime(store *distributed.Store, ledger effectLedger, limits Limits, effectDuration time.Duration) (*Runtime, error) {
	effect := node.MustDefine("fixture/sustained-effect", "1.0.0", func(ctx context.Context, input loadInput) (loadOutput, error) {
		if err := waitContext(ctx, effectDuration); err != nil {
			return loadOutput{}, err
		}
		if err := ledger.append("effect", fmt.Sprintf("%s/%d", input.Tenant, input.Sequence)); err != nil {
			return loadOutput{}, err
		}
		return loadOutput(input), nil
	}, node.Description("counted synthetic external effect under sustained load"), node.Schemas(
		[]byte(`{"type":"object","properties":{"tenant":{"type":"string"},"sequence":{"type":"integer"}},"required":["tenant","sequence"],"additionalProperties":false}`),
		[]byte(`{"type":"object","properties":{"tenant":{"type":"string"},"sequence":{"type":"integer"}},"required":["tenant","sequence"],"additionalProperties":false}`),
	), node.Effects("fixture:sustained-effect")).Any()
	program := contract.InternalProgram{WorkflowID: "sustained-load", Digest: "sha256:" + strings.Repeat("3", 64), Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "effect", Kind: "call", Node: "fixture/sustained-effect"},
		{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "effect"}}},
	}}
	return New(store, engine.New(map[string]node.Any{"fixture/sustained-effect": effect}), map[string]Workflow{"sustained-load": {Program: program, DecodeInput: decodeTyped[loadInput]}}, limits)
}

// runWorkerHelper is a full application worker: Runtime.Run owns every
// partition it can acquire until the test writes the stop file.
func runWorkerHelper() error {
	dir := os.Getenv(helperDirEnv)
	var limits Limits
	if err := json.Unmarshal([]byte(os.Getenv(helperLimitsEnv)), &limits); err != nil {
		return err
	}
	effectMillis, err := strconv.Atoi(os.Getenv(helperEffectMSEnv))
	if err != nil {
		return err
	}
	client, err := namespacedEtcdClient(strings.Split(os.Getenv(helperEndpointsEnv), ","), os.Getenv(helperNamespaceEnv))
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, err := distributed.New(ctx, client, integrationIncarnation)
	if err != nil {
		return err
	}
	runtime, err := loadRuntime(store, effectLedger(filepath.Join(dir, "ledger")), limits, time.Duration(effectMillis)*time.Millisecond)
	if err != nil {
		return err
	}
	go func() {
		_ = waitForFile(ctx, filepath.Join(dir, "stop"))
		cancel()
	}()
	if err := runtime.Run(ctx, os.Getenv(helperOwnerEnv)); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func TestStandbyWorkerBacksOffWhilePartitionsAreOwned(t *testing.T) {
	var fixture struct {
		FixtureVersion      int    `json:"fixtureVersion"`
		Synthetic           bool   `json:"synthetic"`
		Name                string `json:"name"`
		Partitions          int    `json:"partitions"`
		ObserveMillis       int    `json:"observeMillis"`
		RetryIntervalMillis int    `json:"retryIntervalMillis"`
		Expected            struct {
			MaxGrantsPerPartitionPerSecond float64 `json:"maxLeaseGrantsPerPartitionPerSecond"`
			AcquiredPartitions             int     `json:"acquiredPartitions"`
		} `json:"expected"`
	}
	readDistributedFixture(t, "standby-acquire-fixtures.json", &fixture)
	if acquireRetryInterval != time.Duration(fixture.RetryIntervalMillis)*time.Millisecond {
		t.Fatalf("acquire retry interval=%s, fixture %dms", acquireRetryInterval, fixture.RetryIntervalMillis)
	}
	holder := integrationDistributedStore(t)
	hooked := &hookedClient{Client: integrationClient(t)}
	ledger := effectLedger(filepath.Join(t.TempDir(), "ledger"))
	limits := Limits{Partitions: fixture.Partitions, PartitionAdmissions: 16, TenantAdmissions: 4, OwnerTTL: 2 * time.Second}
	standby, err := loadRuntime(integrationStoreFor(t, hooked), ledger, limits, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for partition := 0; partition < fixture.Partitions; partition++ {
		acquireWhenFree(t, ctx, holder, fmt.Sprintf("p-%04d", partition), "active-owner", 30*time.Second)
	}
	runCtx, stop := context.WithTimeout(ctx, time.Duration(fixture.ObserveMillis)*time.Millisecond)
	defer stop()
	start := time.Now()
	err = standby.Run(runCtx, "standby-owner")
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("standby Run=%v, want it to keep waiting until its context ends", err)
	}
	acquired := 0
	for partition := 0; partition < fixture.Partitions; partition++ {
		owner, err := holder.CurrentOwner(ctx, fmt.Sprintf("p-%04d", partition))
		if err != nil {
			t.Fatal(err)
		}
		if owner.ID != "active-owner" {
			acquired++
		}
	}
	grants := hooked.grant.Load()
	rate := float64(grants) / float64(fixture.Partitions) / elapsed.Seconds()
	t.Logf("standby over %s: %d lease grants for %d owned partitions = %.1f grants/partition/s", elapsed.Round(time.Millisecond), grants, fixture.Partitions, rate)
	if rate > fixture.Expected.MaxGrantsPerPartitionPerSecond || acquired != fixture.Expected.AcquiredPartitions {
		t.Fatalf("standby grant rate=%.1f/partition/s acquired=%d; fixture max %.1f acquired %d", rate, acquired, fixture.Expected.MaxGrantsPerPartitionPerSecond, fixture.Expected.AcquiredPartitions)
	}
}

type sustainedLoadFixture struct {
	FixtureVersion int    `json:"fixtureVersion"`
	Synthetic      bool   `json:"synthetic"`
	Name           string `json:"name"`
	Limits         struct {
		Partitions          int `json:"partitions"`
		PartitionAdmissions int `json:"partitionAdmissions"`
		TenantAdmissions    int `json:"tenantAdmissions"`
		OwnerTTLMillis      int `json:"ownerTTLMillis"`
	} `json:"limits"`
	Workers               int    `json:"workers"`
	IngressClients        int    `json:"ingressClients"`
	IngressSeconds        int    `json:"ingressSeconds"`
	KillWorkerAtSeconds   int    `json:"killWorkerAtSeconds"`
	NoisyTenantSubmitters int    `json:"noisyTenantSubmitters"`
	SteadyTenants         int    `json:"steadyTenants"`
	BackgroundTenants     int    `json:"backgroundTenants"`
	BurstPartition        string `json:"burstPartition"`
	BurstTenants          int    `json:"burstTenants"`
	BurstEveryMillis      int    `json:"burstEveryMillis"`
	EffectMillis          int    `json:"effectMillis"`
	DrainTimeoutSeconds   int    `json:"drainTimeoutSeconds"`
	Expected              struct {
		FailedRuns                     int     `json:"failedRuns"`
		NonTerminalRuns                int     `json:"nonTerminalRuns"`
		MaxEffectsPerRun               int     `json:"maxEffectsPerRun"`
		CompletedWithoutExactlyOne     int     `json:"completedRunsWithoutExactlyOneEffect"`
		EffectsWithoutAcceptedRun      int     `json:"effectsWithoutAcceptedRun"`
		UnexpectedIngressErrors        int     `json:"unexpectedIngressErrors"`
		SpuriousAdmissionFull          int     `json:"spuriousAdmissionFull"`
		KilledWorkerOwnedFairness      bool    `json:"killedWorkerOwnedFairnessPartition"`
		MinCompletedRuns               int     `json:"minCompletedRuns"`
		MinSteadyToNoisyCompletionRate float64 `json:"minSteadyToNoisyCompletionRatio"`
	} `json:"expected"`
}

type acceptedLoadRun struct {
	tenant string
	key    string
	runID  string
}

// TestSustainedLoadFailoverWithWorkerKill runs real worker processes
// (Runtime.Run) over every partition while two ingress clients admit work
// continuously, SIGKILLs the worker that owns the contended partition midway,
// and then audits every accepted run and every recorded external effect.
func TestSustainedLoadFailoverWithWorkerKill(t *testing.T) {
	var fixture sustainedLoadFixture
	readDistributedFixture(t, "sustained-failover-load-fixtures.json", &fixture)
	endpoints := integrationEndpoints(t)
	limits := Limits{Partitions: fixture.Limits.Partitions, PartitionAdmissions: fixture.Limits.PartitionAdmissions, TenantAdmissions: fixture.Limits.TenantAdmissions, OwnerTTL: time.Duration(fixture.Limits.OwnerTTLMillis) * time.Millisecond}
	dir := t.TempDir()
	ledger := effectLedger(filepath.Join(dir, "ledger"))
	store := integrationDistributedStore(t)
	ingress := make([]*Runtime, fixture.IngressClients)
	for index := range ingress {
		runtime, err := loadRuntime(integrationDistributedStore(t), ledger, limits, time.Duration(fixture.EffectMillis)*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		ingress[index] = runtime
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := ingress[0].Check(ctx); err != nil {
		t.Fatal(err)
	}
	encodedLimits, _ := json.Marshal(limits)
	type worker struct {
		id      string
		command *exec.Cmd
		done    chan error
	}
	workers := make([]*worker, fixture.Workers)
	for index := range workers {
		id := fmt.Sprintf("worker-%d", index)
		logFile, err := os.Create(filepath.Join(dir, id+".log"))
		if err != nil {
			t.Fatal(err)
		}
		command := exec.Command(os.Args[0], "-test.run=^$")
		command.Env = append(os.Environ(),
			helperRoleEnv+"=worker",
			helperEndpointsEnv+"="+strings.Join(endpoints, ","),
			helperNamespaceEnv+"="+integrationNamespace(t),
			helperDirEnv+"="+dir,
			helperOwnerEnv+"="+id,
			helperLimitsEnv+"="+string(encodedLimits),
			helperEffectMSEnv+"="+strconv.Itoa(fixture.EffectMillis),
		)
		command.Stdout, command.Stderr = logFile, logFile
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		workers[index] = &worker{id: id, command: command, done: make(chan error, 1)}
		go func(w *worker) { w.done <- w.command.Wait() }(workers[index])
		t.Cleanup(func() {
			_ = command.Process.Kill()
			_ = logFile.Close()
		})
	}

	const fairnessPartition = "p-0000"
	burstPartition := fixture.BurstPartition
	contended := tenantsInPartition(ingress[0], fairnessPartition, "load-tenant", 1+fixture.SteadyTenants)
	noisy, steady := contended[0], contended[1:]
	background := make([]string, 0, fixture.BackgroundTenants)
	for candidate := 0; len(background) < fixture.BackgroundTenants; candidate++ {
		tenant := fmt.Sprintf("background-%03d", candidate)
		if partition := ingress[0].Partition(tenant); partition != fairnessPartition && partition != burstPartition {
			background = append(background, tenant)
		}
	}
	var mu sync.Mutex
	accepted := make([]acceptedLoadRun, 0, 1024)
	full := map[string]int{}
	spuriousFull := make([]string, 0)
	unexpected := make([]string, 0)
	deadline := time.Now().Add(time.Duration(fixture.IngressSeconds) * time.Second)
	var submitters sync.WaitGroup
	submit := func(tenant string, submitter int) {
		defer submitters.Done()
		for sequence := 0; time.Now().Before(deadline); sequence++ {
			runtime := ingress[(submitter+sequence)%len(ingress)]
			unique := submitter*1_000_000 + sequence
			key := fmt.Sprintf("%s/%d", tenant, unique)
			input, _ := json.Marshal(loadInput{Tenant: tenant, Sequence: unique})
			admission, err := runtime.Admit(ctx, Submission{Tenant: tenant, RequestKey: key, Workflow: "sustained-load", Input: input})
			mu.Lock()
			switch {
			case err == nil && admission.Accepted:
				accepted = append(accepted, acceptedLoadRun{tenant: tenant, key: key, runID: admission.RunID})
			case errors.Is(err, distributed.ErrAdmissionFull):
				// A legitimate rejection carries the capacity read that showed
				// its partition or tenant bound exhausted.
				var capacity *CapacityError
				if !errors.As(err, &capacity) || capacity.FreePartitionSlots > 0 && capacity.FreeTenantSlots > 0 || capacity.Tenant != tenant {
					spuriousFull = append(spuriousFull, fmt.Sprintf("%s: %v", key, err))
				}
				full[tenant]++
			default:
				unexpected = append(unexpected, fmt.Sprintf("%s: admission=%+v err=%v", key, admission, err))
			}
			mu.Unlock()
			if err != nil {
				time.Sleep(20 * time.Millisecond)
			}
		}
	}
	// Bursts of distinct, single-use tenants arrive at the same instant on a
	// partition that otherwise has spare capacity: the shape in which
	// concurrent ingress nodes contend for the same free slots.
	burstOutcomes := map[string]int{}
	submitters.Add(1)
	go func() {
		defer submitters.Done()
		for burst := 0; time.Now().Before(deadline); burst++ {
			tenants := tenantsInPartition(ingress[0], burstPartition, fmt.Sprintf("burst-%03d", burst), fixture.BurstTenants)
			start := make(chan struct{})
			var group sync.WaitGroup
			for index, tenant := range tenants {
				group.Add(1)
				go func(index int, tenant string) {
					defer group.Done()
					<-start
					key := fmt.Sprintf("%s/%d", tenant, 0)
					input, _ := json.Marshal(loadInput{Tenant: tenant, Sequence: 0})
					admission, err := ingress[index%len(ingress)].Admit(ctx, Submission{Tenant: tenant, RequestKey: key, Workflow: "sustained-load", Input: input})
					mu.Lock()
					defer mu.Unlock()
					switch {
					case err == nil && admission.Accepted:
						accepted = append(accepted, acceptedLoadRun{tenant: tenant, key: key, runID: admission.RunID})
						burstOutcomes["accepted"]++
					case errors.Is(err, distributed.ErrAdmissionFull):
						var capacity *CapacityError
						if !errors.As(err, &capacity) || capacity.FreePartitionSlots > 0 && capacity.FreeTenantSlots > 0 || capacity.Tenant != tenant {
							spuriousFull = append(spuriousFull, fmt.Sprintf("%s: %v", key, err))
						}
						full[tenant]++
						burstOutcomes["admission_full"]++
					default:
						unexpected = append(unexpected, fmt.Sprintf("%s: admission=%+v err=%v", key, admission, err))
						burstOutcomes["unexpected"]++
					}
				}(index, tenant)
			}
			close(start)
			group.Wait()
			if err := waitContext(ctx, time.Duration(fixture.BurstEveryMillis)*time.Millisecond); err != nil {
				return
			}
		}
	}()
	for submitter := 0; submitter < fixture.NoisyTenantSubmitters; submitter++ {
		submitters.Add(1)
		go submit(noisy, submitter)
	}
	for index, tenant := range append(append([]string(nil), steady...), background...) {
		submitters.Add(1)
		go submit(tenant, 100+index)
	}

	time.Sleep(time.Duration(fixture.KillWorkerAtSeconds) * time.Second)
	// Ownership of the contended partition can be between owners at this
	// instant on a loaded host; wait for the current owner, bounded.
	var owner distributed.Owner
	var err error
	ownerDeadline := time.Now().Add(15 * time.Second)
	for {
		owner, err = store.CurrentOwner(ctx, fairnessPartition)
		if err == nil {
			break
		}
		if !errors.Is(err, distributed.ErrOwnershipLost) || time.Now().After(ownerDeadline) {
			t.Fatalf("fairness partition has no owner under load: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	var victim *worker
	for _, candidate := range workers {
		if candidate.id == owner.ID {
			victim = candidate
		}
	}
	if victim == nil {
		t.Fatalf("fairness partition owner %q is not one of the worker processes", owner.ID)
	}
	killedAt := time.Now()
	if err := victim.command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-victim.done
	var takeover time.Duration
	for takeover == 0 && ctx.Err() == nil {
		successor, err := store.CurrentOwner(ctx, fairnessPartition)
		if err == nil && successor.ID != victim.id && successor.Token > owner.Token {
			takeover = time.Since(killedAt)
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	submitters.Wait()

	drainDeadline := time.Now().Add(time.Duration(fixture.DrainTimeoutSeconds) * time.Second)
	for {
		pending := 0
		for partition := 0; partition < limits.Partitions; partition++ {
			active, err := store.ListActiveRunIDs(ctx, fmt.Sprintf("p-%04d", partition), limits.PartitionAdmissions)
			if err != nil {
				t.Fatal(err)
			}
			pending += len(active)
		}
		if pending == 0 {
			break
		}
		if time.Now().After(drainDeadline) {
			t.Fatalf("%d accepted runs still active %s after ingress stopped", pending, time.Duration(fixture.DrainTimeoutSeconds)*time.Second)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err := os.WriteFile(filepath.Join(dir, "stop"), []byte("stop"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, survivor := range workers {
		if survivor == victim {
			continue
		}
		select {
		case err := <-survivor.done:
			if err != nil {
				log, _ := os.ReadFile(filepath.Join(dir, survivor.id+".log"))
				t.Fatalf("worker %s exited with %v\n%s", survivor.id, err, log)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("worker %s did not stop", survivor.id)
		}
	}

	counts, err := ledger.counts()
	if err != nil {
		t.Fatal(err)
	}
	effects := counts["effect"]
	acceptedKeys := map[string]bool{}
	states := map[string]int{}
	completedByTenant := map[string]int{}
	completedWithoutOne, uncertainWithEffect, nonTerminal, maxEffects := 0, 0, 0, 0
	for _, run := range accepted {
		acceptedKeys[run.key] = true
		record, err := ingress[0].GetRun(ctx, run.tenant, run.runID)
		if err != nil {
			t.Fatal(err)
		}
		states[record.State]++
		switch record.State {
		case "completed":
			completedByTenant[run.tenant]++
			if effects[run.key] != 1 {
				completedWithoutOne++
			}
		case "uncertain":
			if effects[run.key] == 1 {
				uncertainWithEffect++
			}
		case "failed":
		default:
			nonTerminal++
		}
	}
	effectsWithoutRun, totalEffects := 0, 0
	for key, count := range effects {
		totalEffects += count
		if count > maxEffects {
			maxEffects = count
		}
		if !acceptedKeys[key] {
			effectsWithoutRun++
		}
	}
	steadyMin := -1
	for _, tenant := range steady {
		if steadyMin < 0 || completedByTenant[tenant] < steadyMin {
			steadyMin = completedByTenant[tenant]
		}
	}
	ratio := 0.0
	if completedByTenant[noisy] > 0 {
		ratio = float64(steadyMin) / float64(completedByTenant[noisy])
	}
	contendedCompletions := map[string]int{noisy: completedByTenant[noisy]}
	contendedFull := map[string]int{noisy: full[noisy]}
	for _, tenant := range steady {
		contendedCompletions[tenant], contendedFull[tenant] = completedByTenant[tenant], full[tenant]
	}
	sort.Strings(unexpected)
	t.Logf("sustained load: workers=%d ingress=%d accepted=%d states=%v effects=%d (completed=%d + uncertain-with-effect=%d) admission-full=%d (spurious %d) unexpected=%d takeover-after-kill=%s killed=%s",
		fixture.Workers, fixture.IngressClients, len(accepted), states, totalEffects, states["completed"], uncertainWithEffect, sumCounts(full), len(spuriousFull), len(unexpected), takeover.Round(time.Millisecond), victim.id)
	t.Logf("burst partition %s: %d distinct tenants per burst every %dms: outcomes=%v", burstPartition, fixture.BurstTenants, fixture.BurstEveryMillis, burstOutcomes)
	t.Logf("contended partition %s: completions=%v admission-full=%v steady/noisy ratio=%.2f", fairnessPartition, contendedCompletions, contendedFull, ratio)
	if len(spuriousFull) != fixture.Expected.SpuriousAdmissionFull {
		t.Errorf("admission_full without an exhausted-capacity read=%d (first: %v), fixture %d", len(spuriousFull), spuriousFull[:min(3, len(spuriousFull))], fixture.Expected.SpuriousAdmissionFull)
	}
	if len(unexpected) != fixture.Expected.UnexpectedIngressErrors {
		t.Errorf("unexpected ingress errors=%d (first: %v), fixture %d", len(unexpected), unexpected[:min(3, len(unexpected))], fixture.Expected.UnexpectedIngressErrors)
	}
	if states["failed"] != fixture.Expected.FailedRuns || nonTerminal != fixture.Expected.NonTerminalRuns || maxEffects > fixture.Expected.MaxEffectsPerRun || completedWithoutOne != fixture.Expected.CompletedWithoutExactlyOne || effectsWithoutRun != fixture.Expected.EffectsWithoutAcceptedRun {
		t.Errorf("failed=%d nonTerminal=%d maxEffects=%d completedWithoutExactlyOne=%d effectsWithoutRun=%d; fixture %d/%d/%d/%d/%d", states["failed"], nonTerminal, maxEffects, completedWithoutOne, effectsWithoutRun, fixture.Expected.FailedRuns, fixture.Expected.NonTerminalRuns, fixture.Expected.MaxEffectsPerRun, fixture.Expected.CompletedWithoutExactlyOne, fixture.Expected.EffectsWithoutAcceptedRun)
	}
	if totalEffects != states["completed"]+uncertainWithEffect {
		t.Errorf("effects=%d, want completed %d + uncertain-with-effect %d", totalEffects, states["completed"], uncertainWithEffect)
	}
	if !fixture.Expected.KilledWorkerOwnedFairness || takeover == 0 {
		t.Errorf("no successor took the killed worker's partition")
	}
	if states["completed"] < fixture.Expected.MinCompletedRuns || ratio < fixture.Expected.MinSteadyToNoisyCompletionRate {
		t.Errorf("completed=%d steady/noisy ratio=%.2f; fixture min %d / %.2f", states["completed"], ratio, fixture.Expected.MinCompletedRuns, fixture.Expected.MinSteadyToNoisyCompletionRate)
	}
}

func sumCounts(values map[string]int) int {
	total := 0
	for _, value := range values {
		total += value
	}
	return total
}
