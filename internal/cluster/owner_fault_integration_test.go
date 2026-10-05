package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/clustertest"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store/distributed"
)

const (
	helperRoleEnv      = "BLOK_CLUSTER_HELPER"
	helperEndpointsEnv = "BLOK_CLUSTER_HELPER_ENDPOINTS"
	helperNamespaceEnv = "BLOK_CLUSTER_HELPER_NAMESPACE"
	helperDirEnv       = "BLOK_CLUSTER_HELPER_DIR"
	helperPartitionEnv = "BLOK_CLUSTER_HELPER_PARTITION"
	helperOwnerEnv     = "BLOK_CLUSTER_HELPER_OWNER"
	helperPatternEnv   = "BLOK_CLUSTER_HELPER_HOOK_PATTERN"
	helperSkipEnv      = "BLOK_CLUSTER_HELPER_HOOK_SKIP"
	helperAfterEnv     = "BLOK_CLUSTER_HELPER_HOOK_AFTER"
)

var ownerFaultLimits = Limits{Partitions: 8, PartitionAdmissions: 16, TenantAdmissions: 4, OwnerTTL: 2 * time.Second}

// ownerFaultRuntime is identical in the test process and in owner helper
// processes: a counted pure step, a counted effectful step and an output.
// inEffect, when set, runs after the effect was recorded in the ledger and
// before the node returns, i.e. inside the external effect.
func ownerFaultRuntime(store *distributed.Store, ledger effectLedger, inEffect func()) (*Runtime, error) {
	prepare := node.MustDefine("fixture/fault-prepare", "1.0.0", func(_ context.Context, input integrationInput) (integrationInput, error) {
		if err := ledger.append("pure", strconv.Itoa(input.Value)); err != nil {
			return integrationInput{}, err
		}
		return integrationInput{Value: input.Value + 1}, nil
	}, node.Description("counted pure step"), node.Schemas([]byte(waitInputSchema), []byte(waitInputSchema))).Any()
	effect := node.MustDefine("fixture/fault-effect", "1.0.0", func(_ context.Context, input integrationInput) (integrationOutput, error) {
		if err := ledger.append("effect", strconv.Itoa(input.Value)); err != nil {
			return integrationOutput{}, err
		}
		if inEffect != nil {
			inEffect()
		}
		return integrationOutput{Value: input.Value + 1}, nil
	}, node.Description("counted synthetic external effect"), node.Schemas([]byte(waitInputSchema), []byte(outputSchema)), node.Effects("fixture:fault-effect")).Any()
	program := contract.InternalProgram{WorkflowID: "owner-fault", Digest: "sha256:" + strings.Repeat("2", 64), Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "prepare", Kind: "call", Node: "fixture/fault-prepare"},
		{Index: 1, ID: "effect", Kind: "call", Node: "fixture/fault-effect", References: []contract.Reference{{Step: "prepare"}}},
		{Index: 2, ID: "output", Kind: "output", References: []contract.Reference{{Step: "effect"}}},
	}}
	return New(store, engine.New(map[string]node.Any{"fixture/fault-prepare": prepare, "fixture/fault-effect": effect}), map[string]Workflow{"owner-fault": {Program: program, DecodeInput: decodeTyped[integrationInput]}}, ownerFaultLimits)
}

func runClusterHelper(role string) error {
	switch role {
	case "owner":
		return runOwnerHelper()
	case "worker":
		return runWorkerHelper()
	default:
		return fmt.Errorf("unknown helper role %q", role)
	}
}

type ownerHelperResult struct {
	State         string `json:"state"`
	Error         string `json:"error"`
	OwnershipLost bool   `json:"ownershipLost"`
}

// runOwnerHelper is the stale owner: it acquires the partition, keeps its
// lease alive, executes one run with processOne and blocks at the configured
// transition until the test process writes the resume file (or kills it).
func runOwnerHelper() error {
	dir := os.Getenv(helperDirEnv)
	client, err := namespacedEtcdClient(strings.Split(os.Getenv(helperEndpointsEnv), ","), os.Getenv(helperNamespaceEnv))
	if err != nil {
		return err
	}
	defer client.Close()
	hooked := &hookedClient{Client: client}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	store, err := distributed.New(ctx, hooked, integrationIncarnation)
	if err != nil {
		return err
	}
	barrier := func() {
		_ = os.WriteFile(filepath.Join(dir, "reached"), []byte(strconv.Itoa(os.Getpid())), 0o600)
		_ = waitForFile(ctx, filepath.Join(dir, "resume"))
		// After a healed network partition, wait until this stale owner can
		// reach etcd again, so its next fenced transaction is decided by etcd
		// rather than lost on a dead connection.
		for ctx.Err() == nil {
			probeCtx, stop := context.WithTimeout(ctx, time.Second)
			_, _, err := store.ReadState(probeCtx, "connectivity-probe", "connectivity-probe")
			stop()
			if err == nil {
				return
			}
		}
	}
	pattern := os.Getenv(helperPatternEnv)
	var inEffect func()
	if pattern == "" {
		inEffect = barrier
	}
	runtime, err := ownerFaultRuntime(store, effectLedger(filepath.Join(dir, "ledger")), inEffect)
	if err != nil {
		return err
	}
	var owner distributed.Owner
	for owner.ID == "" {
		owner, err = store.Acquire(ctx, os.Getenv(helperPartitionEnv), os.Getenv(helperOwnerEnv), ownerFaultLimits.OwnerTTL)
		if err != nil && !errors.Is(err, distributed.ErrOwnershipLost) {
			return err
		}
		if err != nil {
			if waitErr := waitContext(ctx, 50*time.Millisecond); waitErr != nil {
				return waitErr
			}
		}
	}
	encodedOwner, _ := json.Marshal(map[string]int64{"token": owner.Token})
	if err := os.WriteFile(filepath.Join(dir, "owner.json"), encodedOwner, 0o600); err != nil {
		return err
	}
	go func() {
		for ctx.Err() == nil {
			renewCtx, stop := context.WithTimeout(ctx, 400*time.Millisecond)
			_ = store.Renew(renewCtx, owner)
			stop()
			_ = waitContext(ctx, 500*time.Millisecond)
		}
	}()
	if pattern != "" {
		skip, _ := strconv.Atoi(os.Getenv(helperSkipEnv))
		hooked.arm(pattern, skip, os.Getenv(helperAfterEnv) == "true", barrier)
	}
	record, processErr := runtime.processOne(ctx, owner)
	result := ownerHelperResult{State: record.State, OwnershipLost: errors.Is(processErr, distributed.ErrOwnershipLost)}
	if processErr != nil {
		result.Error = processErr.Error()
	}
	encoded, _ := json.Marshal(result)
	return os.WriteFile(filepath.Join(dir, "result.json"), encoded, 0o600)
}

type ownerFaultFixture struct {
	FixtureVersion int               `json:"fixtureVersion"`
	Synthetic      bool              `json:"synthetic"`
	Name           string            `json:"name"`
	Workflow       string            `json:"workflow"`
	Input          string            `json:"input"`
	Faults         map[string]string `json:"faults"`
	Stages         []struct {
		Name       string `json:"name"`
		Transition string `json:"transition"`
		Hook       struct {
			Pattern string `json:"pattern"`
			Skip    int    `json:"skip"`
			After   bool   `json:"after"`
		} `json:"hook"`
		SuccessorHold *struct {
			Pattern string `json:"pattern"`
			Skip    int    `json:"skip"`
			After   bool   `json:"after"`
		} `json:"successorHold"`
		Expected struct {
			DuringSuccessorRun *struct {
				State            string `json:"state"`
				OwnedBySuccessor bool   `json:"ownedBySuccessor"`
				ActiveRuns       int    `json:"activeRuns"`
			} `json:"duringSuccessorRun"`
			State                string         `json:"state"`
			Output               string         `json:"output"`
			Effects              map[string]int `json:"effects"`
			PureInvocations      map[string]int `json:"pureInvocations"`
			EffectDispatches     int            `json:"effectDispatches"`
			PureDispatches       int            `json:"pureDispatches"`
			SuccessorPureAttempt int            `json:"successorPureAttempt"`
		} `json:"expected"`
	} `json:"stages"`
	Invariants struct {
		ActiveRunsAfterSuccessor      int               `json:"activeRunsAfterSuccessor"`
		StaleOwnerResult              map[string]string `json:"staleOwnerResult"`
		StaleFenceEventsAfterTakeover int               `json:"staleFenceEventsAfterTakeover"`
		TerminalEvents                int               `json:"terminalEvents"`
	} `json:"invariants"`
}

// ownerHelper is a running stale owner in a separate OS process.
type ownerHelper interface {
	inject(t *testing.T)
	resume(t *testing.T)
	wait(t *testing.T) (killed bool)
	logs() string
}

type processOwnerHelper struct {
	fault   string
	dir     string
	command *exec.Cmd
	done    chan error
	logFile *os.File
}

func startProcessOwnerHelper(t *testing.T, fault string, env []string, dir string) *processOwnerHelper {
	t.Helper()
	logFile, err := os.Create(filepath.Join(dir, "helper.log"))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^$")
	command.Env = append(os.Environ(), env...)
	command.Stdout, command.Stderr = logFile, logFile
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	helper := &processOwnerHelper{fault: fault, dir: dir, command: command, done: make(chan error, 1), logFile: logFile}
	go func() { helper.done <- command.Wait() }()
	t.Cleanup(func() {
		if command.ProcessState == nil {
			_ = resumeProcess(command.Process)
			_ = command.Process.Kill()
		}
		_ = logFile.Close()
	})
	return helper
}

func (h *processOwnerHelper) inject(t *testing.T) {
	t.Helper()
	var err error
	switch h.fault {
	case "kill":
		err = h.command.Process.Kill()
	case "stop":
		err = suspendProcess(h.command.Process)
	default:
		err = fmt.Errorf("unsupported process fault %q", h.fault)
	}
	if err != nil {
		t.Fatalf("inject %s: %v", h.fault, err)
	}
}

func (h *processOwnerHelper) resume(t *testing.T) {
	t.Helper()
	if h.fault == "kill" {
		return
	}
	if err := os.WriteFile(filepath.Join(h.dir, "resume"), []byte("resume"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := resumeProcess(h.command.Process); err != nil {
		t.Fatal(err)
	}
}

func (h *processOwnerHelper) wait(t *testing.T) bool {
	t.Helper()
	select {
	case err := <-h.done:
		if h.fault == "kill" {
			return err != nil && !h.command.ProcessState.Success()
		}
		if err != nil {
			t.Fatalf("resumed owner helper failed: %v\n%s", err, h.logs())
		}
		return false
	case <-time.After(90 * time.Second):
		t.Fatalf("owner helper did not exit\n%s", h.logs())
		return false
	}
}

func (h *processOwnerHelper) logs() string {
	data, _ := os.ReadFile(filepath.Join(h.dir, "helper.log"))
	return string(data)
}

type containerOwnerHelper struct {
	name    string
	network string
	dir     string
}

func startContainerOwnerHelper(t *testing.T, binaryDir string, env []string, dir string) *containerOwnerHelper {
	t.Helper()
	network := os.Getenv("BLOK_DISTRIBUTED_ETCD_NETWORK")
	if network == "" {
		t.Fatal("BLOK_DISTRIBUTED_ETCD_NETWORK names the docker network shared with the etcd voters")
	}
	image := os.Getenv("BLOK_DISTRIBUTED_HELPER_IMAGE")
	if image == "" {
		image = "quay.io/coreos/etcd:v3.6.5"
	}
	name := fmt.Sprintf("%s-owner-%d", network, time.Now().UnixNano())
	args := []string{"run", "-d", "--name", name, "--network", network, "-v", dir + ":/work", "-v", binaryDir + ":/helper:ro", "--entrypoint", "/helper/cluster.test"}
	for _, entry := range env {
		args = append(args, "-e", entry)
	}
	args = append(args, image, "-test.run=^$")
	if output, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
		t.Fatalf("start owner container: %v: %s", err, output)
	}
	t.Cleanup(func() {
		_, _ = exec.Command("docker", "rm", "-f", name).CombinedOutput()
	})
	return &containerOwnerHelper{name: name, network: network, dir: dir}
}

func (h *containerOwnerHelper) inject(t *testing.T) {
	t.Helper()
	if output, err := exec.Command("docker", "network", "disconnect", h.network, h.name).CombinedOutput(); err != nil {
		t.Fatalf("partition owner container from etcd: %v: %s", err, output)
	}
}

func (h *containerOwnerHelper) resume(t *testing.T) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(h.dir, "resume"), []byte("resume"), 0o600); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("docker", "network", "connect", h.network, h.name).CombinedOutput(); err != nil {
		t.Fatalf("reconnect owner container: %v: %s", err, output)
	}
}

func (h *containerOwnerHelper) wait(t *testing.T) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", "wait", h.name).CombinedOutput()
	if err != nil || strings.TrimSpace(string(output)) != "0" {
		t.Fatalf("owner container exit=%s err=%v\n%s", output, err, h.logs())
	}
	return false
}

func (h *containerOwnerHelper) logs() string {
	output, _ := exec.Command("docker", "logs", h.name).CombinedOutput()
	return string(output)
}

// buildLinuxHelper cross-compiles this package's test binary so the stale
// owner can run inside a container attached to the etcd network.
func buildLinuxHelper(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	goTool := filepath.Join(runtime.GOROOT(), "bin", "go")
	if _, err := os.Stat(goTool); err != nil {
		goTool = "go"
	}
	command := exec.Command(goTool, "test", "-c", "-o", filepath.Join(dir, "cluster.test"), ".")
	command.Env = append(os.Environ(), "GOOS=linux", "GOARCH="+runtime.GOARCH, "CGO_ENABLED=0")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build linux owner helper: %v: %s", err, output)
	}
	return dir
}

// TestOwnerFaultsAroundEveryStepTransition stops a real owner process at each
// durable step transition, injects a real fault (SIGKILL, SIGSTOP, or a docker
// network partition between the owner container and every etcd voter), lets a
// successor take the partition after the lease expires, and then lets any
// surviving stale owner continue. Counts come from an on-disk effect ledger.
func TestOwnerFaultsAroundEveryStepTransition(t *testing.T) {
	var fixture ownerFaultFixture
	readDistributedFixture(t, "owner-fault-matrix-fixtures.json", &fixture)
	endpoints := integrationEndpoints(t)
	var linuxHelper string
	for _, fault := range []string{"kill", "stop", "partition"} {
		if fixture.Faults[fault] == "" {
			t.Fatalf("fixture does not declare fault %q", fault)
		}
		t.Run(fault, func(t *testing.T) {
			if fault == "partition" && linuxHelper == "" {
				linuxHelper = buildLinuxHelper(t)
			}
			for _, stage := range fixture.Stages {
				t.Run(stage.Name, func(t *testing.T) {
					expected := stage.Expected
					dir := t.TempDir()
					ledger := effectLedger(filepath.Join(dir, "ledger"))
					store := integrationDistributedStore(t)
					// The successor runs on its own client so a stage can hold it
					// mid-run at a chosen transition.
					successorClient := &hookedClient{Client: integrationClient(t)}
					successorRuntime, err := ownerFaultRuntime(integrationStoreFor(t, successorClient), ledger, nil)
					if err != nil {
						t.Fatal(err)
					}
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
					defer cancel()
					if err := successorRuntime.Check(ctx); err != nil {
						t.Fatal(err)
					}
					const partition = "p-0000"
					tenant := tenantsInPartition(successorRuntime, partition, "fault-tenant", 1)[0]
					admission, err := successorRuntime.Admit(ctx, Submission{Tenant: tenant, RequestKey: "fault-run", Workflow: "owner-fault", Input: json.RawMessage(fixture.Input)})
					if err != nil || !admission.Accepted {
						t.Fatalf("admission=%+v err=%v", admission, err)
					}
					helperEndpoints := strings.Join(endpoints, ",")
					helperDir := dir
					if fault == "partition" {
						voters := clustertest.Voters()
						internal := make([]string, len(voters))
						for index, voter := range voters {
							internal[index] = "http://" + voter + ":2379"
						}
						helperEndpoints, helperDir = strings.Join(internal, ","), "/work"
					}
					env := []string{
						helperRoleEnv + "=owner",
						helperEndpointsEnv + "=" + helperEndpoints,
						helperNamespaceEnv + "=" + integrationNamespace(t),
						helperDirEnv + "=" + helperDir,
						helperPartitionEnv + "=" + partition,
						helperOwnerEnv + "=stale-owner",
						helperPatternEnv + "=" + stage.Hook.Pattern,
						helperSkipEnv + "=" + strconv.Itoa(stage.Hook.Skip),
						helperAfterEnv + "=" + strconv.FormatBool(stage.Hook.After),
					}
					var helper ownerHelper
					if fault == "partition" {
						helper = startContainerOwnerHelper(t, linuxHelper, env, dir)
					} else {
						helper = startProcessOwnerHelper(t, fault, env, dir)
					}
					reachCtx, stopReach := context.WithTimeout(ctx, 60*time.Second)
					err = waitForFile(reachCtx, filepath.Join(dir, "reached"))
					stopReach()
					if err != nil {
						result, _ := os.ReadFile(filepath.Join(dir, "result.json"))
						t.Fatalf("owner did not reach %s: %v result=%s\n%s", stage.Name, err, result, helper.logs())
					}
					ownerData, err := os.ReadFile(filepath.Join(dir, "owner.json"))
					if err != nil {
						t.Fatal(err)
					}
					var stale struct {
						Token int64 `json:"token"`
					}
					if err := json.Unmarshal(ownerData, &stale); err != nil {
						t.Fatal(err)
					}
					// The stale owner's lease is live at the barrier: the fault,
					// not an unrenewed lease, is what ends its ownership.
					if probe, err := store.Acquire(ctx, partition, "live-lease-probe", ownerFaultLimits.OwnerTTL); !errors.Is(err, distributed.ErrOwnershipLost) {
						if err == nil {
							_ = store.Release(ctx, probe)
						}
						t.Fatalf("partition was acquirable before the fault: err=%v", err)
					}
					helper.inject(t)
					successor := acquireWhenFree(t, ctx, store, partition, "successor-owner", 30*time.Second)
					if successor.Token <= stale.Token {
						t.Fatalf("fence did not advance: stale=%d successor=%d", stale.Token, successor.Token)
					}
					var record RunRecord
					var killed bool
					if hold := stage.SuccessorHold; hold != nil {
						// Hold the successor inside its own run, then let the
						// stale owner resume and attempt its remaining
						// transitions against a run the successor still owns.
						held, release := make(chan struct{}), make(chan struct{})
						successorClient.arm(hold.Pattern, hold.Skip, hold.After, func() {
							close(held)
							<-release
						})
						successorDone := make(chan error, 1)
						go func() {
							var processErr error
							record, processErr = successorRuntime.processOne(ctx, successor)
							successorDone <- processErr
						}()
						select {
						case <-held:
						case err := <-successorDone:
							t.Fatalf("successor finished before reaching its hold: %v", err)
						case <-ctx.Done():
							t.Fatal("successor did not reach its hold")
						}
						helper.resume(t)
						killed = helper.wait(t)
						during, err := successorRuntime.GetRun(ctx, tenant, admission.RunID)
						if err != nil {
							t.Fatal(err)
						}
						activeDuring, err := store.ListActiveRunIDs(ctx, partition, ownerFaultLimits.PartitionAdmissions)
						if err != nil {
							t.Fatal(err)
						}
						ownedBySuccessor := during.OwnerID == successor.ID && during.Fence == successor.Token
						t.Logf("%s/%s while successor held: state=%s ownedBySuccessor=%v active=%d", fault, stage.Name, during.State, ownedBySuccessor, len(activeDuring))
						want := stage.Expected.DuringSuccessorRun
						if want == nil || during.State != want.State || ownedBySuccessor != want.OwnedBySuccessor || len(activeDuring) != want.ActiveRuns {
							t.Errorf("while successor held: state=%s ownedBySuccessor=%v active=%v; fixture %+v", during.State, ownedBySuccessor, activeDuring, want)
						}
						close(release)
						if err := <-successorDone; err != nil {
							t.Fatalf("successor processOne after stale owner resumed: %v", err)
						}
					} else {
						record, err = successorRuntime.processOne(ctx, successor)
						if err != nil {
							t.Fatalf("successor processOne: %v", err)
						}
						helper.resume(t)
						killed = helper.wait(t)
					}
					staleResult := "unknown"
					if fault == "kill" {
						if killed {
							staleResult = "killed"
						}
						if _, err := os.Stat(filepath.Join(dir, "result.json")); err == nil {
							staleResult = "completed-after-kill"
						}
					} else {
						data, err := os.ReadFile(filepath.Join(dir, "result.json"))
						if err != nil {
							t.Fatalf("stale owner result: %v\n%s", err, helper.logs())
						}
						var result ownerHelperResult
						if err := json.Unmarshal(data, &result); err != nil {
							t.Fatal(err)
						}
						staleResult = "error: " + result.Error
						if result.OwnershipLost {
							staleResult = "ownership_lost"
						}
					}
					final, err := successorRuntime.GetRun(ctx, tenant, admission.RunID)
					if err != nil {
						t.Fatal(err)
					}
					active, err := store.ListActiveRunIDs(ctx, partition, ownerFaultLimits.PartitionAdmissions)
					if err != nil {
						t.Fatal(err)
					}
					events, err := store.ListEvents(ctx, partition)
					if err != nil {
						t.Fatal(err)
					}
					staleAfter, terminal, pureDispatches, effectDispatches, successorPureAttempt := 0, 0, 0, 0, 0
					staleAttempts := map[string]bool{}
					var successorAttempt string
					for _, event := range events {
						if event.Fence == stale.Token && event.Revision > successor.Token {
							staleAfter++
						}
						switch event.Kind {
						case "run.completed", "run.uncertain", "run.failed":
							terminal++
						case "step.dispatched":
							var step stepRecord
							if err := json.Unmarshal(event.Payload, &step); err != nil {
								t.Fatal(err)
							}
							switch step.Identity.StepID {
							case "prepare":
								pureDispatches++
								if event.Fence == successor.Token {
									successorPureAttempt, successorAttempt = step.AttemptNumber, step.CurrentAttempt
								} else {
									staleAttempts[step.CurrentAttempt] = true
								}
							case "effect":
								effectDispatches++
							}
						}
					}
					counts, err := ledger.counts()
					if err != nil {
						t.Fatal(err)
					}
					effects, pure := ledger.total("effect"), ledger.total("pure")
					t.Logf("%s/%s: successor=%s output=%s final=%s effects=%d pure=%d dispatches pure=%d effect=%d successorPureAttempt=%d stale=%s staleFenceEventsAfterTakeover=%d fences stale=%d successor=%d ledger=%v",
						fault, stage.Name, record.State, record.Output, final.State, effects, pure, pureDispatches, effectDispatches, successorPureAttempt, staleResult, staleAfter, stale.Token, successor.Token, counts)
					if final.State != expected.State || string(final.Output) != expected.Output || record.State != expected.State {
						t.Errorf("final state=%s output=%s successor=%s; fixture %s %s", final.State, final.Output, record.State, expected.State, expected.Output)
					}
					if effects != expected.Effects[fault] || pure != expected.PureInvocations[fault] {
						t.Errorf("effects=%d pure=%d; fixture %d/%d", effects, pure, expected.Effects[fault], expected.PureInvocations[fault])
					}
					if pureDispatches != expected.PureDispatches || effectDispatches != expected.EffectDispatches || successorPureAttempt != expected.SuccessorPureAttempt {
						t.Errorf("dispatches pure=%d effect=%d successor pure attempt=%d; fixture %d/%d/%d", pureDispatches, effectDispatches, successorPureAttempt, expected.PureDispatches, expected.EffectDispatches, expected.SuccessorPureAttempt)
					}
					if successorAttempt != "" && staleAttempts[successorAttempt] {
						t.Errorf("successor reused a stale step attempt ID %s", successorAttempt)
					}
					if len(active) != fixture.Invariants.ActiveRunsAfterSuccessor || staleResult != fixture.Invariants.StaleOwnerResult[fault] || staleAfter != fixture.Invariants.StaleFenceEventsAfterTakeover || terminal != fixture.Invariants.TerminalEvents {
						t.Errorf("active=%v stale=%s staleFenceEventsAfterTakeover=%d terminal events=%d; fixture %d %s %d %d", active, staleResult, staleAfter, terminal, fixture.Invariants.ActiveRunsAfterSuccessor, fixture.Invariants.StaleOwnerResult[fault], fixture.Invariants.StaleFenceEventsAfterTakeover, fixture.Invariants.TerminalEvents)
					}
				})
			}
		})
	}
}
