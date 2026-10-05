package parity

import (
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

// runProvenance is captured by each sampler itself, so a committed raw file
// says which source, tree state, invocation and installed old packages
// produced it without hand-added wrapper fields.
type runProvenance struct {
	Test             string            `json:"test"`
	CapturedAt       string            `json:"capturedAt"`
	NewBlokRevision  string            `json:"newBlokRevision"`
	SourceTreeClean  bool              `json:"sourceTreeClean"`
	DirtyPaths       []string          `json:"dirtyPaths,omitempty"`
	OldBlokTag       string            `json:"oldBlokTag"`
	OldBlokCommit    string            `json:"oldBlokCommit"`
	OldPackages      map[string]string `json:"oldInstalledPackages"`
	Node             string            `json:"node"`
	Go               string            `json:"go"`
	GoTestArguments  []string          `json:"goTestArguments"`
	ParityGates      map[string]string `json:"parityEnvironment"`
	Host             map[string]any    `json:"host"`
	LoadAverageAtRun string            `json:"loadAverageAtStart,omitempty"`
}

func captureProvenance(t *testing.T) runProvenance {
	t.Helper()
	revision, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatalf("read new-framework revision: %v", err)
	}
	status, err := exec.Command("git", "status", "--porcelain", "--untracked-files=normal").Output()
	if err != nil {
		t.Fatalf("read source tree state: %v", err)
	}
	var dirty []string
	for _, line := range strings.Split(strings.TrimRight(string(status), "\n"), "\n") {
		if line != "" && len(dirty) < 50 {
			dirty = append(dirty, line)
		}
	}
	provenance := runProvenance{
		Test:            t.Name(),
		CapturedAt:      time.Now().UTC().Format(time.RFC3339),
		NewBlokRevision: strings.TrimSpace(string(revision)),
		SourceTreeClean: len(dirty) == 0,
		DirtyPaths:      dirty,
		OldBlokTag:      "v2.5.0",
		OldBlokCommit:   "7611e434f716a5a8efbed26613e546893d5bcba7",
		OldPackages:     installedOldPackages(t),
		Node:            nodeVersion(t),
		Go:              runtime.Version(),
		GoTestArguments: append([]string(nil), os.Args[1:]...),
		ParityGates:     map[string]string{},
		Host:            map[string]any{"os": runtime.GOOS, "arch": runtime.GOARCH, "logicalCPUs": runtime.NumCPU(), "gomaxprocs": runtime.GOMAXPROCS(0)},
	}
	for _, entry := range os.Environ() {
		name, value, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "BLOK_PARITY_") && name != "BLOK_PARITY_RAW_DIR" {
			provenance.ParityGates[name] = value
		}
	}
	if runtime.GOOS == "darwin" {
		if load, err := exec.Command("sysctl", "-n", "vm.loadavg").Output(); err == nil {
			provenance.LoadAverageAtRun = strings.TrimSpace(string(load))
		}
	} else if load, err := os.ReadFile("/proc/loadavg"); err == nil {
		provenance.LoadAverageAtRun = strings.TrimSpace(string(load))
	}
	return provenance
}

// installedOldPackages reads the installed version of every pinned direct
// dependency, rather than restating the pins.
func installedOldPackages(t *testing.T) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("old-engine", "package.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Dependencies map[string]string `json:"dependencies"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(manifest.Dependencies))
	for name := range manifest.Dependencies {
		names = append(names, name)
	}
	sort.Strings(names)
	installed := map[string]string{}
	for _, name := range names {
		data, err := os.ReadFile(filepath.Join("old-engine", "node_modules", filepath.FromSlash(name), "package.json"))
		if err != nil {
			t.Fatalf("read installed %s: %v", name, err)
		}
		var pkg struct {
			Version string `json:"version"`
		}
		if err := json.Unmarshal(data, &pkg); err != nil {
			t.Fatal(err)
		}
		installed[name] = pkg.Version
	}
	return installed
}

type postgresProvenance struct {
	Container     string          `json:"container"`
	ContainerID   string          `json:"containerId"`
	ConfigImage   string          `json:"configImage"`
	ImageID       string          `json:"imageId"`
	RepoDigests   []string        `json:"repoDigests"`
	PublishedPort string          `json:"publishedPort"`
	Tmpfs         json.RawMessage `json:"tmpfs"`
}

// inspectPostgresContainer reads the disposable server's identity from the
// running container named by BLOK_PARITY_POSTGRES_CONTAINER and checks that
// it is the server BLOK_PARITY_POSTGRES_URL points at (same loopback port).
func inspectPostgresContainer(t *testing.T, postgresURL string) postgresProvenance {
	t.Helper()
	name := os.Getenv("BLOK_PARITY_POSTGRES_CONTAINER")
	if name == "" {
		t.Fatal("set BLOK_PARITY_POSTGRES_CONTAINER to the disposable PostgreSQL container so its image identity is read, not asserted")
	}
	inspect := func(args ...string) string {
		output, err := exec.Command("docker", args...).Output()
		if err != nil {
			t.Fatalf("docker %s: %v", strings.Join(args, " "), err)
		}
		return strings.TrimSpace(string(output))
	}
	fields := strings.Fields(inspect("inspect", name, "--format", "{{.Id}} {{.Config.Image}} {{.Image}} {{.State.Running}}"))
	if len(fields) != 4 || fields[3] != "true" {
		t.Fatalf("PostgreSQL container %q is not running: %v", name, fields)
	}
	provenance := postgresProvenance{Container: name, ContainerID: fields[0], ConfigImage: fields[1], ImageID: fields[2]}
	if err := json.Unmarshal([]byte(inspect("image", "inspect", provenance.ImageID, "--format", "{{json .RepoDigests}}")), &provenance.RepoDigests); err != nil || len(provenance.RepoDigests) == 0 {
		t.Fatalf("read repository digests for %s: %v", provenance.ImageID, err)
	}
	provenance.Tmpfs = json.RawMessage(inspect("inspect", name, "--format", "{{json .HostConfig.Tmpfs}}"))
	provenance.PublishedPort = inspect("port", name, "5432/tcp")
	parsed, err := url.Parse(postgresURL)
	if err != nil {
		t.Fatal(err)
	}
	matched := false
	for _, line := range strings.Split(provenance.PublishedPort, "\n") {
		host, port, err := net.SplitHostPort(strings.TrimSpace(line))
		if err == nil && port == parsed.Port() && net.ParseIP(host) != nil && net.ParseIP(host).IsLoopback() {
			matched = true
		}
	}
	if !matched {
		t.Fatalf("container %s publishes %q, which is not the loopback port in BLOK_PARITY_POSTGRES_URL (%s)", name, provenance.PublishedPort, parsed.Port())
	}
	return provenance
}

func (p postgresProvenance) String() string {
	return fmt.Sprintf("%s %s %v", p.Container, p.ConfigImage, p.RepoDigests)
}
