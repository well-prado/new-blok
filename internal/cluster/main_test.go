package cluster

import (
	"fmt"
	"os"
	"testing"
)

// TestMain lets owner/worker fault tests re-execute this test binary as a real
// separate process (or inside a container on the etcd network). A helper
// process runs only the named role and never the package's tests.
func TestMain(m *testing.M) {
	if role := os.Getenv(helperRoleEnv); role != "" {
		if err := runClusterHelper(role); err != nil {
			fmt.Fprintf(os.Stderr, "cluster helper %s: %v\n", role, err)
			os.Exit(2)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}
