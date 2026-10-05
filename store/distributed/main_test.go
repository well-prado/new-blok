package distributed

import (
	"fmt"
	"os"
	"testing"
)

// TestMain re-executes this test binary as the paused stale-owner process
// when BLOK_DISTRIBUTED_OWNER_HELPER is set; otherwise it runs the tests.
func TestMain(m *testing.M) {
	if os.Getenv("BLOK_DISTRIBUTED_OWNER_HELPER") == "1" {
		if err := runPausedOwnerHelper(); err != nil {
			fmt.Fprintf(os.Stderr, "paused owner helper: %v\n", err)
			os.Exit(2)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}
