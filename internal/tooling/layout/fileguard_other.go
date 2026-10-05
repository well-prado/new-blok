//go:build !unix

package layout

import "os"

// openFlags is a plain read-only open: these platforms have no FIFO that a
// regular-file open could block on.
const openFlags = os.O_RDONLY

// linkCount is unknown on these platforms; see ADR 0023 limits.
func linkCount(*os.File) (uint64, bool) { return 0, false }
