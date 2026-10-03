//go:build race

package sse_test

import "time"

// The race detector slows publishing roughly tenfold.
const publishBound = 500 * time.Millisecond
