//go:build !race

package sse_test

import "time"

// publishBound is the longest one Publish may take while a subscriber
// stalls, without the race detector.
const publishBound = 50 * time.Millisecond
