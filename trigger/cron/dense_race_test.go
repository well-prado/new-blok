//go:build race

package cron_test

import "time"

// The race detector slows ticks roughly tenfold.
const denseTickBound = 30 * time.Second

// denseWallBackstop bounds the same tick's wall time loosely (see the
// non-race build).
const denseWallBackstop = 300 * time.Second
