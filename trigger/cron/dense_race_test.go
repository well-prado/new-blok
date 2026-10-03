//go:build race

package cron_test

import "time"

// The race detector slows ticks roughly tenfold.
const denseTickBound = 30 * time.Second
