//go:build race

package cron

import "time"

// The race detector slows this computation roughly tenfold.
const catchUpBound = 30 * time.Second
