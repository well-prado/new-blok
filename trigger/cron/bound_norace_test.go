//go:build !race

package cron

import "time"

// catchUpBound bounds the CPU time computing a year of missed minutely
// firings may take without the race detector.
const catchUpBound = 2 * time.Second
