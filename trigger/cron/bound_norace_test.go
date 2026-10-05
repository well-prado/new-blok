//go:build !race

package cron

import "time"

// catchUpBound bounds the CPU time computing a year of missed minutely
// firings may take without the race detector: about 3 ms when the count is
// clamped at the cap, about 90 ms if every firing were enumerated (#214).
const catchUpBound = 40 * time.Millisecond
