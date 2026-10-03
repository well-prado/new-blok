//go:build !race

package cron

import "time"

// catchUpBound is how long computing a year of missed minutely firings may
// take without the race detector.
const catchUpBound = 2 * time.Second
