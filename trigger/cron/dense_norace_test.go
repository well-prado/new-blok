//go:build !race

package cron_test

import "time"

// denseTickBound bounds one tick of 1000 dense schedules without the race
// detector.
const denseTickBound = 2 * time.Second
