//go:build !race

package cron_test

import "time"

// denseTickBound bounds the CPU time of one tick of 1000 dense schedules
// without the race detector.
const denseTickBound = 2 * time.Second

// denseWallBackstop bounds the same tick's wall time loosely: it catches a
// hang or an I/O pathology that CPU time cannot see, without failing on a
// contended host the way a tight wall bound did (#214).
const denseWallBackstop = 30 * time.Second
