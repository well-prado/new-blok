//go:build race

package cron

import "time"

// The race detector slows this computation roughly tenfold: about 20 ms
// clamped at the cap, about 870 ms if every firing were enumerated (#214).
const catchUpBound = 400 * time.Millisecond
