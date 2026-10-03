//go:build !race

package grpc_test

// deadlineSlack widens timing bounds; none without the race detector.
const deadlineSlack = 1

// expansionBound bounds what refusing an expanding request may allocate.
const expansionBound = 48 << 20
