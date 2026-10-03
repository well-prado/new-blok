//go:build race

package grpc_test

// The race detector slows calls; timing bounds widen accordingly.
const deadlineSlack = 4

// expansionBound bounds what refusing an expanding request may allocate.
const expansionBound = 48 << 20
