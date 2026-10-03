//go:build race

package websocket_test

// The race detector shadows every allocation, inflating resident memory
// several times; the bound scales with it.
const rssBoundPerConnection = 2 << 20
