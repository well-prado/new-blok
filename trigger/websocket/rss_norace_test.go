//go:build !race

package websocket_test

// rssBoundPerConnection is the resident memory allowed per open connection
// (client and server sides) without the race detector.
const rssBoundPerConnection = 256 << 10
