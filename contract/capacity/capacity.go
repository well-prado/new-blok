// Package capacity names the error every layer uses to say it ran out of
// capacity. It is a leaf: the store, the engine and the triggers can all
// depend on it without depending on one another.
package capacity

import "errors"

// ErrSaturated reports that an operation was refused, or could not commit,
// for want of capacity. Nothing that operation was to write was committed,
// and the same operation may succeed when retried later. It says nothing
// about operations that ran before it: a caller that wrote elsewhere first
// must not read it as "nothing happened".
var ErrSaturated = errors.New("admission_saturated")
