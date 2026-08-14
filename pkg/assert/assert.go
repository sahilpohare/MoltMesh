// Package assert provides invariant checks for conditions that must never
// occur if the code is correct — as opposed to domain-rule violations
// (invalid input, illegal state transitions) that a caller can legitimately
// trigger and which must be returned as errors, not panics.
//
// A failed assertion means the program's internal model of its own state is
// wrong: it always panics, in every build, so a violated invariant is loud
// wherever it happens rather than silently corrupting data.
package assert

import "fmt"

// That panics if cond is false. format/args describe the invariant that was
// expected to hold.
func That(cond bool, format string, args ...any) {
	if !cond {
		panic("assertion failed: " + fmt.Sprintf(format, args...))
	}
}
