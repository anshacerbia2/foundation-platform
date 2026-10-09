// Package e2e holds tests that run several packages of this module together, the way a
// consuming system composes them, and assert a property none of them can assert alone.
//
// It ships no code. A unit test proves its own package's half of a hand-off; only a test
// that drives every half in one process proves the halves agree.
package e2e
