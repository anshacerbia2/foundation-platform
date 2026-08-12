// This file is a fixture. It lives under testdata, which the go tool ignores, so it is
// never built or vetted as part of the module. It exists to be parsed.
package impure

import "context"

// Decide takes a context, which rule 4 reserves for app/. A domain method decides
// whether a transition is permitted; it waits for nothing.
func Decide(ctx context.Context, amount int) error {
	return nil
}

// Start begins a goroutine, which rule 7 places in a driving adapter.
func Start() {
	go func() {
		_ = 1
	}()
}

// Pure has neither, and must produce no finding.
func Pure(amount int) bool {
	return amount > 0
}

// Method receivers are covered too, because a context reaching a domain type through a
// method is the same violation as one reaching it through a function.
type Aggregate struct{}

func (a *Aggregate) Load(ctx context.Context) error { return nil }
