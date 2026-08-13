package outbox

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestClassifyRecognisesPoisonThroughWrapping(t *testing.T) {
	err := fmt.Errorf("broker rejected the type: %w", ErrPoison)

	if got := classify(err); got != FailurePoison {
		t.Errorf("classify = %v, want %v", got, FailurePoison)
	}
}

// The asymmetry that keeps a revocation alive through an outage. An error this package
// does not recognise must never be treated as unfixable.
func TestClassifyDefaultsToUnavailable(t *testing.T) {
	for _, err := range []error{
		errors.New("connection refused"),
		errors.New("context deadline exceeded"),
		fmt.Errorf("wrapped: %w", errors.New("i/o timeout")),
	} {
		if got := classify(err); got != FailureUnavailable {
			t.Errorf("classify(%v) = %v, want %v", err, got, FailureUnavailable)
		}
	}
}

func TestDecideCoversEveryCombination(t *testing.T) {
	const maxAttempts = 3

	for _, tc := range []struct {
		name     string
		class    FailureClass
		priority int16
		attempts int
		want     disposition
	}{
		{"poison spends no attempts, priority", FailurePoison, PriorityHigh, 1, dispositionDeadLetter},
		{"poison spends no attempts, standard", FailurePoison, PriorityStandard, 1, dispositionDeadLetter},
		{"poison at the limit still dead-letters", FailurePoison, PriorityStandard, 3, dispositionDeadLetter},

		{"first failure retries, priority", FailureUnavailable, PriorityHigh, 1, dispositionRetry},
		{"first failure retries, standard", FailureUnavailable, PriorityStandard, 1, dispositionRetry},
		{"below the limit retries", FailureUnavailable, PriorityStandard, 2, dispositionRetry},

		{"standard row is abandoned at the limit", FailureUnavailable, PriorityStandard, 3, dispositionDeadLetter},
		{"standard row past the limit stays abandoned", FailureUnavailable, PriorityStandard, 9, dispositionDeadLetter},

		{"priority row is released at the limit", FailureUnavailable, PriorityHigh, 3, dispositionRelease},
		{"priority row past the limit stays released", FailureUnavailable, PriorityHigh, 99, dispositionRelease},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := decide(tc.class, tc.priority, tc.attempts, maxAttempts); got != tc.want {
				t.Errorf("decide = %v, want %v", got, tc.want)
			}
		})
	}
}

// The rule the design states twice because it is the one most likely to be optimised away
// by someone tidying up the retry logic.
func TestAPriorityRowIsNeverDeadLetteredForUnavailability(t *testing.T) {
	for attempts := 1; attempts < 500; attempts++ {
		if got := decide(FailureUnavailable, PriorityHigh, attempts, 3); got == dispositionDeadLetter {
			t.Fatalf("a priority row was dead-lettered for unavailability after %d attempts", attempts)
		}
	}
}

func TestBackoffGrowsAndRespectsTheCeiling(t *testing.T) {
	const (
		base    = 250 * time.Millisecond
		ceiling = 30 * time.Second
	)

	var previous time.Duration
	for attempts := 1; attempts <= 12; attempts++ {
		got := backoffFor(base, attempts, ceiling, 0)

		if got > ceiling {
			t.Fatalf("attempt %d produced %v, above the %v ceiling", attempts, got, ceiling)
		}
		if got < previous {
			t.Fatalf("attempt %d produced %v, less than the previous %v", attempts, got, previous)
		}
		previous = got
	}
}

// Equal jitter: never below half the interval, never above it. A near-zero delay would
// defeat the backoff on the attempt that needed it most; an identical delay would
// synchronise every worker that failed against the same broker into one burst on recovery.
func TestBackoffStaysWithinTheEqualJitterBand(t *testing.T) {
	const base = 250 * time.Millisecond

	for _, jitter := range []float64{0, 0.25, 0.5, 0.75, 0.999999} {
		got := backoffFor(base, 1, time.Minute, jitter)

		if got < base/2 {
			t.Errorf("jitter %v produced %v, below the %v floor", jitter, got, base/2)
		}
		if got > base {
			t.Errorf("jitter %v produced %v, above the %v interval", jitter, got, base)
		}
	}
}

// A jitter outside [0,1) comes from a caller's bug, and the delay must stay usable rather
// than becoming negative — which the claim predicate would read as "retry immediately".
func TestBackoffIsNeverNegativeForAnyInput(t *testing.T) {
	for _, tc := range []struct {
		attempts int
		jitter   float64
	}{
		{1, -1}, {1, 2}, {0, 0.5}, {-5, 0.5}, {1000, 0.5}, {64, 0.5},
	} {
		if got := backoffFor(250*time.Millisecond, tc.attempts, 30*time.Second, tc.jitter); got < 0 {
			t.Errorf("attempts %d jitter %v produced a negative delay %v", tc.attempts, tc.jitter, got)
		}
	}
}

func TestBackoffIsZeroWithoutABase(t *testing.T) {
	if got := backoffFor(0, 3, time.Minute, 0.5); got != 0 {
		t.Errorf("backoffFor with no base = %v, want 0", got)
	}
}

func TestEmptyPollDelayGrowsThenHoldsAtTheCeiling(t *testing.T) {
	const (
		interval = 500 * time.Millisecond
		ceiling  = 5 * time.Second
	)

	if got := emptyPollDelay(interval, 0, ceiling); got != interval {
		t.Errorf("a busy worker waits %v, want the interval %v", got, interval)
	}

	var previous time.Duration
	for empty := 1; empty <= 20; empty++ {
		got := emptyPollDelay(interval, empty, ceiling)

		if got > ceiling {
			t.Fatalf("%d empty polls produced %v, above the %v ceiling", empty, got, ceiling)
		}
		if got < previous {
			t.Fatalf("%d empty polls produced %v, less than the previous %v", empty, got, previous)
		}
		previous = got
	}

	if previous != ceiling {
		t.Errorf("an idle worker settled at %v, want the ceiling %v", previous, ceiling)
	}
}

func TestWaitReturnsEarlyOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	if wait(ctx, time.Minute) {
		t.Error("wait reported completion despite cancellation")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("wait took %v to notice cancellation", elapsed)
	}
}

func TestWaitCompletesWhenTheDelayElapses(t *testing.T) {
	if !wait(context.Background(), time.Millisecond) {
		t.Error("wait reported cancellation on an uncancelled context")
	}
}

func TestDispositionsAreNamedForLogs(t *testing.T) {
	for d, want := range map[disposition]string{
		dispositionRetry:      "retry",
		dispositionDeadLetter: "dead-letter",
		dispositionRelease:    "release",
		disposition(99):       "unknown",
	} {
		if got := d.String(); got != want {
			t.Errorf("disposition %d = %q, want %q", int(d), got, want)
		}
	}
}
