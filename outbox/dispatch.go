package outbox

import (
	"context"
	"errors"
	"time"

	"github.com/anshacerbia2/foundation-platform/event"
)

// Publisher delivers an envelope to the broker.
//
// The interface is declared here, by the consumer, rather than beside an implementation.
// This package states what it needs from a broker; which broker satisfies it is recorded
// in the consuming system's SAD, and the choice lands in one adapter.
//
// It returns a Receipt rather than only an error because "the publish succeeded" is two
// different facts, and the dead-letter resolution contract can only use one of them. See
// Evidence.
type Publisher interface {
	Publish(ctx context.Context, e event.Envelope) (Receipt, error)
}

// Evidence says what a successful publication actually established.
//
// The distinction is load-bearing rather than descriptive. An abandoned delivery may be
// resolved as REPLAYED when the producer's own dispatcher witnessed the consumer accept the
// event -- that is the only evidence in the contract which does not rest on the consumer's
// report about itself. A broker acknowledgement establishes something much weaker: the
// message was handed on. The consumer may still be hours behind, or gone.
//
// Today's transport makes the strong form available: the consumer applies the event inside
// the same transaction as its inbox guard and only then answers, so its acknowledgement means
// applied. The moment a broker is introduced that stops being true, and the failure would be
// silent -- a resolution path quietly accepting "handed to the broker" as "the consumer has
// it".
type Evidence string

const (
	// EvidenceConsumerApplied means the consumer asserted it applied the event within the
	// delivery it acknowledged. Resolution evidence.
	EvidenceConsumerApplied Evidence = "consumer_applied"

	// EvidenceTransportAccepted means something accepted the message for onward delivery.
	// Adequate for dispatch bookkeeping and never adequate as resolution evidence.
	EvidenceTransportAccepted Evidence = "transport_accepted"
)

// ApplicationReceiptHeader is the header a consumer sets to assert that it applied the event
// before acknowledging it.
//
// Named here, in the producer, because the meaning of the assertion belongs to the contract
// rather than to whichever adapter reads it.
const ApplicationReceiptHeader = "X-Application-Receipt"

// ApplicationReceiptApplied is the only value that establishes EvidenceConsumerApplied.
const ApplicationReceiptApplied = "applied"

// Receipt is what a publication established, and it cannot be forged from outside.
//
// The field is unexported deliberately, and this is the difference between a mechanism and a
// convention. With an exported field, a broker adapter written six months from now would
// simply fill in `Evidence: EvidenceConsumerApplied` on a 200 from the broker -- not out of
// dishonesty but because it is the obvious thing to write, and it would pass review. The
// typed result would then have bought one compile error at the moment of the change and
// nothing afterwards.
//
// Unexported, the strong value is reachable only through ReceiptFromMarker, which requires
// the consumer's own marker. A broker cannot produce that marker, because it is not the
// consumer. So introducing a broker degrades the evidence automatically and correctly, and
// resolution stops finding valid proof -- a loud failure instead of a quiet downgrade.
type Receipt struct {
	evidence Evidence
}

// Evidence reports what the publication established.
//
// An unset Receipt reads as EvidenceTransportAccepted rather than as empty: a publisher that
// forgets to say what it established must never have that read as the stronger claim.
func (r Receipt) Evidence() Evidence {
	if r.evidence == EvidenceConsumerApplied {
		return EvidenceConsumerApplied
	}
	return EvidenceTransportAccepted
}

// ReceiptFromMarker derives the evidence from what the consumer returned.
//
// The only constructor that can produce EvidenceConsumerApplied. Pass the value of
// ApplicationReceiptHeader from the consumer's response; anything else, including an absent
// header, yields transport evidence.
func ReceiptFromMarker(marker string) Receipt {
	if marker == ApplicationReceiptApplied {
		return Receipt{evidence: EvidenceConsumerApplied}
	}
	return Receipt{evidence: EvidenceTransportAccepted}
}

// TransportReceipt states that something accepted the message and nothing more. What a broker
// adapter returns, and the honest answer for any transport that cannot carry the consumer's
// own assertion back.
func TransportReceipt() Receipt {
	return Receipt{evidence: EvidenceTransportAccepted}
}

// ErrPoison marks a publication failure that retrying cannot fix: an unregistered event
// type, a payload the broker rejects as malformed, a contract violation.
//
// A Publisher signals it by wrapping — fmt.Errorf("…: %w", outbox.ErrPoison) — so the
// broker adapter, which is the only code that can tell a rejection from an outage, owns
// the judgement.
var ErrPoison = errors.New("outbox: poison")

// FailureClass is the branch the dispatcher takes when publication fails.
type FailureClass string

const (
	// FailurePoison is a failure that will recur on every attempt.
	FailurePoison FailureClass = "poison"

	// FailureUnavailable is a failure that a later attempt may not see.
	FailureUnavailable FailureClass = "unavailable"
)

// classify reads a publication error.
//
// Anything not explicitly poison is unavailable, and the asymmetry is the point. Treating
// an outage as poison discards a revocation that would have published a minute later, and
// the publisher cannot compensate — organization-control holds no Keycloak credential and
// cannot enforce the change itself. Treating a poison message as an outage costs three
// retries and a slower path to the same dead-letter row. Only one of those loses a
// security state change, so an unrecognised error is never poison.
func classify(err error) FailureClass {
	if errors.Is(err, ErrPoison) {
		return FailurePoison
	}
	return FailureUnavailable
}

// disposition is what happens to a row after a failed publication.
type disposition int

const (
	// dispositionRetry releases the row for another claim once its backoff elapses.
	dispositionRetry disposition = iota

	// dispositionDeadLetter moves the row to platform.dead_letter and marks it published
	// so it stops being redelivered.
	dispositionDeadLetter

	// dispositionRelease returns a priority row to the unpublished pool with its attempts
	// reset, so it keeps trying for as long as the outage lasts.
	dispositionRelease
)

func (d disposition) String() string {
	switch d {
	case dispositionRetry:
		return "retry"
	case dispositionDeadLetter:
		return "dead-letter"
	case dispositionRelease:
		return "release"
	default:
		return "unknown"
	}
}

// decide chooses what happens to a row whose publication failed.
//
// attempts is the count including the attempt that just failed.
//
// The rule worth reading twice is the last one: a priority row is never dead-lettered for
// unavailability. Dead-lettering is calibrated for a message that will never succeed —
// three attempts, then abandon. A broker outage is not that. So a priority row exhausts
// its local retries, returns to the pool, and publishes when the broker recovers.
//
// What bounds enforcement meanwhile is not delivery but the consumer: a projection past
// its max_accepted_age under fail_closed denies. An outage delays enforcement; it does
// not remove it. That leaves exactly one unbounded path — a priority event classified
// poison — which is a containment failure and is alerted at any occurrence.
func decide(class FailureClass, priority int16, attempts, maxAttempts int) disposition {
	if class == FailurePoison {
		// No attempts are spent on a message that cannot succeed.
		return dispositionDeadLetter
	}
	if attempts < maxAttempts {
		return dispositionRetry
	}
	if priority == PriorityHigh {
		return dispositionRelease
	}
	return dispositionDeadLetter
}

// backoffFor returns the delay before a failed row may be claimed again.
//
// jitter is in [0,1) and is supplied by the caller rather than drawn here, so a schedule
// is reproducible in a test rather than merely plausible.
//
// Equal jitter is used: half the computed interval, plus a random share of the other
// half. Full jitter can return close to zero, which defeats the backoff on the attempt
// that needed it most; a fixed interval synchronises every worker that failed against the
// same broker into one burst the moment it recovers. Equal jitter guarantees a floor and
// still decorrelates the workers.
func backoffFor(base time.Duration, attempts int, ceiling time.Duration, jitter float64) time.Duration {
	if base <= 0 {
		return 0
	}
	if attempts < 1 {
		attempts = 1
	}

	// Shifting past this would overflow the duration and produce a negative delay, which
	// reads as "retry immediately" — the opposite of what an exhausted row needs.
	const maxShift = 30
	shift := attempts - 1
	if shift > maxShift {
		shift = maxShift
	}

	interval := base << shift
	if interval <= 0 || (ceiling > 0 && interval > ceiling) {
		interval = ceiling
	}

	if jitter < 0 {
		jitter = 0
	}
	if jitter >= 1 {
		jitter = 0.999999
	}

	half := interval / 2
	return half + time.Duration(jitter*float64(half))
}

// emptyPollDelay returns how long a worker waits after claiming nothing.
//
// An idle dispatcher polling at a fixed interval wakes the database forever for no
// reason, and two consuming systems each running six workers make that twelve pointless
// queries every interval. The delay grows with consecutive empty polls and is reset by
// the first row claimed, so a busy dispatcher never pays for it.
func emptyPollDelay(interval time.Duration, consecutiveEmpty int, ceiling time.Duration) time.Duration {
	if interval <= 0 {
		return 0
	}
	if consecutiveEmpty < 1 {
		return interval
	}

	const maxShift = 10
	shift := consecutiveEmpty - 1
	if shift > maxShift {
		shift = maxShift
	}

	delay := interval << shift
	if delay <= 0 || (ceiling > 0 && delay > ceiling) {
		delay = ceiling
	}
	return delay
}

// wait sleeps for d or returns early if ctx is cancelled. It reports whether the wait
// completed, so a caller can distinguish a due poll from a shutdown.
func wait(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
