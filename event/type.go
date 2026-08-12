package event

import (
	"fmt"
	"strconv"
	"strings"
)

// Type is a validated CloudEvents type name.
//
// The enterprise form is fixed by TDD-foundation-platform-001 and ADR-GLB-006:
//
//	com.scnehaux.<domain>.<aggregate>.<lifecycle>.<action>[.v<major>]
//
// It is a value object rather than a string alias with helper functions, so an
// invalid type cannot exist. Every construction path runs through ParseType, and
// callers holding a Type know it parsed.
type Type string

const (
	typePrefix      = "com.scnehaux."
	typeSegmentsMin = 6 // com, scnehaux, domain, aggregate, lifecycle, action
	typeSegmentsMax = 7 // ... plus the major version
)

// ParseType validates s and returns it as a Type.
func ParseType(s string) (Type, error) {
	if !strings.HasPrefix(s, typePrefix) {
		return "", fmt.Errorf("event: type %q must begin with %q", s, typePrefix)
	}

	segments := strings.Split(s, ".")
	if len(segments) < typeSegmentsMin || len(segments) > typeSegmentsMax {
		return "", fmt.Errorf(
			"event: type %q has %d segments, want %d or %d",
			s, len(segments), typeSegmentsMin, typeSegmentsMax,
		)
	}

	for i, seg := range segments[:typeSegmentsMin] {
		if err := validSegment(seg); err != nil {
			return "", fmt.Errorf("event: type %q segment %d: %w", s, i+1, err)
		}
	}

	if len(segments) == typeSegmentsMax {
		if err := validVersionSegment(segments[typeSegmentsMax-1]); err != nil {
			return "", fmt.Errorf("event: type %q: %w", s, err)
		}
	}

	return Type(s), nil
}

// MustParseType is ParseType for package-level constants declared in a producing
// system. It panics on an invalid value, which surfaces at process start rather than
// at the first publication. It must not be used on input received from a broker.
func MustParseType(s string) Type {
	t, err := ParseType(s)
	if err != nil {
		panic(err)
	}
	return t
}

func validSegment(seg string) error {
	if seg == "" {
		return fmt.Errorf("empty segment")
	}
	if seg[0] < 'a' || seg[0] > 'z' {
		return fmt.Errorf("segment %q must begin with a lowercase letter", seg)
	}
	for i := 0; i < len(seg); i++ {
		c := seg[i]
		switch {
		case c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9':
		case c == '-':
		default:
			return fmt.Errorf("segment %q contains %q; permitted are a-z, 0-9, and hyphen", seg, string(c))
		}
	}
	return nil
}

// validVersionSegment accepts v2 and above.
//
// Major version 1 is expressed by the absence of a version segment, so ".v1" is
// rejected deliberately. Permitting both spellings would give one contract two type
// names, and a consumer subscribed to one would silently miss the other.
func validVersionSegment(seg string) error {
	if len(seg) < 2 || seg[0] != 'v' {
		return fmt.Errorf("segment %q is not a major version", seg)
	}
	n, err := strconv.Atoi(seg[1:])
	if err != nil {
		return fmt.Errorf("segment %q is not a major version", seg)
	}
	if n == 1 {
		return fmt.Errorf("major version 1 is written by omitting the segment, not as %q", seg)
	}
	if n < 1 {
		return fmt.Errorf("major version %d is not valid", n)
	}
	return nil
}

// Domain returns the owning domain segment, which is the third.
func (t Type) Domain() string { return t.segment(2) }

// Aggregate returns the aggregate segment, which is the fourth.
func (t Type) Aggregate() string { return t.segment(3) }

// Lifecycle returns the lifecycle segment, which is the fifth. It distinguishes a
// security-relevant event from an ordinary one, which is what the dispatcher reads to
// decide the priority lane.
func (t Type) Lifecycle() string { return t.segment(4) }

// Action returns the action segment, which is the sixth.
func (t Type) Action() string { return t.segment(5) }

// MajorVersion returns the contract major version. A type without a version segment
// is major version 1.
func (t Type) MajorVersion() int {
	segments := strings.Split(string(t), ".")
	if len(segments) != typeSegmentsMax {
		return 1
	}
	n, err := strconv.Atoi(segments[typeSegmentsMax-1][1:])
	if err != nil {
		return 1
	}
	return n
}

// Base returns the type without its version segment, so two major versions of one
// contract can be recognised as the same contract.
func (t Type) Base() Type {
	segments := strings.Split(string(t), ".")
	if len(segments) != typeSegmentsMax {
		return t
	}
	return Type(strings.Join(segments[:typeSegmentsMax-1], "."))
}

func (t Type) String() string { return string(t) }

func (t Type) segment(i int) string {
	segments := strings.Split(string(t), ".")
	if i >= len(segments) {
		return ""
	}
	return segments[i]
}
