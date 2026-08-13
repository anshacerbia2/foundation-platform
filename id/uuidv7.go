// Package id generates the canonical Scnehaux identifier form.
//
// UUIDv7 is the enterprise default for externally durable identifiers under
// STD-GLB-002. It is time-ordered, which gives index locality on the mapping tables
// and on every downstream table that references one, and it is non-enumerable, which
// a sequential identifier is not.
//
// This package holds no domain concept. It generates identifiers; it does not know
// what they identify.
package id

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

// UUID is a 128-bit identifier in canonical byte order.
type UUID [16]byte

// Nil is the zero identifier. NewV7 never returns it, and it is never valid as a
// canonical identifier.
var Nil UUID

// ErrInvalid reports a value that is not a canonical UUID.
var ErrInvalid = errors.New("id: not a canonical UUID")

const maxSequence = 0x0FFF

var (
	mu       sync.Mutex
	lastMS   int64
	sequence uint16
)

// NewV7 returns a UUIDv7 as specified by RFC 9562.
//
// Layout:
//
//	unix_ts_ms (48) | ver (4) | seq (12) | var (2) | random (62)
//
// The 12 bits RFC 9562 leaves to rand_a carry a monotonic counter rather than random
// data, so two identifiers generated in the same millisecond still order correctly.
// TDD-identity-control-001 relies on that when it requires values to be monotonically
// ordered within a process.
//
// When the counter is exhausted inside one millisecond, generation waits for the next
// millisecond rather than wrapping. Wrapping would produce a value sorting before its
// predecessor, and a caller relying on ordering has no way to detect that.
func NewV7() (UUID, error) {
	var u UUID

	ms, seq := nextSequence()

	// 48 bits of Unix milliseconds, big-endian.
	u[0] = byte(ms >> 40)
	u[1] = byte(ms >> 32)
	u[2] = byte(ms >> 24)
	u[3] = byte(ms >> 16)
	u[4] = byte(ms >> 8)
	u[5] = byte(ms)

	// Version 7 in the high nibble of byte 6, then the 12-bit counter.
	u[6] = 0x70 | byte((seq>>8)&0x0F)
	u[7] = byte(seq)

	// 62 bits of randomness, with the RFC 4122 variant in the top two bits of byte 8.
	if _, err := rand.Read(u[8:]); err != nil {
		return Nil, err
	}
	u[8] = (u[8] & 0x3F) | 0x80

	return u, nil
}

// nextSequence returns the millisecond to encode and the counter to pair with it.
//
// A wall clock that moves backwards is handled by continuing on the last observed
// millisecond rather than by emitting an identifier that sorts before one already
// handed out. Correct ordering is worth more here than an accurate embedded timestamp,
// because consumers are forbidden from reading the timestamp and are permitted to rely
// on the ordering.
func nextSequence() (int64, uint16) {
	for {
		mu.Lock()

		if ms := time.Now().UnixMilli(); ms > lastMS {
			lastMS = ms
			sequence = 0
			mu.Unlock()
			return ms, 0
		}

		if sequence < maxSequence {
			sequence++
			ms, seq := lastMS, sequence
			mu.Unlock()
			return ms, seq
		}

		mu.Unlock()
		time.Sleep(time.Millisecond)
	}
}

// Version reports the UUID version nibble.
func (u UUID) Version() int { return int(u[6] >> 4) }

// Variant reports the two-bit RFC 4122 variant field.
func (u UUID) Variant() int { return int(u[8] >> 6) }

// Timestamp returns the embedded creation time.
//
// Consumers must not depend on this. The identifier is opaque and the embedded
// timestamp exists for index locality, not for business logic. It is exposed for tests
// and operational forensics.
func (u UUID) Timestamp() time.Time {
	ms := int64(u[0])<<40 | int64(u[1])<<32 | int64(u[2])<<24 |
		int64(u[3])<<16 | int64(u[4])<<8 | int64(u[5])
	return time.UnixMilli(ms).UTC()
}

// IsNil reports whether the identifier is the zero value.
func (u UUID) IsNil() bool { return u == Nil }

// String returns the canonical 8-4-4-4-12 hyphenated lowercase form.
func (u UUID) String() string {
	var b [36]byte
	hex.Encode(b[0:8], u[0:4])
	b[8] = '-'
	hex.Encode(b[9:13], u[4:6])
	b[13] = '-'
	hex.Encode(b[14:18], u[6:8])
	b[18] = '-'
	hex.Encode(b[19:23], u[8:10])
	b[23] = '-'
	hex.Encode(b[24:36], u[10:16])
	return string(b[:])
}

// MarshalText implements encoding.TextMarshaler, so JSON encoding produces the
// canonical string rather than a byte array.
func (u UUID) MarshalText() ([]byte, error) { return []byte(u.String()), nil }

// UnmarshalText implements encoding.TextUnmarshaler.
func (u *UUID) UnmarshalText(text []byte) error {
	parsed, err := Parse(string(text))
	if err != nil {
		return err
	}
	*u = parsed
	return nil
}

// Parse reads the canonical hyphenated form. It accepts any version, because an
// identifier minted under an earlier scheme must remain readable.
func Parse(s string) (UUID, error) {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return Nil, ErrInvalid
	}
	var u UUID
	for _, seg := range [...]struct{ dst, lo, hi int }{
		{0, 0, 8}, {4, 9, 13}, {6, 14, 18}, {8, 19, 23}, {10, 24, 36},
	} {
		if _, err := hex.Decode(u[seg.dst:], []byte(s[seg.lo:seg.hi])); err != nil {
			return Nil, ErrInvalid
		}
	}
	if u.IsNil() {
		return Nil, ErrInvalid
	}
	return u, nil
}

// MustParse is Parse for compile-time constants and test fixtures. It panics on an
// invalid value and must not be used on input from a request.
func MustParse(s string) UUID {
	u, err := Parse(s)
	if err != nil {
		panic(err)
	}
	return u
}

// Compare orders two identifiers by byte order, which for UUIDv7 is creation order.
// It returns a negative number, zero, or a positive number.
func Compare(a, b UUID) int { return bytes.Compare(a[:], b[:]) }
