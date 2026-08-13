package id

import (
	"encoding/json"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestNewV7VersionAndVariant(t *testing.T) {
	u, err := NewV7()
	if err != nil {
		t.Fatalf("NewV7: %v", err)
	}
	if got := u.Version(); got != 7 {
		t.Errorf("version = %d, want 7", got)
	}
	if got := u.Variant(); got != 0b10 {
		t.Errorf("variant = %b, want 10", got)
	}
	if u.IsNil() {
		t.Error("NewV7 returned the nil identifier")
	}
}

// TDD-identity-control-001 requires values to be monotonically ordered within a
// process. Generating far more than the 4096 counter slots forces the wait-for-next-
// millisecond path, which is where wrapping would break ordering.
func TestNewV7MonotonicWithinProcess(t *testing.T) {
	const n = 20_000

	prev, err := NewV7()
	if err != nil {
		t.Fatalf("NewV7: %v", err)
	}

	for i := 1; i < n; i++ {
		next, err := NewV7()
		if err != nil {
			t.Fatalf("NewV7 at %d: %v", i, err)
		}
		if Compare(next, prev) <= 0 {
			t.Fatalf("at %d: %s does not sort after %s", i, next, prev)
		}
		prev = next
	}
}

func TestNewV7UniqueUnderConcurrency(t *testing.T) {
	const (
		goroutines = 16
		perRoutine = 500
	)

	var (
		wg  sync.WaitGroup
		mu  sync.Mutex
		all = make([]UUID, 0, goroutines*perRoutine)
	)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]UUID, 0, perRoutine)
			for i := 0; i < perRoutine; i++ {
				u, err := NewV7()
				if err != nil {
					t.Errorf("NewV7: %v", err)
					return
				}
				local = append(local, u)
			}
			mu.Lock()
			all = append(all, local...)
			mu.Unlock()
		}()
	}
	wg.Wait()

	seen := make(map[UUID]struct{}, len(all))
	for _, u := range all {
		if _, dup := seen[u]; dup {
			t.Fatalf("duplicate identifier %s under concurrency", u)
		}
		seen[u] = struct{}{}
	}
	if len(seen) != goroutines*perRoutine {
		t.Fatalf("generated %d unique, want %d", len(seen), goroutines*perRoutine)
	}
}

// Byte order must equal creation order, because index locality on every downstream
// table depends on it.
func TestSortedByteOrderMatchesCreationOrder(t *testing.T) {
	const n = 5_000

	created := make([]UUID, n)
	for i := range created {
		u, err := NewV7()
		if err != nil {
			t.Fatalf("NewV7: %v", err)
		}
		created[i] = u
	}

	sorted := make([]UUID, n)
	copy(sorted, created)
	sort.Slice(sorted, func(i, j int) bool { return Compare(sorted[i], sorted[j]) < 0 })

	for i := range created {
		if created[i] != sorted[i] {
			t.Fatalf("at %d: creation order and byte order diverge", i)
		}
	}
}

func TestTimestampWithinTolerance(t *testing.T) {
	before := time.Now().UTC()
	u, err := NewV7()
	if err != nil {
		t.Fatalf("NewV7: %v", err)
	}
	after := time.Now().UTC()

	ts := u.Timestamp()
	// The embedded value is millisecond-truncated, so allow the truncation window.
	if ts.Before(before.Add(-time.Millisecond)) || ts.After(after.Add(time.Millisecond)) {
		t.Errorf("timestamp %v outside [%v, %v]", ts, before, after)
	}
}

func TestStringRoundTrip(t *testing.T) {
	u, err := NewV7()
	if err != nil {
		t.Fatalf("NewV7: %v", err)
	}

	parsed, err := Parse(u.String())
	if err != nil {
		t.Fatalf("Parse(%q): %v", u.String(), err)
	}
	if parsed != u {
		t.Errorf("round trip changed the value: %s != %s", parsed, u)
	}
}

func TestParseRejectsTheNilIdentifier(t *testing.T) {
	if _, err := Parse("00000000-0000-0000-0000-000000000000"); err == nil {
		t.Error("Parse accepted the nil identifier")
	}
}

func TestStringForm(t *testing.T) {
	u := MustParse("019235f1-8c4a-7c1e-9d0b-3f4a2b6e5d71")
	if got := u.String(); got != "019235f1-8c4a-7c1e-9d0b-3f4a2b6e5d71" {
		t.Errorf("String() = %q", got)
	}
	if got := u.Version(); got != 7 {
		t.Errorf("version = %d, want 7", got)
	}
}

func TestParseRejectsInvalid(t *testing.T) {
	for _, s := range []string{
		"",
		"not-a-uuid",
		"019235f18c4a7c1e9d0b3f4a2b6e5d71",      // unhyphenated
		"019235f1-8c4a-7c1e-9d0b-3f4a2b6e5d7",   // too short
		"019235f1-8c4a-7c1e-9d0b-3f4a2b6e5d711", // too long
		"019235f1_8c4a-7c1e-9d0b-3f4a2b6e5d71",  // wrong separator
		"019235g1-8c4a-7c1e-9d0b-3f4a2b6e5d71",  // non-hex
	} {
		if _, err := Parse(s); err == nil {
			t.Errorf("Parse(%q) accepted an invalid value", s)
		}
	}
}

// The identifier must serialize as its canonical string, not as a byte array, or every
// consumer persists a shape nobody intended.
func TestJSONUsesCanonicalString(t *testing.T) {
	type wrapper struct {
		PrincipalID UUID `json:"principal_id"`
	}

	in := wrapper{PrincipalID: MustParse("019235f1-8c4a-7c1e-9d0b-3f4a2b6e5d71")}

	encoded, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	const want = `{"principal_id":"019235f1-8c4a-7c1e-9d0b-3f4a2b6e5d71"}`
	if string(encoded) != want {
		t.Errorf("Marshal = %s, want %s", encoded, want)
	}

	var out wrapper
	if err := json.Unmarshal(encoded, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out.PrincipalID != in.PrincipalID {
		t.Errorf("Unmarshal changed the value")
	}
}

func TestJSONRejectsInvalid(t *testing.T) {
	var u UUID
	if err := json.Unmarshal([]byte(`"nope"`), &u); err == nil {
		t.Error("Unmarshal accepted an invalid value")
	}
}

func TestNilIsNotGenerated(t *testing.T) {
	if !Nil.IsNil() {
		t.Error("Nil.IsNil() = false")
	}
	var zero UUID
	if Nil != zero {
		t.Error("Nil is not the zero value")
	}
}

func BenchmarkNewV7(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if _, err := NewV7(); err != nil {
			b.Fatal(err)
		}
	}
}
