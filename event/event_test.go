package event

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

const (
	revoked   = "com.scnehaux.organization.membership.security.revoked"
	activated = "com.scnehaux.organization.tenant.lifecycle.activated"
	revokedV2 = "com.scnehaux.organization.membership.security.revoked.v2"
)

func TestParseTypeAcceptsEnterpriseForm(t *testing.T) {
	for _, s := range []string{
		revoked,
		activated,
		revokedV2,
		"com.scnehaux.identity.principal.lifecycle.activated",
		"com.scnehaux.identity.protocol-client.lifecycle.registered",
		"com.scnehaux.organization.membership.security.revoked.v10",
	} {
		if _, err := ParseType(s); err != nil {
			t.Errorf("ParseType(%q): %v", s, err)
		}
	}
}

func TestParseTypeRejects(t *testing.T) {
	for name, s := range map[string]string{
		"wrong prefix":      "org.example.identity.principal.lifecycle.activated",
		"no prefix":         "identity.principal.lifecycle.activated",
		"too few segments":  "com.scnehaux.identity.principal.activated",
		"too many segments": "com.scnehaux.identity.principal.lifecycle.activated.extra.v2",
		"uppercase":         "com.scnehaux.identity.Principal.lifecycle.activated",
		"underscore":        "com.scnehaux.identity.principal_record.lifecycle.activated",
		"leading digit":     "com.scnehaux.identity.1principal.lifecycle.activated",
		"empty segment":     "com.scnehaux.identity..lifecycle.activated.now",
		"version not last":  "com.scnehaux.identity.v2.principal.lifecycle.activated",
		"malformed version": "com.scnehaux.identity.principal.lifecycle.activated.version2",
		"empty":             "",
	} {
		if _, err := ParseType(s); err == nil {
			t.Errorf("%s: ParseType(%q) accepted an invalid type", name, s)
		}
	}
}

// Major version 1 is expressed by omitting the segment. Accepting ".v1" as well would
// give one contract two names, and a consumer subscribed to one would miss the other.
func TestParseTypeRejectsExplicitV1(t *testing.T) {
	s := revoked + ".v1"
	_, err := ParseType(s)
	if err == nil {
		t.Fatalf("ParseType(%q) accepted an explicit major version 1", s)
	}
	if !strings.Contains(err.Error(), "omitting") {
		t.Errorf("error does not explain the rule: %v", err)
	}
}

func TestTypeSegments(t *testing.T) {
	typ := MustParseType(revoked)

	for _, tc := range []struct{ name, got, want string }{
		{"domain", typ.Domain(), "organization"},
		{"aggregate", typ.Aggregate(), "membership"},
		{"lifecycle", typ.Lifecycle(), "security"},
		{"action", typ.Action(), "revoked"},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %q, want %q", tc.name, tc.got, tc.want)
		}
	}
}

func TestTypeMajorVersionAndBase(t *testing.T) {
	v1 := MustParseType(revoked)
	v2 := MustParseType(revokedV2)

	if got := v1.MajorVersion(); got != 1 {
		t.Errorf("unversioned MajorVersion() = %d, want 1", got)
	}
	if got := v2.MajorVersion(); got != 2 {
		t.Errorf("v2 MajorVersion() = %d, want 2", got)
	}
	if v1.Base() != v2.Base() {
		t.Errorf("Base() differs across major versions: %q vs %q", v1.Base(), v2.Base())
	}
}

func TestParseSource(t *testing.T) {
	if _, err := ParseSource("/systems/organization-control"); err != nil {
		t.Errorf("ParseSource: %v", err)
	}
	for name, s := range map[string]string{
		"empty":    "",
		"relative": "systems/organization-control",
		"spaced":   "/systems/organization control",
	} {
		if _, err := ParseSource(s); err == nil {
			t.Errorf("%s: ParseSource(%q) accepted an invalid source", name, s)
		}
	}
}

type revocation struct {
	MembershipID  string `json:"membership_id"`
	PrincipalID   string `json:"principal_id"`
	Version       int64  `json:"membership_version"`
	CorrelationID string `json:"correlation_id"`
}

func sample(t *testing.T) Envelope {
	t.Helper()
	occurred := time.Date(2026, 8, 11, 9, 14, 22, 0, time.UTC)
	e, err := New(
		MustParseSource("/systems/organization-control"),
		MustParseType(revoked),
		occurred,
		revocation{MembershipID: "m-1", PrincipalID: "p-1", Version: 15, CorrelationID: "c-1"},
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return e
}

func TestNewProducesConformingEnvelope(t *testing.T) {
	e := sample(t)

	if e.SpecVersion != SpecVersion {
		t.Errorf("specversion = %q", e.SpecVersion)
	}
	if e.DataContentType != ContentTypeJSON {
		t.Errorf("datacontenttype = %q", e.DataContentType)
	}
	if e.ID.IsNil() {
		t.Error("id is nil")
	}
	if e.ID.Version() != 7 {
		t.Errorf("id is UUIDv%d, want v7", e.ID.Version())
	}
	if err := e.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

func TestNewRejectsIncompleteInput(t *testing.T) {
	occurred := time.Now()
	src := MustParseSource("/systems/organization-control")
	typ := MustParseType(revoked)

	if _, err := New("", typ, occurred, struct{}{}); err == nil {
		t.Error("New accepted an empty source")
	}
	if _, err := New(src, "", occurred, struct{}{}); err == nil {
		t.Error("New accepted an empty type")
	}
	if _, err := New(src, typ, time.Time{}, struct{}{}); err == nil {
		t.Error("New accepted a zero occurrence time")
	}
}

// STD-GLB-004 names the seven members. A rename in this package would silently break
// every consumer, so the wire keys are asserted literally.
func TestWireFormCarriesCloudEventsMembers(t *testing.T) {
	encoded, err := json.Marshal(sample(t))
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("Unmarshal to map: %v", err)
	}

	for _, member := range []string{
		"specversion", "id", "source", "type", "time", "datacontenttype", "data",
	} {
		if _, present := decoded[member]; !present {
			t.Errorf("required CloudEvents member %q is absent from the wire form", member)
		}
	}
	if _, present := decoded["dataschema"]; present {
		t.Error("dataschema is emitted when unset; it must be omitted")
	}
}

func TestWithSchemaIsEmitted(t *testing.T) {
	const uri = "https://schemas.scnehaux.com/organization/membership.security.revoked/1"

	encoded, err := json.Marshal(sample(t).WithSchema(uri))
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !strings.Contains(string(encoded), uri) {
		t.Errorf("dataschema absent from wire form: %s", encoded)
	}
}

func TestRoundTrip(t *testing.T) {
	original := sample(t)

	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var decoded Envelope
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if decoded.ID != original.ID {
		t.Errorf("id changed across the round trip")
	}
	if decoded.Type != original.Type {
		t.Errorf("type = %q, want %q", decoded.Type, original.Type)
	}
	if !decoded.Time.Equal(original.Time) {
		t.Errorf("time = %v, want %v", decoded.Time, original.Time)
	}

	var payload revocation
	if err := decoded.UnmarshalData(&payload); err != nil {
		t.Fatalf("UnmarshalData: %v", err)
	}
	if payload.MembershipID != "m-1" || payload.Version != 15 {
		t.Errorf("payload did not survive: %+v", payload)
	}
}

// An envelope from a broker has crossed a boundary this process does not control.
// Accepting an incomplete one pushes the failure into a handler with no way to report
// it as a contract violation.
func TestUnmarshalRejectsNonConformingEnvelope(t *testing.T) {
	for name, body := range map[string]string{
		"missing specversion": `{"id":"019235f1-8c4a-7c1e-9d0b-3f4a2b6e5d71","source":"/s","type":"` + revoked + `","time":"2026-08-11T09:14:22Z","datacontenttype":"application/json","data":{}}`,
		"wrong specversion":   `{"specversion":"0.3","id":"019235f1-8c4a-7c1e-9d0b-3f4a2b6e5d71","source":"/s","type":"` + revoked + `","time":"2026-08-11T09:14:22Z","datacontenttype":"application/json","data":{}}`,
		"missing type":        `{"specversion":"1.0","id":"019235f1-8c4a-7c1e-9d0b-3f4a2b6e5d71","source":"/s","time":"2026-08-11T09:14:22Z","datacontenttype":"application/json","data":{}}`,
		"malformed type":      `{"specversion":"1.0","id":"019235f1-8c4a-7c1e-9d0b-3f4a2b6e5d71","source":"/s","type":"whatever","time":"2026-08-11T09:14:22Z","datacontenttype":"application/json","data":{}}`,
		"wrong content type":  `{"specversion":"1.0","id":"019235f1-8c4a-7c1e-9d0b-3f4a2b6e5d71","source":"/s","type":"` + revoked + `","time":"2026-08-11T09:14:22Z","datacontenttype":"application/xml","data":{}}`,
		"missing data":        `{"specversion":"1.0","id":"019235f1-8c4a-7c1e-9d0b-3f4a2b6e5d71","source":"/s","type":"` + revoked + `","time":"2026-08-11T09:14:22Z","datacontenttype":"application/json"}`,
		"malformed time":      `{"specversion":"1.0","id":"019235f1-8c4a-7c1e-9d0b-3f4a2b6e5d71","source":"/s","type":"` + revoked + `","time":"yesterday","datacontenttype":"application/json","data":{}}`,
	} {
		var e Envelope
		if err := json.Unmarshal([]byte(body), &e); err == nil {
			t.Errorf("%s: Unmarshal accepted a non-conforming envelope", name)
		}
	}
}

func TestMarshalRefusesInvalidEnvelope(t *testing.T) {
	if _, err := json.Marshal(Envelope{}); err == nil {
		t.Error("Marshal emitted an invalid envelope")
	}
}

// Two events built in the same millisecond must still carry distinct identifiers,
// because the identifier is the deduplication key the consumer guard reads.
func TestIdentifiersAreDistinct(t *testing.T) {
	const n = 2_000
	seen := make(map[string]struct{}, n)

	for i := 0; i < n; i++ {
		e := sample(t)
		key := e.ID.String()
		if _, dup := seen[key]; dup {
			t.Fatalf("duplicate event identifier %s at %d", key, i)
		}
		seen[key] = struct{}{}
	}
}

func TestUnmarshalDataRejectsEmpty(t *testing.T) {
	var target revocation
	if err := (Envelope{}).UnmarshalData(&target); err == nil {
		t.Error("UnmarshalData accepted an envelope with no data")
	}
}
