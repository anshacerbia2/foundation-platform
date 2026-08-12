// Package event constructs and validates the enterprise event envelope.
//
// STD-GLB-004 mandates CloudEvents 1.0 in JSON for every asynchronous event, and its
// exception clause reads "None. All event-driven architecture rules apply
// unconditionally." This package is the single place that envelope is built, so a
// producing system cannot emit a shape that does not conform.
//
// It holds no domain concept. It knows an event has a type, a source, and a payload;
// it does not know what any of them mean.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/anshacerbia2/foundation-platform/id"
)

// SpecVersion is the only CloudEvents version this enterprise emits or accepts.
const SpecVersion = "1.0"

// ContentTypeJSON is the only payload encoding this enterprise emits.
const ContentTypeJSON = "application/json"

// Source identifies the emitting system as a URI reference.
//
// It is a value object for the same reason Type is: a source that varies in spelling
// between two deployments of one system makes every consumer-side filter unreliable.
type Source string

// ParseSource validates s as an absolute-path URI reference such as
// "/systems/organization-control".
func ParseSource(s string) (Source, error) {
	switch {
	case s == "":
		return "", errors.New("event: source must not be empty")
	case !strings.HasPrefix(s, "/"):
		return "", fmt.Errorf("event: source %q must be an absolute path reference", s)
	case strings.Contains(s, " "):
		return "", fmt.Errorf("event: source %q must not contain a space", s)
	}
	return Source(s), nil
}

// MustParseSource is ParseSource for a package-level constant in a producing system.
func MustParseSource(s string) Source {
	src, err := ParseSource(s)
	if err != nil {
		panic(err)
	}
	return src
}

func (s Source) String() string { return string(s) }

// Envelope is a CloudEvents 1.0 event in its enterprise form.
//
// The seven fields STD-GLB-004 requires are all present and all mandatory. DataSchema
// is the one optional member, and it carries the registry location of the payload
// contract.
//
// Domain-specific fields — aggregate version, correlation, causation — live inside
// Data, because that is the layer that owns them. Placing them beside the CloudEvents
// members would extend the envelope with meaning this package cannot validate.
type Envelope struct {
	SpecVersion     string
	ID              id.UUID
	Source          Source
	Type            Type
	Time            time.Time
	DataContentType string
	DataSchema      string
	Data            json.RawMessage
}

// New builds an envelope for payload, which is marshalled to JSON.
//
// occurredAt is supplied by the caller rather than read from the wall clock here.
// The caller owns the fact and therefore owns when it happened, and a state machine
// that supplies its own time is reproducible in a test while one that reads the clock
// is not.
func New(source Source, typ Type, occurredAt time.Time, payload any) (Envelope, error) {
	if source == "" {
		return Envelope{}, errors.New("event: source is required")
	}
	if typ == "" {
		return Envelope{}, errors.New("event: type is required")
	}
	if occurredAt.IsZero() {
		return Envelope{}, errors.New("event: occurredAt is required")
	}

	eventID, err := id.NewV7()
	if err != nil {
		return Envelope{}, fmt.Errorf("event: generating identifier: %w", err)
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("event: marshalling payload: %w", err)
	}

	return Envelope{
		SpecVersion:     SpecVersion,
		ID:              eventID,
		Source:          source,
		Type:            typ,
		Time:            occurredAt.UTC(),
		DataContentType: ContentTypeJSON,
		Data:            data,
	}, nil
}

// WithSchema records the payload contract location. ADR-GLB-006 requires every event
// schema to be registered, and this member is how a consumer resolves the one that
// applies to this payload.
func (e Envelope) WithSchema(uri string) Envelope {
	e.DataSchema = uri
	return e
}

// Validate reports whether the envelope satisfies STD-GLB-004.
//
// It runs on construction and again on receipt. An envelope arriving from a broker
// has crossed a boundary this process does not control, and accepting one with a
// missing member would push the failure into a handler that has no way to report it
// as a contract violation.
func (e Envelope) Validate() error {
	var problems []string

	if e.SpecVersion != SpecVersion {
		problems = append(problems, fmt.Sprintf("specversion is %q, want %q", e.SpecVersion, SpecVersion))
	}
	if e.ID.IsNil() {
		problems = append(problems, "id is absent")
	}
	if e.Source == "" {
		problems = append(problems, "source is absent")
	}
	if e.Type == "" {
		problems = append(problems, "type is absent")
	} else if _, err := ParseType(string(e.Type)); err != nil {
		problems = append(problems, err.Error())
	}
	if e.Time.IsZero() {
		problems = append(problems, "time is absent")
	}
	if e.DataContentType != ContentTypeJSON {
		problems = append(problems, fmt.Sprintf("datacontenttype is %q, want %q", e.DataContentType, ContentTypeJSON))
	}
	if len(e.Data) == 0 {
		problems = append(problems, "data is absent")
	}

	if len(problems) > 0 {
		return fmt.Errorf("event: invalid envelope: %s", strings.Join(problems, "; "))
	}
	return nil
}

// wire is the JSON representation. Field names are the CloudEvents members exactly,
// which is why the wire form is a separate type rather than struct tags on Envelope:
// the Go field names read for Go callers and the wire names read for every consumer,
// and neither has to compromise.
type wire struct {
	SpecVersion     string          `json:"specversion"`
	ID              id.UUID         `json:"id"`
	Source          string          `json:"source"`
	Type            string          `json:"type"`
	Time            string          `json:"time"`
	DataContentType string          `json:"datacontenttype"`
	DataSchema      string          `json:"dataschema,omitempty"`
	Data            json.RawMessage `json:"data"`
}

// MarshalJSON emits the CloudEvents 1.0 JSON form.
func (e Envelope) MarshalJSON() ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(wire{
		SpecVersion:     e.SpecVersion,
		ID:              e.ID,
		Source:          string(e.Source),
		Type:            string(e.Type),
		Time:            e.Time.UTC().Format(time.RFC3339Nano),
		DataContentType: e.DataContentType,
		DataSchema:      e.DataSchema,
		Data:            e.Data,
	})
}

// UnmarshalJSON reads the CloudEvents 1.0 JSON form and validates it.
func (e *Envelope) UnmarshalJSON(b []byte) error {
	var w wire
	if err := json.Unmarshal(b, &w); err != nil {
		return fmt.Errorf("event: decoding envelope: %w", err)
	}

	occurred, err := time.Parse(time.RFC3339Nano, w.Time)
	if err != nil {
		return fmt.Errorf("event: decoding time %q: %w", w.Time, err)
	}

	candidate := Envelope{
		SpecVersion:     w.SpecVersion,
		ID:              w.ID,
		Source:          Source(w.Source),
		Type:            Type(w.Type),
		Time:            occurred.UTC(),
		DataContentType: w.DataContentType,
		DataSchema:      w.DataSchema,
		Data:            w.Data,
	}
	if err := candidate.Validate(); err != nil {
		return err
	}

	*e = candidate
	return nil
}

// UnmarshalData decodes the payload into target.
func (e Envelope) UnmarshalData(target any) error {
	if len(e.Data) == 0 {
		return errors.New("event: envelope carries no data")
	}
	if err := json.Unmarshal(e.Data, target); err != nil {
		return fmt.Errorf("event: decoding data for %s: %w", e.Type, err)
	}
	return nil
}
