package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/anshacerbia2/foundation-platform/observability"
	"github.com/anshacerbia2/foundation-platform/redact"
)

// ProblemType is a closed registry key for the RFC 7807 types exposed by control APIs.
type ProblemType uint8

const (
	ValidationFailed ProblemType = iota + 1
	AuthenticationRequired
	Forbidden
	NotFound
	VersionConflict
	IdempotencyKeyConflict
	StateTransitionRefused

	// RequestInProgress is a retry arriving while the original request is still running.
	//
	// Distinct from StateTransitionRefused, which consumers were using for it because nothing
	// better existed. The two are the same status and different facts: a refused transition means
	// the caller asked for something the record cannot do, and no retry will change that; an
	// in-progress request means the caller asked for something that is happening, and retrying
	// after a moment is the correct response. Collapsing them tells a client to give up when it
	// should wait.
	RequestInProgress
	PreconditionUnmet
	RateLimited
	Overloaded
	DependencyUnavailable
	Internal
)

type problemDefinition struct {
	URI    string
	Title  string
	Status int
}

var problemRegistry = map[ProblemType]problemDefinition{
	ValidationFailed:       {"https://problems.scnehaux.com/validation-failed", "The request is invalid", http.StatusBadRequest},
	AuthenticationRequired: {"https://problems.scnehaux.com/authentication-required", "Authentication is required", http.StatusUnauthorized},
	Forbidden:              {"https://problems.scnehaux.com/forbidden", "The operation is forbidden", http.StatusForbidden},
	NotFound:               {"https://problems.scnehaux.com/not-found", "The requested resource was not found", http.StatusNotFound},
	VersionConflict:        {"https://problems.scnehaux.com/version-conflict", "The record changed since it was read", http.StatusConflict},
	IdempotencyKeyConflict: {"https://problems.scnehaux.com/idempotency-key-conflict", "The idempotency key was reused with a different request", http.StatusConflict},
	StateTransitionRefused: {"https://problems.scnehaux.com/state-transition-refused", "The requested state transition was refused", http.StatusConflict},
	RequestInProgress:      {"https://problems.scnehaux.com/request-in-progress", "An identical request is still in progress", http.StatusConflict},
	PreconditionUnmet:      {"https://problems.scnehaux.com/precondition-unmet", "A request precondition was not met", http.StatusPreconditionFailed},
	RateLimited:            {"https://problems.scnehaux.com/rate-limited", "The request rate is too high", http.StatusTooManyRequests},
	Overloaded:             {"https://problems.scnehaux.com/overloaded", "The service is overloaded", http.StatusServiceUnavailable},
	DependencyUnavailable:  {"https://problems.scnehaux.com/dependency-unavailable", "A required dependency is unavailable", http.StatusServiceUnavailable},
	Internal:               {"https://problems.scnehaux.com/internal", "An internal error occurred", http.StatusInternalServerError},
}

// ProblemDocument is the sole error representation written by this package.
type ProblemDocument struct {
	Type          string `json:"type"`
	Title         string `json:"title"`
	Status        int    `json:"status"`
	Detail        string `json:"detail,omitempty"`
	Instance      string `json:"instance"`
	CorrelationID string `json:"correlation_id,omitempty"`
}

// Problem writes an RFC 7807 response from the compiled registry.
func Problem(w http.ResponseWriter, r *http.Request, problemType ProblemType, detail string) {
	definition, ok := problemRegistry[problemType]
	if !ok {
		definition = problemRegistry[Internal]
	}

	document := ProblemDocument{
		Type:     definition.URI,
		Title:    definition.Title,
		Status:   definition.Status,
		Detail:   redact.String(detail),
		Instance: r.URL.Path,
	}
	if correlationID, ok := observability.CorrelationID(r.Context()); ok {
		document.CorrelationID = correlationID.String()
		w.Header().Set(CorrelationHeader, correlationID.String())
	}

	w.Header().Set("Content-Type", "application/problem+json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(definition.Status)
	_ = json.NewEncoder(w).Encode(document)
}
