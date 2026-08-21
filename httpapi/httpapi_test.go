package httpapi

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anshacerbia2/foundation-platform/id"
	"github.com/anshacerbia2/foundation-platform/observability"
)

func testTelemetry(t *testing.T, output *bytes.Buffer) *observability.Telemetry {
	t.Helper()
	telemetry, err := observability.New(observability.Config{
		Deployable: "control-test",
		System:     "SAD-test",
		Logger:     slog.New(slog.NewJSONHandler(output, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return telemetry
}

func TestProblemUsesTheRegistryAndRedacts(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/v1/records?token=not-in-instance", nil)
	ctx, correlationID, err := observability.EnsureCorrelation(request.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	request = request.WithContext(ctx)
	response := httptest.NewRecorder()

	Problem(response, request, VersionConflict, "password=visible")
	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d", response.Code)
	}
	if got := response.Header().Get("Content-Type"); got != "application/problem+json" {
		t.Errorf("content type = %q", got)
	}
	var document ProblemDocument
	if err := json.Unmarshal(response.Body.Bytes(), &document); err != nil {
		t.Fatalf("problem JSON: %v", err)
	}
	if document.CorrelationID != correlationID.String() || document.Instance != "/v1/records" {
		t.Errorf("problem = %+v", document)
	}
	if strings.Contains(response.Body.String(), "visible") || strings.Contains(response.Body.String(), "not-in-instance") {
		t.Errorf("problem leaked sensitive input: %s", response.Body.String())
	}
}

func TestChainRunsConsumerHooksInOrder(t *testing.T) {
	var order []string
	middleware := func(name string) Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, name)
				next.ServeHTTP(w, r)
			})
		}
	}
	handler := Chain(Options{
		Authentication: middleware("authentication"),
		Authorization:  middleware("authorization"),
		Idempotency:    middleware("idempotency"),
	})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		order = append(order, "handler")
		w.WriteHeader(http.StatusNoContent)
	}))

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/", nil))
	if got := strings.Join(order, ","); got != "authentication,authorization,idempotency,handler" {
		t.Errorf("order = %s", got)
	}
}

func TestCorrelationIsPropagatedAndLogged(t *testing.T) {
	var output bytes.Buffer
	telemetry := testTelemetry(t, &output)
	want := id.MustParse("019235f1-8c4a-7c1e-9d0b-3f4a2b6e5d71")
	handler := Chain(Options{Telemetry: telemetry})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, ok := observability.CorrelationID(r.Context())
		if !ok || got != want {
			t.Errorf("correlation = %s, present = %v", got, ok)
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest(http.MethodGet, "/v1/records", nil)
	request.Header.Set(CorrelationHeader, want.String())
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Header().Get(CorrelationHeader) != want.String() {
		t.Errorf("response correlation = %q", response.Header().Get(CorrelationHeader))
	}
	if !strings.Contains(output.String(), want.String()) {
		t.Errorf("first request log omits correlation: %s", output.String())
	}
}

func TestPanicDiscardsPartialResponseAndWritesProblem(t *testing.T) {
	var output bytes.Buffer
	handler := Chain(Options{Telemetry: testTelemetry(t, &output)})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("partial-secret-response"))
		panic("password=visible")
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/panic", nil))

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", response.Code)
	}
	if strings.Contains(response.Body.String(), "partial-secret-response") {
		t.Errorf("partial response survived panic: %s", response.Body.String())
	}
	if response.Header().Get(CorrelationHeader) == "" {
		t.Error("panic response has no correlation identifier")
	}
	if strings.Contains(output.String(), "visible") {
		t.Errorf("panic log leaked a credential: %s", output.String())
	}
}

func TestLoadSheddingRunsBeforeAuthentication(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var authCalls atomic.Int64
	auth := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authCalls.Add(1)
			next.ServeHTTP(w, r)
		})
	}
	handler := Chain(Options{MaxInFlight: 1, Authentication: auth})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusNoContent)
	}))

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/first", nil))
	}()
	<-entered

	second := httptest.NewRecorder()
	handler.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/second", nil))
	close(release)
	<-done

	if second.Code != http.StatusServiceUnavailable {
		t.Errorf("shed status = %d", second.Code)
	}
	if authCalls.Load() != 1 {
		t.Errorf("authentication ran %d times, want only the admitted request", authCalls.Load())
	}
}

func TestTimeoutReachesTheHandlerContext(t *testing.T) {
	const budget = 50 * time.Millisecond
	handler := Chain(Options{Timeout: budget})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deadline, ok := r.Context().Deadline()
		if !ok || time.Until(deadline) > budget {
			t.Errorf("deadline = %v, present = %v", deadline, ok)
		}
		<-r.Context().Done()
		w.WriteHeader(http.StatusNoContent)
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/slow", nil))
}

func TestMalformedCorrelationIsNotReflected(t *testing.T) {
	handler := Chain(Options{})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set(CorrelationHeader, "attacker-controlled")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if got := response.Header().Get(CorrelationHeader); got == "attacker-controlled" || got == "" {
		t.Errorf("correlation response = %q", got)
	}
}

// TestEveryProblemTypeIsRegistered closes the gap that a new constant makes reachable.
//
// ProblemType is an iota, so adding a constant compiles whether or not the registry gained an
// entry, and an unregistered type would reach a caller as an empty document with a zero status.
// The loop walks the whole range rather than a hand-written list, so a constant added tomorrow is
// covered without anyone remembering to extend this test.
func TestEveryProblemTypeIsRegistered(t *testing.T) {
	for kind := ValidationFailed; kind <= RequestInProgress+ProblemType(len(problemRegistry)); kind++ {
		definition, ok := problemRegistry[kind]
		if !ok {
			// Past the end of the declared constants, which is where the loop is expected to stop.
			continue
		}
		if definition.URI == "" || definition.Title == "" || definition.Status == 0 {
			t.Errorf("problem type %d is registered with an incomplete definition: %+v", kind, definition)
		}
	}

	// Count rather than range: the registry and the constant block must agree, and a constant
	// added without an entry is exactly the omission this asserts.
	if len(problemRegistry) != int(Internal) {
		t.Errorf("the registry holds %d definitions and the constants declare %d; one was added without the other",
			len(problemRegistry), int(Internal))
	}
}

// TestRequestInProgressIsDistinctFromStateTransitionRefused states why the type was added.
//
// Both answer 409, and the consuming service was using the refusal type for an in-flight retry
// because nothing better existed. The two carry opposite advice: a refused transition means no
// retry will help, an in-progress request means the retry is what will. A client that cannot tell
// them apart gives up when it should wait.
func TestRequestInProgressIsDistinctFromStateTransitionRefused(t *testing.T) {
	inProgress := problemRegistry[RequestInProgress]
	refused := problemRegistry[StateTransitionRefused]

	if inProgress.URI == refused.URI {
		t.Error("the two types share a URI, so a client cannot distinguish them")
	}
	if inProgress.Status != refused.Status {
		t.Errorf("status differs: %d and %d; both are conflicts", inProgress.Status, refused.Status)
	}
}
