package observability_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/anshacerbia2/foundation-platform/observability"
)

// A Collector stand-in that records which OTLP paths were posted to.
type collector struct {
	mu    sync.Mutex
	paths map[string]int
}

func (c *collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	c.paths[r.Method+" "+r.URL.Path]++
	c.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (c *collector) saw(path string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.paths[path] > 0
}

// What the export is for: a metric recorded and a span ended reach the Collector, flushed on
// Shutdown, which is the last thing a deployable does.
func TestExportDeliversMetricsAndSpansToTheCollector(t *testing.T) {
	sink := &collector{paths: map[string]int{}}
	server := httptest.NewServer(sink)
	defer server.Close()

	exported, err := observability.Export(context.Background(), observability.ExportConfig{
		Endpoint: server.URL + "/", Deployable: "export-test", System: "SAD-000", Interval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Export: %v", err)
	}

	counter, err := exported.MeterProvider.Meter("test").Int64Counter("export.test.events")
	if err != nil {
		t.Fatalf("counter: %v", err)
	}
	counter.Add(context.Background(), 1)
	_, span := exported.TracerProvider.Tracer("test").Start(context.Background(), "export-test")
	span.End()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := exported.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	for _, path := range []string{"POST /v1/metrics", "POST /v1/traces"} {
		if !sink.saw(path) {
			t.Errorf("the Collector never received %s; recorded telemetry was not exported", path)
		}
	}
}

func TestExportRefusesWhatCannotBeAnEndpoint(t *testing.T) {
	for _, endpoint := range []string{"", "   ", "collector:4318", "ftp://collector:4318", "http://"} {
		if _, err := observability.Export(context.Background(), observability.ExportConfig{
			Endpoint: endpoint, Deployable: "d", System: "s",
		}); err == nil {
			t.Errorf("Export accepted %q as an OTLP endpoint", endpoint)
		}
	}
	if _, err := observability.Export(context.Background(), observability.ExportConfig{
		Endpoint: "http://collector:4318",
	}); err == nil {
		t.Error("Export accepted telemetry with no deployable or system")
	}
}
