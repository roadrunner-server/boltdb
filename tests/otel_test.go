package boltdb

import (
	"slices"
	"testing"

	"tests/helpers"
	mocklogger "tests/mock"

	"github.com/stretchr/testify/require"
)

// TestOtelSpansCoverTheJobLifecycle pushes one job and checks the driver opens
// a span for each stage, from the listener through the push to the shutdown.
func TestOtelSpansCoverTheJobLifecycle(t *testing.T) {
	removeDBs(t)

	tracer := mocklogger.NewInMemoryTracer(t)

	rr, stop := helpers.Start(t, "configs/.rr-boltdb-otel.yaml",
		append(jobsPlugins(), tracer),
		helpers.WithObservedLogger(),
		helpers.WithTCPProbe(defaultRPC),
	)

	helpers.PushToPipe("test-1", false, defaultRPC)(t)

	rr.WaitLog(t, "job was pushed successfully", 1)
	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.DestroyPipelines(defaultRPC, "test-1")(t)

	// the stop spans are only emitted while the container shuts down
	stop()

	want := []string{
		"boltdb_listener",
		"boltdb_push",
		"boltdb_stop",
		"destroy_pipeline",
		"jobs_listener",
		"push",
	}

	require.Equal(t, want, uniqueSpanNames(tracer))
}

// uniqueSpanNames returns the sorted, deduplicated names of the collected spans.
func uniqueSpanNames(tracer *mocklogger.InMemoryTracer) []string {
	seen := make(map[string]struct{})
	for _, s := range tracer.Exp.GetSpans() {
		seen[s.Name] = struct{}{}
	}

	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	slices.Sort(names)

	return names
}
