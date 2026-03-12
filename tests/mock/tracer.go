package mocklogger

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type InMemoryTracer struct {
	Tp  *sdktrace.TracerProvider
	Exp *tracetest.InMemoryExporter
}

func NewInMemoryTracer(t *testing.T) *InMemoryTracer {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return &InMemoryTracer{Tp: tp, Exp: exp}
}

func (m *InMemoryTracer) Init() error                      { return nil }
func (m *InMemoryTracer) Name() string                     { return "inMemoryTracer" }
func (m *InMemoryTracer) Tracer() *sdktrace.TracerProvider { return m.Tp }
