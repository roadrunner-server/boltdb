package boltdb

import (
	"context"

	_ "google.golang.org/genproto/protobuf/ptype" //nolint:revive,nolintlint

	"github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/api-plugins/v6/kv"
	"github.com/roadrunner-server/boltdb/v6/boltjobs"
	"github.com/roadrunner-server/boltdb/v6/boltkv"
	"github.com/roadrunner-server/endure/v2/dep"
	"github.com/roadrunner-server/errors"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

var _ jobs.Constructor = (*Plugin)(nil)

const pluginName string = "boltdb"

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if a config section exists.
	Has(name string) bool
}

type Logger interface {
	NamedLogger(name string) *zap.Logger
}

type Tracer interface {
	Tracer() *sdktrace.TracerProvider
}

type Plugin struct {
	log    *zap.Logger
	cfg    Configurer
	tracer *sdktrace.TracerProvider
}

func (p *Plugin) Init(log Logger, cfg Configurer) error {
	p.log = log.NamedLogger(pluginName)
	p.cfg = cfg
	return nil
}

func (p *Plugin) Name() string {
	return pluginName
}

func (p *Plugin) Collects() []*dep.In {
	return []*dep.In{
		dep.Fits(func(pp any) {
			p.tracer = pp.(Tracer).Tracer()
		}, (*Tracer)(nil)),
	}
}

func (p *Plugin) KvFromConfig(_ context.Context, key string) (kv.Storage, error) {
	const op = errors.Op("boltdb_plugin_provide")
	st, err := boltkv.NewBoltDBDriver(p.log, key, p.cfg, p.tracer)
	if err != nil {
		return nil, errors.E(op, err)
	}
	return st, nil
}

// DriverFromConfig constructs boltdb driver from the .rr.yaml configuration
func (p *Plugin) DriverFromConfig(ctx context.Context, configKey string, pq jobs.Queue, pipeline jobs.Pipeline) (jobs.Driver, error) {
	return boltjobs.FromConfig(ctx, p.tracer, configKey, p.log, p.cfg, pipeline, pq)
}

// DriverFromPipeline constructs boltdb driver from pipeline
func (p *Plugin) DriverFromPipeline(ctx context.Context, pipe jobs.Pipeline, pq jobs.Queue) (jobs.Driver, error) {
	return boltjobs.FromPipeline(ctx, p.tracer, pipe, p.log, p.cfg, pq)
}
