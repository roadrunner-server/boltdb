package boltdb

import (
	"github.com/roadrunner-server/api/v4/plugins/v1/kv"
	"github.com/roadrunner-server/api/v4/plugins/v2/jobs"
	"github.com/roadrunner-server/boltdb/v4/boltjobs"
	"github.com/roadrunner-server/boltdb/v4/boltkv"
	"github.com/roadrunner-server/endure/v2/dep"
	"github.com/roadrunner-server/errors"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

const (
	PluginName string = "boltdb"
)

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if config section exists.
	Has(name string) bool
}

type Logger interface {
	NamedLogger(name string) *zap.Logger
}

type Tracer interface {
	Tracer() *sdktrace.TracerProvider
}

// Plugin BoltDB K/V storage.
type Plugin struct {
	cfg    Configurer
	log    *zap.Logger
	tracer *sdktrace.TracerProvider
}

func (p *Plugin) Init(log Logger, cfg Configurer) error {
	p.log = log.NamedLogger(PluginName)
	p.cfg = cfg
	return nil
}

// Name returns plugin name
func (p *Plugin) Name() string {
	return PluginName
}

func (p *Plugin) Collects() []*dep.In {
	return []*dep.In{
		dep.Fits(func(pp any) {
			p.tracer = pp.(Tracer).Tracer()
		}, (*Tracer)(nil)),
	}
}

// KV bolt implementation

func (p *Plugin) KvFromConfig(key string) (kv.Storage, error) {
	const op = errors.Op("boltdb_plugin_provide")
	st, err := boltkv.NewBoltDBDriver(p.log, key, p.cfg)
	if err != nil {
		return nil, errors.E(op, err)
	}
	return st, nil
}

// JOBS bbolt implementation

// DriverFromConfig constructs kafka driver from the .rr.yaml configuration
func (p *Plugin) DriverFromConfig(configKey string, pq jobs.Queue, pipeline jobs.Pipeline, _ chan<- jobs.Commander) (jobs.Driver, error) {
	return boltjobs.FromConfig(p.tracer, configKey, p.log, p.cfg, pipeline, pq)
}

// DriverFromPipeline constructs kafka driver from pipeline
func (p *Plugin) DriverFromPipeline(pipe jobs.Pipeline, pq jobs.Queue, _ chan<- jobs.Commander) (jobs.Driver, error) {
	return boltjobs.FromPipeline(p.tracer, pipe, p.log, p.cfg, pq)
}
