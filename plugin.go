package boltdb

import (
	"github.com/roadrunner-server/api/v4/plugins/v1/jobs"
	"github.com/roadrunner-server/api/v4/plugins/v1/kv"
	pq "github.com/roadrunner-server/api/v4/plugins/v1/priority_queue"
	"github.com/roadrunner-server/boltdb/v4/boltjobs"
	"github.com/roadrunner-server/boltdb/v4/boltkv"
	"github.com/roadrunner-server/errors"
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

// Plugin BoltDB K/V storage.
type Plugin struct {
	cfg Configurer
	// logger
	log *zap.Logger
}

func (p *Plugin) Init(log Logger, cfg Configurer) error {
	if !cfg.Has(PluginName) {
		return errors.E(errors.Disabled)
	}

	p.log = log.NamedLogger(PluginName)
	p.cfg = cfg
	return nil
}

// Name returns plugin name
func (p *Plugin) Name() string {
	return PluginName
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
func (p *Plugin) DriverFromConfig(configKey string, pq pq.Queue, pipeline jobs.Pipeline, cmder chan<- jobs.Commander) (jobs.Driver, error) {
	return boltjobs.FromConfig(configKey, p.log, p.cfg, pipeline, pq, cmder)
}

// DriverFromPipeline constructs kafka driver from pipeline
func (p *Plugin) DriverFromPipeline(pipe jobs.Pipeline, pq pq.Queue, cmder chan<- jobs.Commander) (jobs.Driver, error) {
	return boltjobs.FromPipeline(pipe, p.log, p.cfg, pq, cmder)
}
