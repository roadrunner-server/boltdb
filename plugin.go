package boltdb

import (
	"github.com/roadrunner-server/api/v3/plugins/v1/jobs"
	"github.com/roadrunner-server/api/v3/plugins/v1/kv"
	pq "github.com/roadrunner-server/api/v3/plugins/v1/priority_queue"
	"github.com/roadrunner-server/boltdb/v3/boltjobs"
	"github.com/roadrunner-server/boltdb/v3/boltkv"
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

func (p *Plugin) ConsumerFromConfig(configKey string, queue pq.Queue) (jobs.Consumer, error) {
	return boltjobs.NewBoltDBJobs(configKey, p.log, p.cfg, queue)
}

func (p *Plugin) ConsumerFromPipeline(pipe jobs.Pipeline, queue pq.Queue) (jobs.Consumer, error) {
	return boltjobs.FromPipeline(pipe, p.log, p.cfg, queue)
}
