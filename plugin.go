package boltdb

import (
	"github.com/roadrunner-server/boltdb/v3/boltjobs"
	"github.com/roadrunner-server/boltdb/v3/boltkv"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v3/plugins/jobs"
	"github.com/roadrunner-server/sdk/v3/plugins/jobs/pipeline"
	"github.com/roadrunner-server/sdk/v3/plugins/kv"
	priorityqueue "github.com/roadrunner-server/sdk/v3/priority_queue"
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

// Plugin BoltDB K/V storage.
type Plugin struct {
	cfg Configurer
	// logger
	log *zap.Logger
}

func (p *Plugin) Init(log *zap.Logger, cfg Configurer) error {
	p.log = new(zap.Logger)
	*p.log = *log
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

func (p *Plugin) ConsumerFromConfig(configKey string, queue priorityqueue.Queue) (jobs.Consumer, error) {
	return boltjobs.NewBoltDBJobs(configKey, p.log, p.cfg, queue)
}

func (p *Plugin) ConsumerFromPipeline(pipe *pipeline.Pipeline, queue priorityqueue.Queue) (jobs.Consumer, error) {
	return boltjobs.FromPipeline(pipe, p.log, p.cfg, queue)
}
