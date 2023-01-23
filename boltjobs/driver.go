package boltjobs

import (
	"bytes"
	"context"
	"encoding/gob"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/roadrunner-server/api/v4/plugins/v1/jobs"
	pq "github.com/roadrunner-server/api/v4/plugins/v1/priority_queue"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v4/utils"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

var _ jobs.Driver = (*Driver)(nil)

const (
	name string = "boltdb"
	rrDB string = "rr.db"

	PushBucket    string = "push"
	InQueueBucket string = "processing"
	DelayBucket   string = "delayed"
)

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if config section exists.
	Has(name string) bool
}

type Driver struct {
	file        string
	permissions int64
	priority    int64
	prefetch    int

	db *bolt.DB

	bPool    sync.Pool
	log      *zap.Logger
	pq       pq.Queue
	pipeline atomic.Pointer[jobs.Pipeline]
	cond     *sync.Cond

	listeners uint32
	active    *uint64
	delayed   *uint64

	stopCh chan struct{}
}

func FromConfig(configKey string, log *zap.Logger, cfg Configurer, pipe jobs.Pipeline, pq pq.Queue, _ chan<- jobs.Commander) (*Driver, error) {
	const op = errors.Op("init_boltdb_jobs")

	var localCfg config
	err := cfg.UnmarshalKey(configKey, &localCfg)
	if err != nil {
		return nil, errors.E(op, err)
	}

	localCfg.InitDefaults()
	db, err := bolt.Open(localCfg.File, os.FileMode(localCfg.Permissions), &bolt.Options{
		Timeout: time.Second * 20,
	})

	if err != nil {
		return nil, errors.E(op, err)
	}

	// create bucket if it does not exist
	// tx.Commit invokes via the db.Update
	err = create(db)
	if err != nil {
		return nil, errors.E(op, err)
	}

	dr := &Driver{
		permissions: int64(localCfg.Permissions),
		file:        localCfg.File,
		priority:    localCfg.Priority,
		prefetch:    localCfg.Prefetch,

		bPool: sync.Pool{
			New: func() any {
				return new(bytes.Buffer)
			},
		},
		cond: sync.NewCond(&sync.Mutex{}),

		delayed: utils.Uint64(0),
		active:  utils.Uint64(0),

		db:     db,
		log:    log,
		pq:     pq,
		stopCh: make(chan struct{}),
	}

	dr.pipeline.Store(&pipe)

	return dr, nil
}

func FromPipeline(pipeline jobs.Pipeline, log *zap.Logger, cfg Configurer, pq pq.Queue, _ chan<- jobs.Commander) (*Driver, error) {
	const op = errors.Op("init_boltdb_jobs")

	var conf config
	err := cfg.UnmarshalKey(name, conf)
	if err != nil {
		return nil, errors.E(op, err)
	}

	var perm int64
	perm, err = strconv.ParseInt(pipeline.String(permissions, "0777"), 8, 64)
	if err != nil {
		log.Warn("failed to parse permissions, fallback to default 0777", zap.String("provided", pipeline.String(permissions, "")))
		perm = 511 // 0777
	}

	// add default values
	conf.InitDefaults()

	db, err := bolt.Open(pipeline.String(file, rrDB), os.FileMode(perm), &bolt.Options{
		Timeout: time.Second * 20,
	})

	if err != nil {
		return nil, errors.E(op, err)
	}

	// create bucket if it does not exist
	// tx.Commit invokes via the db.Update
	err = create(db)
	if err != nil {
		return nil, errors.E(op, err)
	}

	dr := &Driver{
		file:        pipeline.String(file, rrDB),
		priority:    pipeline.Priority(),
		prefetch:    pipeline.Int(prefetch, 1000),
		permissions: perm,

		bPool: sync.Pool{New: func() any {
			return new(bytes.Buffer)
		}},
		cond: sync.NewCond(&sync.Mutex{}),

		delayed: utils.Uint64(0),
		active:  utils.Uint64(0),

		db:     db,
		log:    log,
		pq:     pq,
		stopCh: make(chan struct{}),
	}

	dr.pipeline.Store(&pipeline)

	return dr, nil
}

func (d *Driver) Push(_ context.Context, job jobs.Job) error {
	const op = errors.Op("boltdb_jobs_push")
	err := d.db.Update(func(tx *bolt.Tx) error {
		item := fromJob(job)
		// pool with buffers
		buf := d.get()
		// encode the job
		enc := gob.NewEncoder(buf)
		err := enc.Encode(item)
		if err != nil {
			d.put(buf)
			return errors.E(op, err)
		}

		value := make([]byte, buf.Len())
		copy(value, buf.Bytes())
		d.put(buf)

		// handle delay
		if item.Options.Delay > 0 {
			b := tx.Bucket(utils.AsBytes(DelayBucket))
			tKey := time.Now().UTC().Add(time.Second * time.Duration(item.Options.Delay)).Format(time.RFC3339)

			err = b.Put(utils.AsBytes(tKey), value)
			if err != nil {
				return errors.E(op, err)
			}

			atomic.AddUint64(d.delayed, 1)

			return nil
		}

		b := tx.Bucket(utils.AsBytes(PushBucket))
		err = b.Put(utils.AsBytes(item.ID()), value)
		if err != nil {
			return errors.E(op, err)
		}

		// increment active counter
		atomic.AddUint64(d.active, 1)

		return nil
	})

	if err != nil {
		return errors.E(op, err)
	}

	return nil
}

func (d *Driver) Run(_ context.Context, p jobs.Pipeline) error {
	const op = errors.Op("boltdb_run")
	start := time.Now()

	pipe := *d.pipeline.Load()
	if pipe.Name() != p.Name() {
		return errors.E(op, errors.Errorf("no such pipeline registered: %s", pipe.Name()))
	}

	// run listener
	go d.listener()
	go d.delayedJobsListener()

	// increase number of listeners
	atomic.AddUint32(&d.listeners, 1)
	d.log.Debug("pipeline is active", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))
	return nil
}

func (d *Driver) Stop(_ context.Context) error {
	start := time.Now()
	if atomic.LoadUint32(&d.listeners) > 0 {
		d.stopCh <- struct{}{}
		d.stopCh <- struct{}{}
	}

	pipe := *d.pipeline.Load()
	d.log.Debug("pipeline was stopped", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))
	return d.db.Close()
}

func (d *Driver) Pause(_ context.Context, p string) error {
	start := time.Now()
	pipe := *d.pipeline.Load()
	if pipe.Name() != p {
		return errors.Errorf("no such pipeline: %s", pipe.Name())
	}

	l := atomic.LoadUint32(&d.listeners)
	// no active listeners
	if l == 0 {
		return errors.Str("no active listeners, nothing to pause")
	}

	d.stopCh <- struct{}{}
	d.stopCh <- struct{}{}

	atomic.AddUint32(&d.listeners, ^uint32(0))

	d.log.Debug("pipeline was paused", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))

	return nil
}

func (d *Driver) Resume(_ context.Context, p string) error {
	start := time.Now()
	pipe := *d.pipeline.Load()
	if pipe.Name() != p {
		return errors.Errorf("no such pipeline: %s", pipe.Name())
	}

	l := atomic.LoadUint32(&d.listeners)
	// no active listeners
	if l == 1 {
		return errors.Str("boltdb listener is already in the active state")
	}

	// run listener
	go d.listener()
	go d.delayedJobsListener()

	// increase number of listeners
	atomic.AddUint32(&d.listeners, 1)

	d.log.Debug("pipeline was resumed", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))

	return nil
}

func (d *Driver) State(_ context.Context) (*jobs.State, error) {
	pipe := *d.pipeline.Load()

	return &jobs.State{
		Pipeline: pipe.Name(),
		Driver:   pipe.Driver(),
		Queue:    PushBucket,
		Priority: uint64(pipe.Priority()),
		Active:   int64(atomic.LoadUint64(d.active)),
		Delayed:  int64(atomic.LoadUint64(d.delayed)),
		Ready:    toBool(atomic.LoadUint32(&d.listeners)),
	}, nil
}

// Private methods

func create(db *bolt.DB) error {
	err := db.Update(func(tx *bolt.Tx) error {
		const upOp = errors.Op("boltdb_plugin_update")
		_, err := tx.CreateBucketIfNotExists(utils.AsBytes(DelayBucket))
		if err != nil {
			return errors.E(upOp, err)
		}

		_, err = tx.CreateBucketIfNotExists(utils.AsBytes(PushBucket))
		if err != nil {
			return errors.E(upOp, err)
		}

		_, err = tx.CreateBucketIfNotExists(utils.AsBytes(InQueueBucket))
		if err != nil {
			return errors.E(upOp, err)
		}

		inQb := tx.Bucket(utils.AsBytes(InQueueBucket))
		cursor := inQb.Cursor()

		pushB := tx.Bucket(utils.AsBytes(PushBucket))

		// get all items, which are in the InQueueBucket and put them into the PushBucket
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			err = pushB.Put(k, v)
			if err != nil {
				return errors.E(upOp, err)
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (d *Driver) get() *bytes.Buffer {
	return d.bPool.Get().(*bytes.Buffer)
}

func (d *Driver) put(b *bytes.Buffer) {
	b.Reset()
	d.bPool.Put(b)
}

func toBool(r uint32) bool {
	return r > 0
}
