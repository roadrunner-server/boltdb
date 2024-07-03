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

	"github.com/roadrunner-server/api/v4/plugins/v4/jobs"
	"github.com/roadrunner-server/errors"
	bolt "go.etcd.io/bbolt"
	jprop "go.opentelemetry.io/contrib/propagators/jaeger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

var _ jobs.Driver = (*Driver)(nil)

const (
	pluginName string = "boltdb"
	rrDB       string = "rr.db"

	PushBucket    string = "push"
	InQueueBucket string = "processing"
	DelayBucket   string = "delayed"

	tracerName string = "jobs"
)

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if a config section exists.
	Has(name string) bool
}

type Driver struct {
	file        string
	permissions int64
	priority    int64
	prefetch    int
	tracer      *sdktrace.TracerProvider
	prop        propagation.TextMapPropagator

	db *bolt.DB

	bPool    sync.Pool
	log      *zap.Logger
	pq       jobs.Queue
	pipeline atomic.Pointer[jobs.Pipeline]
	cond     *sync.Cond

	listeners uint32
	active    *uint64
	delayed   *uint64

	stopCh chan struct{}
}

func FromConfig(tracer *sdktrace.TracerProvider, configKey string, log *zap.Logger, cfg Configurer, pipe jobs.Pipeline, pq jobs.Queue) (*Driver, error) {
	const op = errors.Op("init_boltdb_jobs")

	var localCfg config
	err := cfg.UnmarshalKey(configKey, &localCfg)
	if err != nil {
		return nil, errors.E(op, err)
	}

	if tracer == nil {
		tracer = sdktrace.NewTracerProvider()
	}

	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}, jprop.Jaeger{})
	otel.SetTextMapPropagator(prop)

	localCfg.InitDefaults()
	db, err := bolt.Open(localCfg.File, os.FileMode(localCfg.Permissions), &bolt.Options{
		Timeout: time.Second * 20,
	})

	if err != nil {
		return nil, errors.E(op, err)
	}

	// create a bucket if it does not exist
	// tx.Commit invokes via the db.Update
	err = create(db)
	if err != nil {
		return nil, errors.E(op, err)
	}

	dr := &Driver{
		tracer:      tracer,
		prop:        prop,
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

		delayed: toPtr(uint64(0)),
		active:  toPtr(uint64(0)),

		db:     db,
		log:    log,
		pq:     pq,
		stopCh: make(chan struct{}),
	}

	dr.pipeline.Store(&pipe)

	return dr, nil
}

func FromPipeline(tracer *sdktrace.TracerProvider, pipeline jobs.Pipeline, log *zap.Logger, cfg Configurer, pq jobs.Queue) (*Driver, error) {
	const op = errors.Op("init_boltdb_jobs")

	var conf config
	err := cfg.UnmarshalKey(pluginName, conf)
	if err != nil {
		return nil, errors.E(op, err)
	}

	if tracer == nil {
		tracer = sdktrace.NewTracerProvider()
	}

	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}, jprop.Jaeger{})
	otel.SetTextMapPropagator(prop)

	var perm int64
	perm, err = strconv.ParseInt(pipeline.String(permissions, "0755"), 8, 32)
	if err != nil {
		log.Warn("failed to parse permissions, fallback to default 0755", zap.String("provided", pipeline.String(permissions, "")))
		perm = 493 // 0755
	}

	// add default values
	conf.InitDefaults()

	db, err := bolt.Open(pipeline.String(file, rrDB), os.FileMode(perm), &bolt.Options{
		Timeout: time.Second * 20,
	})

	if err != nil {
		return nil, errors.E(op, err)
	}

	// create a bucket if it does not exist
	// tx.Commit invokes via the db.Update
	err = create(db)
	if err != nil {
		return nil, errors.E(op, err)
	}

	dr := &Driver{
		tracer:      tracer,
		prop:        prop,
		file:        pipeline.String(file, rrDB),
		priority:    pipeline.Priority(),
		prefetch:    pipeline.Int(prefetch, 1000),
		permissions: perm,

		bPool: sync.Pool{New: func() any {
			return new(bytes.Buffer)
		}},
		cond: sync.NewCond(&sync.Mutex{}),

		delayed: toPtr(uint64(0)),
		active:  toPtr(uint64(0)),

		db:     db,
		log:    log,
		pq:     pq,
		stopCh: make(chan struct{}),
	}

	dr.pipeline.Store(&pipeline)

	return dr, nil
}

func (d *Driver) Push(ctx context.Context, job jobs.Message) error {
	const op = errors.Op("boltdb_jobs_push")

	ctx, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "boltdb_push")
	defer span.End()

	err := d.db.Update(func(tx *bolt.Tx) error {
		item := fromJob(job)
		d.prop.Inject(ctx, propagation.HeaderCarrier(item.headers))
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
			b := tx.Bucket(strToBytes(DelayBucket))
			tKey := time.Now().UTC().Add(time.Second * time.Duration(item.Options.Delay)).Format(time.RFC3339)

			err = b.Put(strToBytes(tKey), value)
			if err != nil {
				return errors.E(op, err)
			}

			atomic.AddUint64(d.delayed, 1)

			return nil
		}

		b := tx.Bucket(strToBytes(PushBucket))
		err = b.Put(strToBytes(item.ID()), value)
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

func (d *Driver) Run(ctx context.Context, p jobs.Pipeline) error {
	const op = errors.Op("boltdb_run")
	start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "boltdb_run")
	defer span.End()

	pipe := *d.pipeline.Load()
	if pipe.Name() != p.Name() {
		return errors.E(op, errors.Errorf("no such pipeline registered: %s", pipe.Name()))
	}

	// run listener
	go d.listener()
	go d.delayedJobsListener()

	// increase number of listeners
	atomic.AddUint32(&d.listeners, 1)
	d.log.Debug("pipeline was started", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Int64("elapsed", time.Since(start).Milliseconds()))
	return nil
}

func (d *Driver) Stop(ctx context.Context) error {
	start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "boltdb_stop")
	defer span.End()

	if atomic.LoadUint32(&d.listeners) > 0 {
		d.stopCh <- struct{}{}
		d.stopCh <- struct{}{}
	}

	pipe := *d.pipeline.Load()

	// remove all pending JOBS associated with the pipeline
	_ = d.pq.Remove(pipe.Name())

	d.log.Debug("pipeline was stopped", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Int64("elapsed", time.Since(start).Milliseconds()))
	return d.db.Close()
}

func (d *Driver) Pause(ctx context.Context, p string) error {
	start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "boltdb_pause")
	defer span.End()

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

	d.log.Debug("pipeline was paused", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Int64("elapsed", time.Since(start).Milliseconds()))

	return nil
}

func (d *Driver) Resume(ctx context.Context, p string) error {
	start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "boltdb_resume")
	defer span.End()

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

	d.log.Debug("pipeline was resumed", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Int64("elapsed", time.Since(start).Milliseconds()))

	return nil
}

func (d *Driver) State(ctx context.Context) (*jobs.State, error) {
	pipe := *d.pipeline.Load()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "boltdb_state")
	defer span.End()

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
		_, err := tx.CreateBucketIfNotExists(strToBytes(DelayBucket))
		if err != nil {
			return errors.E(upOp, err)
		}

		_, err = tx.CreateBucketIfNotExists(strToBytes(PushBucket))
		if err != nil {
			return errors.E(upOp, err)
		}

		_, err = tx.CreateBucketIfNotExists(strToBytes(InQueueBucket))
		if err != nil {
			return errors.E(upOp, err)
		}

		inQb := tx.Bucket(strToBytes(InQueueBucket))
		cursor := inQb.Cursor()

		pushB := tx.Bucket(strToBytes(PushBucket))

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

func toPtr[T any](v T) *T {
	return &v
}
