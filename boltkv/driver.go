package boltkv

import (
	"bytes"
	"context"
	"encoding/gob"
	"os"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/roadrunner-server/api-plugins/v6/kv"
	"github.com/roadrunner-server/errors"
	bolt "go.etcd.io/bbolt"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

var _ kv.Storage = (*Driver)(nil)

const (
	tracerName     string = "boltdb"
	RootPluginName string = "kv"
)

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if config section exists.
	Has(name string) bool
}

type Driver struct {
	clearMu sync.RWMutex
	DB      *bolt.DB
	bucket  []byte
	log     *zap.Logger
	cfg     *Config
	tracer  *sdktrace.TracerProvider

	gc      sync.Map
	timeout time.Duration

	stop chan struct{}
}

func NewBoltDBDriver(log *zap.Logger, key string, cfgPlugin Configurer, tracer *sdktrace.TracerProvider) (*Driver, error) {
	const op = errors.Op("new_boltdb_driver")

	if !cfgPlugin.Has(RootPluginName) {
		return nil, errors.E(op, errors.Str("no kv section in the configuration"))
	}

	d := &Driver{
		log:    log,
		stop:   make(chan struct{}, 1),
		tracer: tracer,
	}

	err := cfgPlugin.UnmarshalKey(key, &d.cfg)
	if err != nil {
		return nil, errors.E(op, err)
	}

	d.cfg.InitDefaults()

	d.bucket = []byte(d.cfg.bucket)
	d.timeout = time.Duration(d.cfg.Interval) * time.Second

	db, err := bolt.Open(d.cfg.File, os.FileMode(d.cfg.Permissions), &bolt.Options{ //nolint:gosec
		Timeout: time.Second * 20,
	})
	if err != nil {
		return nil, errors.E(op, err)
	}

	d.DB = db

	err = db.Update(func(tx *bolt.Tx) error {
		const upOp = errors.Op("boltdb_plugin_update")
		_, err = tx.CreateBucketIfNotExists([]byte(d.cfg.bucket))
		if err != nil {
			return errors.E(op, upOp, err)
		}
		return nil
	})
	if err != nil {
		return nil, errors.E(op, err)
	}

	go d.startGCLoop()

	return d, nil
}

func (d *Driver) Has(ctx context.Context, keys ...string) (map[string]bool, error) {
	const op = errors.Op("boltdb_driver_has")
	_, span := d.tracer.Tracer(tracerName).Start(ctx, "boltdb:has")
	defer span.End()

	d.log.Debug("boltdb HAS method called", zap.Strings("args", keys))
	if keys == nil {
		span.RecordError(errors.E(op, errors.NoKeys))
		return nil, errors.E(op, errors.NoKeys)
	}

	m := make(map[string]bool, len(keys))

	err := d.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(d.bucket)
		if b == nil {
			return errors.E(op, errors.NoSuchBucket)
		}

		for i := range keys {
			if strings.TrimSpace(keys[i]) == "" {
				return errors.E(op, errors.EmptyKey)
			}
			if b.Get([]byte(keys[i])) != nil {
				m[keys[i]] = true
			}
		}
		return nil
	})
	if err != nil {
		span.RecordError(err)
		return nil, errors.E(op, err)
	}

	d.log.Debug("boltdb HAS method finished")
	return m, nil
}

func (d *Driver) Get(ctx context.Context, key string) ([]byte, error) {
	const op = errors.Op("boltdb_driver_get")
	_, span := d.tracer.Tracer(tracerName).Start(ctx, "boltdb:get")
	defer span.End()

	keyTrimmed := strings.TrimSpace(key)
	if keyTrimmed == "" {
		span.RecordError(errors.E(op, errors.EmptyKey))
		return nil, errors.E(op, errors.EmptyKey)
	}

	var val []byte
	err := d.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(d.bucket)
		if b == nil {
			return errors.E(op, errors.NoSuchBucket)
		}
		val = b.Get([]byte(key))

		if val != nil {
			buf := bytes.NewReader(val)
			decoder := gob.NewDecoder(buf)

			var i string
			err := decoder.Decode(&i)
			if err != nil {
				return errors.E(op, err)
			}

			val = strToBytes(i)
		}
		return nil
	})
	if err != nil {
		span.RecordError(err)
		return nil, errors.E(op, err)
	}

	return val, nil
}

func (d *Driver) MGet(ctx context.Context, keys ...string) (map[string][]byte, error) {
	const op = errors.Op("boltdb_driver_mget")
	_, span := d.tracer.Tracer(tracerName).Start(ctx, "boltdb:mget")
	defer span.End()

	if keys == nil {
		span.RecordError(errors.E(op, errors.NoKeys))
		return nil, errors.E(op, errors.NoKeys)
	}

	for i := range keys {
		keyTrimmed := strings.TrimSpace(keys[i])
		if keyTrimmed == "" {
			span.RecordError(errors.E(op, errors.EmptyKey))
			return nil, errors.E(op, errors.EmptyKey)
		}
	}

	m := make(map[string][]byte, len(keys))

	err := d.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(d.bucket)
		if b == nil {
			return errors.E(op, errors.NoSuchBucket)
		}

		buf := new(bytes.Buffer)
		buf.Grow(100)
		for i := range keys {
			value := b.Get([]byte(keys[i]))
			if value != nil {
				buf.Reset()
				buf.Write(value)
				var out []byte
				err := gob.NewDecoder(buf).Decode(&out)
				if err != nil {
					return errors.E(op, err)
				}
				m[keys[i]] = out
			}
		}

		return nil
	})
	if err != nil {
		span.RecordError(err)
		return nil, errors.E(op, err)
	}

	return m, nil
}

func (d *Driver) Set(ctx context.Context, items ...kv.Item) error {
	const op = errors.Op("boltdb_driver_set")
	_, span := d.tracer.Tracer(tracerName).Start(ctx, "boltdb:set")
	defer span.End()
	if items == nil {
		span.RecordError(errors.E(op, errors.NoKeys))
		return errors.E(op, errors.NoKeys)
	}

	tx, err := d.DB.Begin(true)
	if err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}

	b := tx.Bucket(d.bucket)
	for i := range items {
		buf := new(bytes.Buffer)
		encoder := gob.NewEncoder(buf)

		err = encoder.Encode(items[i].Value())
		if err != nil {
			_ = tx.Rollback()
			span.RecordError(err)
			return errors.E(op, err)
		}
		err = b.Put([]byte(items[i].Key()), buf.Bytes())
		if err != nil {
			_ = tx.Rollback()
			span.RecordError(err)
			return errors.E(op, err)
		}

		if items[i].Timeout() != "" {
			_, err := time.Parse(time.RFC3339, items[i].Timeout())
			if err != nil {
				_ = tx.Rollback()
				span.RecordError(err)
				return errors.E(op, err)
			}
			d.gc.Store(items[i].Key(), items[i].Timeout())
		}
	}

	if err = tx.Commit(); err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}

	return nil
}

func (d *Driver) Delete(ctx context.Context, keys ...string) error {
	const op = errors.Op("boltdb_driver_delete")
	_, span := d.tracer.Tracer(tracerName).Start(ctx, "boltdb:delete")
	defer span.End()
	if keys == nil {
		span.RecordError(errors.E(op, errors.NoKeys))
		return errors.E(op, errors.NoKeys)
	}

	for _, key := range keys {
		keyTrimmed := strings.TrimSpace(key)
		if keyTrimmed == "" {
			span.RecordError(errors.E(op, errors.EmptyKey))
			return errors.E(op, errors.EmptyKey)
		}
	}

	tx, err := d.DB.Begin(true)
	if err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}

	b := tx.Bucket(d.bucket)
	if b == nil {
		_ = tx.Rollback()
		span.RecordError(errors.E(op, errors.NoSuchBucket))
		return errors.E(op, errors.NoSuchBucket)
	}

	for _, key := range keys {
		err = b.Delete([]byte(key))
		if err != nil {
			_ = tx.Rollback()
			span.RecordError(err)
			return errors.E(op, err)
		}
	}

	if err = tx.Commit(); err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}

	return nil
}

func (d *Driver) MExpire(ctx context.Context, items ...kv.Item) error {
	const op = errors.Op("boltdb_driver_mexpire")
	_, span := d.tracer.Tracer(tracerName).Start(ctx, "boltdb:mexpire")
	defer span.End()

	for i := range items {
		if items[i].Timeout() == "" || strings.TrimSpace(items[i].Key()) == "" {
			span.RecordError(errors.E(op, errors.Str("should set timeout and at least one key")))
			return errors.E(op, errors.Str("should set timeout and at least one key"))
		}

		_, err := time.Parse(time.RFC3339, items[i].Timeout())
		if err != nil {
			span.RecordError(err)
			return errors.E(op, err)
		}

		d.gc.Store(items[i].Key(), items[i].Timeout())
	}
	return nil
}

func (d *Driver) TTL(ctx context.Context, keys ...string) (map[string]string, error) {
	const op = errors.Op("boltdb_driver_ttl")
	_, span := d.tracer.Tracer(tracerName).Start(ctx, "boltdb:ttl")
	defer span.End()

	if keys == nil {
		span.RecordError(errors.E(op, errors.NoKeys))
		return nil, errors.E(op, errors.NoKeys)
	}

	for i := range keys {
		keyTrimmed := strings.TrimSpace(keys[i])
		if keyTrimmed == "" {
			span.RecordError(errors.E(op, errors.EmptyKey))
			return nil, errors.E(op, errors.EmptyKey)
		}
	}

	m := make(map[string]string, len(keys))

	for i := range keys {
		if item, ok := d.gc.Load(keys[i]); ok {
			m[keys[i]] = item.(string)
		}
	}
	return m, nil
}

func (d *Driver) Clear(ctx context.Context) error {
	_, span := d.tracer.Tracer(tracerName).Start(ctx, "boltdb:clear")
	defer span.End()

	err := d.DB.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket(d.bucket)
		if err != nil {
			d.log.Error("boltdb delete bucket", zap.Error(err))
			return err
		}

		_, err = tx.CreateBucket(d.bucket)
		if err != nil {
			d.log.Error("boltdb create bucket", zap.Error(err))
			return err
		}

		return nil
	})

	if err != nil {
		span.RecordError(err)
		d.log.Error("clear transaction failed", zap.Error(err))
		return err
	}

	d.clearMu.Lock()
	d.gc = sync.Map{}
	d.clearMu.Unlock()

	return nil
}

func (d *Driver) Stop(_ context.Context) {
	d.stop <- struct{}{}
}

// ========================= PRIVATE =================================

func (d *Driver) startGCLoop() {
	t := time.NewTicker(d.timeout)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			d.clearMu.RLock()

			now := time.Now().UTC()
			d.gc.Range(func(key, value any) bool {
				const op = errors.Op("boltdb_plugin_gc")
				k := key.(string)
				v, err := time.Parse(time.RFC3339, value.(string))
				if err != nil {
					d.log.Error("failed to parse TTL, removing entry", zap.String("key", k), zap.Error(err))
					d.gc.Delete(k)
					return true
				}

				if now.After(v) {
					d.gc.Delete(k)
					d.log.Debug("key deleted", zap.String("key", k))
					err := d.DB.Update(func(tx *bolt.Tx) error {
						b := tx.Bucket(d.bucket)
						if b == nil {
							return errors.E(op, errors.NoSuchBucket)
						}
						return b.Delete(strToBytes(k))
					})
					if err != nil {
						d.log.Error("error during the gc phase of update", zap.Error(err))
						return false
					}
				}
				return true
			})

			d.clearMu.RUnlock()
		case <-d.stop:
			err := d.DB.Close()
			if err != nil {
				d.log.Error("error closing boltdb", zap.Error(err))
			}
			return
		}
	}
}

func strToBytes(data string) []byte {
	if data == "" {
		return nil
	}

	return unsafe.Slice(unsafe.StringData(data), len(data))
}
