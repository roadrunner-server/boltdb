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

	"github.com/roadrunner-server/api/v4/plugins/v1/kv"
	"github.com/roadrunner-server/errors"
	bolt "go.etcd.io/bbolt"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

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
	// db instance
	DB *bolt.DB
	// name should be UTF-8
	bucket []byte
	log    *zap.Logger
	cfg    *Config
	tracer *sdktrace.TracerProvider

	// gc contains keys with timeouts
	gc sync.Map
	// default timeout for cache cleanup is 1 minute
	timeout time.Duration

	// stop is used to stop keys GC and close boltdb connection
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

	// add default values
	d.cfg.InitDefaults()

	d.bucket = []byte(d.cfg.bucket)
	d.timeout = time.Duration(d.cfg.Interval) * time.Second
	d.gc = sync.Map{}

	db, err := bolt.Open(d.cfg.File, os.FileMode(d.cfg.Permissions), &bolt.Options{ //nolint:gosec
		Timeout: time.Second * 20,
	})

	if err != nil {
		return nil, errors.E(op, err)
	}

	d.DB = db

	// create bucket if it does not exist
	// tx.Commit invokes via the db.Update
	err = db.Update(func(tx *bolt.Tx) error {
		const upOp = errors.Op("boltdb_plugin_update")
		_, err = tx.CreateBucketIfNotExists([]byte(d.cfg.bucket))
		if err != nil {
			return errors.E(op, upOp)
		}
		return nil
	})

	if err != nil {
		return nil, errors.E(op, err)
	}

	go d.startGCLoop()

	return d, nil
}

func (d *Driver) Has(keys ...string) (map[string]bool, error) {
	const op = errors.Op("boltdb_driver_has")
	_, span := d.tracer.Tracer(tracerName).Start(context.Background(), "boltdb:has")
	defer span.End()

	d.log.Debug("boltdb HAS method called", zap.Strings("args", keys))
	if keys == nil {
		span.RecordError(errors.E(op, errors.NoKeys))
		return nil, errors.E(op, errors.NoKeys)
	}

	m := make(map[string]bool, len(keys))

	// this is readable transaction
	err := d.DB.View(func(tx *bolt.Tx) error {
		// Get retrieves the value for a key in the bucket.
		// Returns a nil value if the key does not exist or if the key is a nested bucket.
		// The returned value is only valid for the life of the transaction.
		for i := range keys {
			keyTrimmed := strings.TrimSpace(keys[i])
			if keyTrimmed == "" {
				return errors.E(op, errors.EmptyKey)
			}
			b := tx.Bucket(d.bucket)
			if b == nil {
				return errors.E(op, errors.NoSuchBucket)
			}
			exist := b.Get([]byte(keys[i]))
			if exist != nil {
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

// Get retrieves the value for a key in the bucket.
// Returns a nil value if the key does not exist or if the key is a nested bucket.
// The returned value is only valid for the life of the transaction.
func (d *Driver) Get(key string) ([]byte, error) {
	const op = errors.Op("boltdb_driver_get")
	_, span := d.tracer.Tracer(tracerName).Start(context.Background(), "boltdb:get")
	defer span.End()
	// to get cases like "  "
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

		// try to decode values
		if val != nil {
			buf := bytes.NewReader(val)
			decoder := gob.NewDecoder(buf)

			var i string
			err := decoder.Decode(&i)
			if err != nil {
				// unsafe (w/o runes) convert
				return errors.E(op, err)
			}

			// set the value
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

func (d *Driver) MGet(keys ...string) (map[string][]byte, error) {
	const op = errors.Op("boltdb_driver_mget")
	_, span := d.tracer.Tracer(tracerName).Start(context.Background(), "boltdb:mget")
	defer span.End()
	// defense
	if keys == nil {
		span.RecordError(errors.E(op, errors.NoKeys))
		return nil, errors.E(op, errors.NoKeys)
	}

	// should not be empty keys
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
		var out []byte
		buf.Grow(100)
		for i := range keys {
			value := b.Get([]byte(keys[i]))
			buf.Write(value)
			// allocate enough space
			dec := gob.NewDecoder(buf)
			if value != nil {
				err := dec.Decode(&out)
				if err != nil {
					return errors.E(op, err)
				}
				m[keys[i]] = out
				buf.Reset()
				out = nil
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

// Set puts the K/V to the bolt
func (d *Driver) Set(items ...kv.Item) error {
	const op = errors.Op("boltdb_driver_set")
	_, span := d.tracer.Tracer(tracerName).Start(context.Background(), "boltdb:set")
	defer span.End()
	if items == nil {
		span.RecordError(errors.E(op, errors.NoKeys))
		return errors.E(op, errors.NoKeys)
	}

	// start writable transaction
	tx, err := d.DB.Begin(true)
	if err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}
	defer func() {
		err = tx.Commit()
		if err != nil {
			errRb := tx.Rollback()
			if errRb != nil {
				d.log.Error("during the commit, Rollback error occurred", zap.Error(err), zap.Error(errRb))
			}
		}
	}()

	b := tx.Bucket(d.bucket)
	// use access by index to avoid copying
	for i := range items {
		// performance note: pass a prepared bytes slice with initial cap
		// we can't move buf and gob out of loop, because we need to clear both from data
		// but gob will contain (w/o re-init) the past data
		buf := new(bytes.Buffer)
		encoder := gob.NewEncoder(buf)

		// Encode value
		err = encoder.Encode(items[i].Value())
		if err != nil {
			span.RecordError(err)
			return errors.E(op, err)
		}
		// buf.Bytes will copy the underlying slice. Take a look in case of performance problems
		err = b.Put([]byte(items[i].Key()), buf.Bytes())
		if err != nil {
			span.RecordError(err)
			return errors.E(op, err)
		}

		// if there are no errors, and TTL > 0,  we put the key with timeout to the hashmap, for future check
		// we do not need mutex here, since we use sync.Map
		if items[i].Timeout() != "" {
			// check correctness of provided TTL
			_, err := time.Parse(time.RFC3339, items[i].Timeout())
			if err != nil {
				span.RecordError(err)
				return errors.E(op, err)
			}
			// Store key TTL in the separate map
			d.gc.Store(items[i].Key(), items[i].Timeout())
		}

		buf.Reset()
	}

	return nil
}

// Delete all keys from DB
func (d *Driver) Delete(keys ...string) error {
	const op = errors.Op("boltdb_driver_delete")
	_, span := d.tracer.Tracer(tracerName).Start(context.Background(), "boltdb:delete")
	defer span.End()
	if keys == nil {
		span.RecordError(errors.E(op, errors.NoKeys))
		return errors.E(op, errors.NoKeys)
	}

	// should not be empty keys
	for _, key := range keys {
		keyTrimmed := strings.TrimSpace(key)
		if keyTrimmed == "" {
			span.RecordError(errors.E(op, errors.EmptyKey))
			return errors.E(op, errors.EmptyKey)
		}
	}

	// start writable transaction
	tx, err := d.DB.Begin(true)
	if err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}

	defer func() {
		err = tx.Commit()
		if err != nil {
			errRb := tx.Rollback()
			if errRb != nil {
				d.log.Error("during the commit, Rollback error occurred", zap.Error(err), zap.Error(errRb))
			}
		}
	}()

	b := tx.Bucket(d.bucket)
	if b == nil {
		span.RecordError(errors.E(op, errors.NoSuchBucket))
		return errors.E(op, errors.NoSuchBucket)
	}

	for _, key := range keys {
		err = b.Delete([]byte(key))
		if err != nil {
			span.RecordError(err)
			return errors.E(op, err)
		}
	}

	return nil
}

// MExpire sets the expiration time to the key
// If key already has the expiration time, it will be overwritten
func (d *Driver) MExpire(items ...kv.Item) error {
	const op = errors.Op("boltdb_driver_mexpire")
	_, span := d.tracer.Tracer(tracerName).Start(context.Background(), "boltdb:mexpire")
	defer span.End()

	for i := range items {
		if items[i].Timeout() == "" || strings.TrimSpace(items[i].Key()) == "" {
			span.RecordError(errors.E(op, errors.Str("should set timeout and at least one key")))
			return errors.E(op, errors.Str("should set timeout and at least one key"))
		}

		// verify provided TTL
		_, err := time.Parse(time.RFC3339, items[i].Timeout())
		if err != nil {
			span.RecordError(err)
			return errors.E(op, err)
		}

		d.gc.Store(items[i].Key(), items[i].Timeout())
	}
	return nil
}

func (d *Driver) TTL(keys ...string) (map[string]string, error) {
	const op = errors.Op("boltdb_driver_ttl")
	_, span := d.tracer.Tracer(tracerName).Start(context.Background(), "boltdb:ttl")
	defer span.End()

	if keys == nil {
		span.RecordError(errors.E(op, errors.NoKeys))
		return nil, errors.E(op, errors.NoKeys)
	}

	// should not be empty keys
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
			// a little bit dangerous operation, but user can't store value other that kv.Item.TTL --> int64
			m[keys[i]] = item.(string)
		}
	}
	return m, nil
}

func (d *Driver) Clear() error {
	_, span := d.tracer.Tracer(tracerName).Start(context.Background(), "boltdb:clear")
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

func (d *Driver) Stop() {
	d.stop <- struct{}{}
}

// ========================= PRIVATE =================================

func (d *Driver) startGCLoop() { //nolint:gocognit
	go func() {
		t := time.NewTicker(d.timeout)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				d.clearMu.RLock()

				// calculate current time before loop started to be fair
				now := time.Now().UTC()
				d.gc.Range(func(key, value any) bool {
					const op = errors.Op("boltdb_plugin_gc")
					k := key.(string)
					v, err := time.Parse(time.RFC3339, value.(string))
					if err != nil {
						return false
					}

					if now.After(v) {
						// time expired
						d.gc.Delete(k)
						d.log.Debug("key deleted", zap.String("key", k))
						err := d.DB.Update(func(tx *bolt.Tx) error {
							b := tx.Bucket(d.bucket)
							if b == nil {
								return errors.E(op, errors.NoSuchBucket)
							}
							err := b.Delete(strToBytes(k))
							if err != nil {
								return errors.E(op, err)
							}
							return nil
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
					d.log.Error("error")
				}
				return
			}
		}
	}()
}

func strToBytes(data string) []byte {
	if data == "" {
		return nil
	}

	return unsafe.Slice(unsafe.StringData(data), len(data))
}
