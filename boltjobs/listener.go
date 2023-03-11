package boltjobs

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync/atomic"
	"time"

	"github.com/roadrunner-server/sdk/v4/utils"
	bolt "go.etcd.io/bbolt"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

func (d *Driver) listener() { //nolint:gocognit
	tt := time.NewTicker(time.Millisecond * 500)
	defer tt.Stop()
	for {
		select {
		case <-d.stopCh:
			d.log.Debug("boltdb listener stopped")
			return
		case <-tt.C:
			if atomic.LoadUint64(d.active) > uint64(d.prefetch) {
				time.Sleep(time.Second)
				continue
			}
			tx, err := d.db.Begin(true)
			if err != nil {
				d.log.Error("failed to begin writable transaction", zap.Error(err))
				continue
			}

			b := tx.Bucket(utils.AsBytes(PushBucket))
			inQb := tx.Bucket(utils.AsBytes(InQueueBucket))

			// get first item
			k, v := b.Cursor().First()
			if k == nil && v == nil {
				_ = tx.Commit()
				continue
			}

			buf := bytes.NewReader(v)
			dec := gob.NewDecoder(buf)

			item := &Item{}
			err = dec.Decode(item)
			if err != nil {
				d.rollback(err, tx)
				continue
			}

			ctx := d.prop.Extract(context.Background(), propagation.HeaderCarrier(item.Headers))
			ctx, span := d.tracer.Tracer(tracerName).Start(ctx, "boltdb_listener")

			if item.Options.Priority == 0 {
				item.Options.Priority = d.priority
			}

			// used only for the debug purposes
			if item.Options.AutoAck {
				d.log.Debug("auto ack is turned on, message acknowledged")
			}

			// If AutoAck is false, put the job into the safe DB
			if !item.Options.AutoAck {
				err = inQb.Put(utils.AsBytes(item.ID()), v)
				if err != nil {
					d.rollback(err, tx)
					span.SetAttributes(attribute.KeyValue{
						Key:   "error",
						Value: attribute.StringValue(err.Error()),
					})
					span.End()
					continue
				}
			}

			// delete key from the PushBucket
			err = b.Delete(k)
			if err != nil {
				d.rollback(err, tx)
				span.SetAttributes(attribute.KeyValue{
					Key:   "error",
					Value: attribute.StringValue(err.Error()),
				})
				span.End()
				continue
			}

			err = tx.Commit()
			if err != nil {
				d.rollback(err, tx)
				span.SetAttributes(attribute.KeyValue{
					Key:   "error",
					Value: attribute.StringValue(err.Error()),
				})
				span.End()
				continue
			}

			d.prop.Inject(ctx, propagation.HeaderCarrier(item.Headers))
			// attach pointer to the DB
			item.attachDB(d.db, d.active, d.delayed)
			// as the last step, after commit, put the item into the PQ
			d.pq.Insert(item)
			span.End()
		}
	}
}

func (d *Driver) delayedJobsListener() { //nolint:gocognit
	tt := time.NewTicker(time.Second)
	defer tt.Stop()

	// just some 90's
	loc, err := time.LoadLocation("UTC")
	if err != nil {
		d.log.Error("failed to load location, delayed jobs won't work", zap.Error(err))
		return
	}

	var startDate = utils.AsBytes(time.Date(1990, 1, 1, 0, 0, 0, 0, loc).Format(time.RFC3339))

	for {
		select {
		case <-d.stopCh:
			d.log.Debug("boltdb listener stopped")
			return
		case <-tt.C:
			tx, err := d.db.Begin(true)
			if err != nil {
				d.log.Error("failed to begin writable transaction, job will be read on the next attempt", zap.Error(err))
				continue
			}

			delayB := tx.Bucket(utils.AsBytes(DelayBucket))
			inQb := tx.Bucket(utils.AsBytes(InQueueBucket))

			cursor := delayB.Cursor()
			endDate := utils.AsBytes(time.Now().UTC().Format(time.RFC3339))

			for k, v := cursor.Seek(startDate); k != nil && bytes.Compare(k, endDate) <= 0; k, v = cursor.Next() {
				buf := bytes.NewReader(v)
				dec := gob.NewDecoder(buf)

				item := &Item{}
				err = dec.Decode(item)
				if err != nil {
					d.rollback(err, tx)
					continue
				}

				if item.Options.Priority == 0 {
					item.Options.Priority = d.priority
				}

				// used only for the debug purposes
				if item.Options.AutoAck {
					d.log.Debug("auto ack is turned on, message acknowledged")
				}

				// If AutoAck is false, put the job into the safe DB
				if !item.Options.AutoAck {
					err = inQb.Put(utils.AsBytes(item.ID()), v)
					if err != nil {
						d.rollback(err, tx)
						continue
					}
				}

				// delete key from the PushBucket
				err = delayB.Delete(k)
				if err != nil {
					d.rollback(err, tx)
					continue
				}

				// attach pointer to the DB
				item.attachDB(d.db, d.active, d.delayed)
				// as the last step, after commit, put the item into the PQ
				d.pq.Insert(item)
			}

			err = tx.Commit()
			if err != nil {
				d.rollback(err, tx)
				continue
			}
		}
	}
}

func (d *Driver) rollback(err error, tx *bolt.Tx) {
	errR := tx.Rollback()
	if errR != nil {
		d.log.Error("transaction commit error, rollback failed", zap.Error(err), zap.Error(errR))
		return
	}

	d.log.Error("transaction commit error, rollback succeed", zap.Error(err))
}
