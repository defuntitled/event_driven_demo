package messaging

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/defuntitled/event_driven_demo/internal/config"
	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

type Sender func(context.Context, contracts.Record) (int32, int64, error)
type pending struct {
	id       int64
	attempts int
	due      int64
	record   contracts.Record
}

func takePending(ctx context.Context, db *sql.DB, run string, gen int64) (pending, error) {
	var p pending
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return p, err
	}
	defer tx.Rollback()
	if err = storage.CheckRun(ctx, tx, run, gen, false); err != nil {
		return p, storage.ErrStopped
	}
	var desired string
	if err = tx.QueryRowContext(ctx, "SELECT desired_state FROM worker_control WHERE worker='publisher' AND run_id=? AND generation=?", run, gen).Scan(&desired); err != nil {
		return p, err
	}
	if desired != "RUNNING" {
		return p, storage.ErrStopped
	}
	err = tx.QueryRowContext(ctx, "SELECT row_id,topic,record_key,envelope_json,attempts,next_retry_at_ms FROM outbox WHERE run_id=? AND status='PENDING' ORDER BY row_id LIMIT 1", run).Scan(&p.id, &p.record.Topic, &p.record.Key, &p.record.Value, &p.attempts, &p.due)
	if err != nil {
		return p, err
	}
	if p.due > time.Now().UnixMilli() {
		return p, sql.ErrNoRows
	}
	if _, err = tx.ExecContext(ctx, "UPDATE outbox SET attempts=attempts+1 WHERE row_id=?", p.id); err != nil {
		return p, err
	}
	return p, tx.Commit()
}

func PublishPending(ctx context.Context, db *sql.DB, run string, gen int64, send Sender) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, storage.Timeout)
	defer cancel()
	p, err := takePending(ctx, db, run, gen)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	network, done := context.WithTimeout(ctx, 5*time.Second)
	partition, offset, sendErr := send(network, p.record)
	done()
	if sendErr != nil {
		delay := min(250*time.Millisecond*time.Duration(1<<min(p.attempts, 5)), 5*time.Second) + time.Duration(rand.IntN(50))*time.Millisecond
		_, err = db.ExecContext(ctx, "UPDATE outbox SET last_error=?,next_retry_at_ms=? WHERE row_id=? AND status='PENDING'", sendErr.Error(), time.Now().Add(delay).UnixMilli(), p.id)
		return true, errors.Join(sendErr, err)
	}
	_, err = db.ExecContext(ctx, "UPDATE outbox SET status='SENT',published_at_ms=?,kafka_partition=?,kafka_offset=?,last_error=NULL WHERE row_id=? AND status='PENDING'", time.Now().UnixMilli(), partition, offset, p.id)
	return true, err
}

func Publisher(ctx context.Context, c config.Config, db *sql.DB, run string, gen int64, ready func()) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(c.Brokers...), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.ProducerLinger(0), kgo.RecordDeliveryTimeout(5*time.Second))
	if err != nil {
		return err
	}
	defer client.Close()
	ping, done := context.WithTimeout(ctx, 5*time.Second)
	err = client.Ping(ping)
	done()
	if err != nil {
		return err
	}
	ready()
	send := func(ctx context.Context, r contracts.Record) (int32, int64, error) {
		result := client.ProduceSync(ctx, &kgo.Record{Topic: r.Topic, Key: []byte(r.Key), Value: r.Value})
		if err := result.FirstErr(); err != nil {
			return 0, 0, err
		}
		return result[0].Record.Partition, result[0].Record.Offset, nil
	}
	for ctx.Err() == nil {
		worked, err := PublishPending(ctx, db, run, gen, send)
		if errors.Is(err, storage.ErrStopped) {
			return nil
		}
		if err != nil {
			slog.Error("outbox publication failed", "service", c.Service, "error", err)
		}
		if !worked || err != nil {
			if !Wait(ctx, c.OutboxPoll) {
				break
			}
		}
	}
	return nil
}

func Wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
