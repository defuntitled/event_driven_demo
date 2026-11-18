package messaging

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/defuntitled/event_driven_demo/internal/config"
	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/orders"
	"github.com/defuntitled/event_driven_demo/internal/payments"
	"github.com/defuntitled/event_driven_demo/internal/reservations"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

func Handler(role string) storage.Handler {
	switch role {
	case "orders":
		return orders.Apply
	case "payments":
		return payments.Handle
	case "reservations":
		return reservations.ReserveSeat
	}
	return nil
}

func Consumer(ctx context.Context, c config.Config, db *sql.DB, run string, gen int64, instance string, ready func()) error {
	var once sync.Once
	client, err := kgo.NewClient(kgo.SeedBrokers(c.Brokers...), kgo.ConsumerGroup("demo."+c.Service), kgo.ConsumeTopics(contracts.Subscriptions(c.Service)...), kgo.DisableAutoCommit(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.BlockRebalanceOnPoll(), kgo.SessionTimeout(6*time.Second), kgo.HeartbeatInterval(2*time.Second), kgo.RebalanceTimeout(60*time.Second), kgo.OnPartitionsAssigned(func(context.Context, *kgo.Client, map[string][]int32) { once.Do(ready) }))
	if err != nil {
		return err
	}
	defer client.CloseAllowingRebalance()
	for ctx.Err() == nil {
		fetch := client.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			return nil
		}
		if errs := fetch.Errors(); len(errs) > 0 {
			client.AllowRebalance()
			return errs[0].Err
		}
		for _, record := range fetch.Records() {
			r := contracts.Record{Topic: record.Topic, Partition: record.Partition, Offset: record.Offset, Key: string(record.Key), Value: record.Value}
			result, err := storage.Process(ctx, db, c.Service, run, gen, instance, r, Handler(c.Service))
			if errors.Is(err, storage.ErrStopped) {
				client.AllowRebalance()
				return nil
			}
			if err != nil {
				client.AllowRebalance()
				if f, ok := errors.AsType[*contracts.Failure](err); ok && f.Code == "PROTOCOL_ERROR" {
					diagCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
					_, diagErr := db.ExecContext(diagCtx, "UPDATE worker_control SET desired_state='ERROR',last_error=?,updated_at_ms=? WHERE worker='consumer' AND run_id=? AND generation=?", err.Error(), time.Now().UnixMilli(), run, gen)
					cancel()
					return errors.Join(err, diagErr)
				}
				return err
			}
			if result.Crash {
				os.Exit(73)
			}
			commitCtx, done := context.WithTimeout(ctx, 5*time.Second)
			commitErr := client.CommitRecords(commitCtx, record)
			done()
			diagErr := storage.ObserveCommit(context.WithoutCancel(ctx), db, result.AttemptID, commitErr)
			client.AllowRebalance()
			if err = errors.Join(commitErr, diagErr); err != nil {
				return err
			}
			slog.Info("record completed", "service", c.Service, "run_id", run, "generation", gen, "event_key", r.Key, "topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "outcome", result.Outcome)
		}
		client.AllowRebalance()
	}
	return nil
}
