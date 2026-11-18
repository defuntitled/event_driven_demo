package demoweb

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/messaging"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

func SaveRecord(ctx context.Context, db *sql.DB, r contracts.Record) error {
	ctx, cancel := context.WithTimeout(ctx, storage.Timeout)
	defer cancel()
	var e contracts.Event
	var parseError any
	if err := json.Unmarshal(r.Value, &e); err != nil {
		parseError = err.Error()
	}
	_, err := db.ExecContext(ctx, `INSERT OR IGNORE INTO kafka_records(topic,partition_id,offset_value,record_key,event_id,run_id,order_id,event_type,causation_id,occurred_at,observed_at_ms,raw_json,parse_error) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`, r.Topic, r.Partition, r.Offset, r.Key, e.EventID, e.RunID, e.OrderID, e.Type, e.CausationID, e.OccurredAt.UTC().Format(time.RFC3339Nano), time.Now().UnixMilli(), string(r.Value), parseError)
	return err
}

func (a *App) Journal(ctx context.Context) {
	for ctx.Err() == nil {
		if err := a.journalSession(ctx); err != nil && ctx.Err() == nil {
			slog.Error("journal stopped", "error", err)
		}
		if !messaging.Wait(ctx, time.Second) {
			return
		}
	}
}

func (a *App) journalSession(ctx context.Context) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(a.Config.Brokers...), kgo.ConsumerGroup("demo.journal"), kgo.ConsumeTopics(contracts.Topics...), kgo.DisableAutoCommit(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.BlockRebalanceOnPoll(), kgo.SessionTimeout(6*time.Second), kgo.HeartbeatInterval(2*time.Second), kgo.RebalanceTimeout(60*time.Second))
	if err != nil {
		return err
	}
	defer client.CloseAllowingRebalance()
	for ctx.Err() == nil {
		fetched := client.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			return nil
		}
		if errs := fetched.Errors(); len(errs) > 0 {
			client.AllowRebalance()
			return errs[0].Err
		}
		for _, r := range fetched.Records() {
			if err = SaveRecord(ctx, a.DB, contracts.Record{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: string(r.Key), Value: r.Value}); err != nil {
				client.AllowRebalance()
				return err
			}
			deadline, done := context.WithTimeout(ctx, 5*time.Second)
			err = client.CommitRecords(deadline, r)
			done()
			client.AllowRebalance()
			if err != nil {
				return err
			}
		}
		client.AllowRebalance()
	}
	return nil
}
