//go:build integration

package integration

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/defuntitled/event_driven_demo/internal/config"
	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/messaging"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

func TestIsolatedPoisonConsumer(t *testing.T) {
	t.Parallel()
	broker := os.Getenv("PROTOCOL_KAFKA_BROKER")
	if broker == "" {
		t.Skip("requires dedicated empty protocol Kafka broker")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	db, err := storage.Open(ctx, filepath.Join(t.TempDir(), "poison.db"), "payments")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	run := "run_0123456789abcdef0123456789abcdef"
	_, err = db.ExecContext(ctx, "INSERT INTO runs VALUES (?,1,'happy_path',1,0); UPDATE service_state SET active_run_id=?,generation=1; UPDATE worker_control SET desired_state='RUNNING',run_id=?,generation=1", run, run, run)
	if err != nil {
		t.Fatal(err)
	}
	c, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// Deliberately invalid JSON: it cannot claim a trustworthy run/order identity.
	poison := &kgo.Record{Topic: "orders.events", Key: []byte("bad"), Value: []byte("not-json")}
	if err = c.ProduceSync(ctx, poison).FirstErr(); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		result <- messaging.Consumer(ctx, config.Config{Service: "payments", Brokers: []string{broker}}, db, run, 1, "isolated", func() {})
	}()
	select {
	case err = <-result:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	f, ok := errors.AsType[*contracts.Failure](err)
	if !ok || f.Code != "PROTOCOL_ERROR" {
		t.Fatalf("consumer did not stop on protocol: %v", err)
	}
	var desired string
	if err = db.QueryRowContext(ctx, "SELECT desired_state FROM worker_control WHERE worker='consumer'").Scan(&desired); err != nil || desired != "ERROR" {
		t.Fatalf("desired=%s err=%v", desired, err)
	}
	p, err := storage.Records(ctx, db, "payments", "processing_attempts", run, "", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Items) != 1 || p.Items[0]["raw_json"] != "not-json" || p.Items[0]["commit_status"] != "NOT_ATTEMPTED" {
		t.Fatal(p)
	}
	req := kmsg.NewPtrOffsetFetchRequest()
	g := kmsg.NewOffsetFetchRequestGroup()
	g.Group = "demo.payments"
	g.Topics = []kmsg.OffsetFetchRequestGroupTopic{{Topic: poison.Topic, Partitions: []int32{poison.Partition}}}
	req.Groups = []kmsg.OffsetFetchRequestGroup{g}
	raw, err := c.Request(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	response := raw.(*kmsg.OffsetFetchResponse)
	if len(response.Groups) != 1 || response.Groups[0].ErrorCode != 0 {
		t.Fatalf("invalid offset response: %+v", response)
	}
	for _, topic := range response.Groups[0].Topics {
		for _, partition := range topic.Partitions {
			if partition.ErrorCode != 0 || partition.Offset > poison.Offset {
				t.Fatalf("poison offset skipped: %+v", partition)
			}
		}
	}
	t.Logf("worker ERROR, raw payload retained, committed-next did not pass %s:%d:%d", poison.Topic, poison.Partition, poison.Offset)
}
