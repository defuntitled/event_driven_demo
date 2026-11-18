//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/control"
	"github.com/defuntitled/event_driven_demo/internal/demoweb"
	"github.com/defuntitled/event_driven_demo/internal/httpapi"
	"github.com/defuntitled/event_driven_demo/internal/messaging"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

func (h *harness) service(role string) control.ServiceState {
	h.t.Helper()
	var state control.ServiceState
	err := httpapi.Call(h.t.Context(), h.client, "GET", internalURL(role)+"/internal/state", nil, &state)
	if err != nil {
		h.t.Fatal(err)
	}
	return state
}
func internalURL(role string) string {
	return "http://localhost:" + map[string]string{"orders": "18081", "payments": "18082", "reservations": "18083"}[role]
}
func (h *harness) docker(args ...string) {
	h.t.Helper()
	project := os.Getenv("DEMO_COMPOSE_PROJECT")
	if project != "last-ticket-go-acceptance" {
		h.t.Fatal("disruption tests require DEMO_COMPOSE_PROJECT=last-ticket-go-acceptance")
	}
	ctx, cancel := context.WithTimeout(h.t.Context(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", append([]string{"compose", "-p", project, "-f", "../../compose.yaml", "-f", "../../compose.test.yaml"}, args...)...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		h.t.Fatalf("docker %v: %v %s", args, err, output)
	}
}
func (h *harness) events() []map[string]any {
	h.t.Helper()
	var p storage.Page
	h.call("GET", "/api/presenter/events?run_id="+h.run+"&order_id="+h.order, nil, &p)
	return p.Items
}
func (h *harness) publish(raw string, key string, topic string) {
	h.t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers("localhost:19092"))
	if err != nil {
		h.t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(h.t.Context(), 10*time.Second)
	defer cancel()
	if err = c.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(key), Value: []byte(raw)}).FirstErr(); err != nil {
		h.t.Fatal(err)
	}
}
func (h *harness) journalCaughtUp() {
	h.t.Helper()
	var sampled messaging.KafkaSnapshot
	h.await("kafka metrics", func() bool { h.call("GET", "/api/presenter/kafka", nil, &sampled); return sampled.OK })
	targets := map[string]int64{}
	for _, i := range sampled.Items {
		if i.Group == "demo.journal" && i.LogEnd != nil {
			targets[fmt.Sprint(i.Topic, ":", i.Partition)] = *i.LogEnd
		}
	}
	h.await("journal reaches sampled log end", func() bool {
		var now messaging.KafkaSnapshot
		h.call("GET", "/api/presenter/kafka", nil, &now)
		if !now.OK {
			return false
		}
		for _, i := range now.Items {
			if i.Group != "demo.journal" {
				continue
			}
			end := targets[fmt.Sprint(i.Topic, ":", i.Partition)]
			if end > 0 && (i.CommittedNext == nil || *i.CommittedNext < end) {
				return false
			}
		}
		return true
	})
}
func TestKafkaOutageRetainsOutbox(t *testing.T) {
	t.Parallel()
	stand.Lock()
	defer stand.Unlock()
	h := newHarness(t)
	h.prepare("happy_path")
	h.docker("stop", "kafka")
	defer h.docker("start", "kafka")
	h.buy()
	db := h.databases()
	if db["orders"].Data["outbox"][0]["status"] != "PENDING" {
		t.Fatal("outbox lost during outage")
	}
	h.docker("start", "kafka")
	h.status("CONFIRMED")
}
func TestRepublishAndBusinessDuplicate(t *testing.T) {
	t.Parallel()
	stand.Lock()
	defer stand.Unlock()
	h := newHarness(t)
	h.prepare("happy_path")
	h.buy()
	h.status("CONFIRMED")
	h.journalCaughtUp()
	raw := h.databases()["orders"].Data["outbox"][0]["envelope_json"].(string)
	h.publish(raw, h.order, "orders.events")
	h.await("duplicate event", func() bool {
		for _, a := range h.databases()["payments"].Data["processing_attempts"] {
			if a["outcome"] == "DUPLICATE_EVENT" {
				return true
			}
		}
		return false
	})
	var e contracts.Event
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		t.Fatal(err)
	}
	id, err := contracts.NewID("evt")
	if err != nil {
		t.Fatal(err)
	}
	e.EventID = id
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	h.publish(string(b), h.order, "orders.events")
	h.await("business duplicate", func() bool {
		for _, a := range h.databases()["payments"].Data["processing_attempts"] {
			if a["outcome"] == "DUPLICATE_BUSINESS" {
				return true
			}
		}
		return false
	})
	h.journalCaughtUp()
	n := 0
	for _, e := range h.events() {
		if e["event_type"] == "OrderCreated" {
			n++
		}
	}
	if n != 3 || len(h.databases()["payments"].Data["payment_operations"]) != 1 {
		t.Fatalf("physical=%d or duplicate charge", n)
	}
}
func TestResetIgnoresOldEvent(t *testing.T) {
	t.Parallel()
	stand.Lock()
	defer stand.Unlock()
	h := newHarness(t)
	h.prepare("reservation_paused")
	h.buy()
	h.status("PAID_PENDING_RESERVATION")
	oldRun, oldOrder := h.run, h.order
	raw := h.databases()["payments"].Data["outbox"][0]["envelope_json"].(string)
	h.prepare("happy_path")
	h.order = ""
	h.publish(raw, oldOrder, "payments.events")
	h.await("old event ignored", func() bool {
		var p storage.Page
		err := httpapi.Call(t.Context(), h.client, "GET", internalURL("reservations")+"/internal/records/processing_attempts?run_id="+oldRun, nil, &p)
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range p.Items {
			if a["outcome"] == "IGNORED_RUN" {
				return true
			}
		}
		return false
	})
	db := h.databases()
	if db["reservations"].Data["inventory"][0]["available"] != float64(1) || len(db["reservations"].Data["reservation_decisions"]) != 0 {
		t.Fatal("old event changed new run")
	}
}
func TestServiceRestartStates(t *testing.T) {
	t.Parallel()
	stand.Lock()
	defer stand.Unlock()
	h := newHarness(t)
	for _, scenario := range []string{"happy_path", "reservation_paused", "payment_crash"} {
		h.prepare(scenario)
		role, desired := "payments", "RUNNING"
		if scenario == "reservation_paused" {
			role, desired = "reservations", "PAUSED"
		}
		if scenario == "payment_crash" {
			h.buy()
			h.await("fault", func() bool { return h.service(role).Workers["consumer"].DesiredState == "FAULTED" })
			desired = "FAULTED"
		}
		h.docker("restart", role)
		h.await("saved worker state", func() bool {
			var s control.ServiceState
			err := httpapi.Call(t.Context(), h.client, "GET", internalURL(role)+"/internal/state", nil, &s)
			if err != nil {
				return false
			}
			w := s.Workers["consumer"]
			return s.RunID == h.run && w.DesiredState == desired && ((desired == "RUNNING" && w.ActualState == "RUNNING") || (desired != "RUNNING" && w.PID == 0))
		})
		h.docker("restart", "demo-web")
		h.await("same session", func() bool {
			var s demoweb.Session
			err := httpapi.Call(t.Context(), h.client, "GET", h.base+"/api/session", nil, &s)
			return err == nil && s.State == "READY" && s.RunID == h.run && s.Generation == h.generation
		})
	}
}

// Use a separate group and temporary owner database to hit offset commit precisely,
// without injecting test-only faults into the production binary.
func TestOffsetCommitNetworkFailure(t *testing.T) {
	t.Parallel()
	stand.Lock()
	defer stand.Unlock()
	h := newHarness(t)
	h.prepare("orders_outbox_paused")
	run, err := contracts.NewID("run")
	if err != nil {
		t.Fatal(err)
	}
	order, err := contracts.NewID("ord")
	if err != nil {
		t.Fatal(err)
	}
	e, err := contracts.NewEvent(contracts.OrderCreated, run, order, nil, contracts.Payload{ConcertID: contracts.ConcertID, BuyerID: "test", Quantity: 1, AmountCents: 50000, Currency: "RUB"})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	db, err := storage.Open(t.Context(), filepath.Join(t.TempDir(), "payments.db"), "payments")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.ExecContext(t.Context(), "INSERT INTO runs VALUES (?,1,'happy_path',1,0); UPDATE service_state SET active_run_id=?,generation=1; UPDATE worker_control SET desired_state='RUNNING',run_id=?,generation=1", run, run, run)
	if err != nil {
		t.Fatal(err)
	}
	group := "acceptance." + run
	newClient := func() *kgo.Client {
		c, err := kgo.NewClient(kgo.SeedBrokers("localhost:19092"), kgo.ConsumerGroup(group), kgo.SessionTimeout(6*time.Second), kgo.HeartbeatInterval(2*time.Second), kgo.RebalanceTimeout(60*time.Second), kgo.ConsumeTopics("orders.events"), kgo.DisableAutoCommit(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	c := newClient()
	defer c.Close()
	h.publish(string(raw), order, "orders.events")
	consume := func(c *kgo.Client) *kgo.Record {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		for ctx.Err() == nil {
			f := c.PollRecords(ctx, 1)
			if len(f.Errors()) > 0 {
				t.Fatal(f.Errors())
			}
			for _, r := range f.Records() {
				if string(r.Key) == order {
					return r
				}
			}
		}
		t.Fatal("target event missing")
		return nil
	}
	record := consume(c)
	r := contracts.Record{Topic: record.Topic, Partition: record.Partition, Offset: record.Offset, Key: order, Value: raw}
	result, err := storage.Process(t.Context(), db, "payments", run, 1, "before", r, messaging.Handler("payments"))
	if err != nil {
		t.Fatal(err)
	}
	h.docker("stop", "kafka")
	defer h.docker("start", "kafka")
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	err = c.CommitRecords(ctx, record)
	cancel()
	if err == nil {
		t.Fatal("commit acknowledged while broker stopped")
	}
	if err = storage.ObserveCommit(t.Context(), db, result.AttemptID, err); err != nil {
		t.Fatal(err)
	}
	h.docker("start", "kafka")
	c.Close()
	next := newClient()
	defer next.Close()
	again := consume(next)
	if again.Partition != record.Partition || again.Offset != record.Offset {
		t.Fatal("did not redeliver same coordinate")
	}
	duplicate, err := storage.Process(t.Context(), db, "payments", run, 1, "after", r, messaging.Handler("payments"))
	if err != nil || duplicate.Outcome != "DUPLICATE_EVENT" {
		t.Fatalf("%+v %v", duplicate, err)
	}
	var n int
	if err = db.QueryRowContext(t.Context(), "SELECT count(*) FROM payment_operations").Scan(&n); err != nil || n != 1 {
		t.Fatalf("charges=%d err=%v", n, err)
	}
	ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err = next.CommitRecords(ctx, again); err != nil {
		t.Fatal(err)
	}
}

func TestProtocolStopsWithoutCommit(t *testing.T) {
	t.Parallel()
	stand.Lock()
	defer stand.Unlock()
	// Isolated owner database: malformed records never enter the demo topics.
	db, err := storage.Open(t.Context(), filepath.Join(t.TempDir(), "protocol.db"), "payments")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	run := "run_0123456789abcdef0123456789abcdef"
	for _, raw := range []string{"not-json", `{"schema_version":99}`} {
		result, err := storage.Process(t.Context(), db, "payments", run, 1, "protocol-test", contracts.Record{Topic: "orders.events", Partition: 1, Offset: 55, Value: []byte(raw)}, messaging.Handler("payments"))
		if err == nil || !strings.Contains(err.Error(), "schema") && raw != "not-json" {
			t.Fatalf("accepted invalid payload: %v", err)
		}
		var commit, outcome, stored string
		if err = db.QueryRowContext(t.Context(), "SELECT outcome,commit_status,raw_json FROM processing_attempts WHERE attempt_id=?", result.AttemptID).Scan(&outcome, &commit, &stored); err != nil {
			t.Fatal(err)
		}
		if outcome != "PROTOCOL_ERROR" || commit != "NOT_ATTEMPTED" || stored != raw {
			t.Fatalf("%s %s %s", outcome, commit, stored)
		}
	}
}
