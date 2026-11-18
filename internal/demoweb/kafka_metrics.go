package demoweb

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/defuntitled/event_driven_demo/internal/messaging"
)

func (a *App) Metrics(ctx context.Context) {
	client, err := kgo.NewClient(kgo.SeedBrokers(a.Config.Brokers...))
	if err != nil {
		slog.Error("kafka metrics client failed", "error", err)
		return
	}
	defer client.Close()
	for ctx.Err() == nil {
		snapshot, err := messaging.FetchKafka(ctx, client)
		a.metricsMu.Lock()
		if err != nil {
			a.metrics.OK = false
			a.metrics.Error = err.Error()
		} else {
			a.metrics = snapshot
		}
		a.metricsMu.Unlock()
		if !messaging.Wait(ctx, a.Config.MetricsInterval) {
			return
		}
	}
}
