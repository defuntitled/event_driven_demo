package messaging

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

type metricsRequester struct{}

func (metricsRequester) Request(_ context.Context, r kmsg.Request) (kmsg.Response, error) {
	switch r.(type) {
	case *kmsg.MetadataRequest:
		return &kmsg.MetadataResponse{Topics: []kmsg.MetadataResponseTopic{{Topic: new("orders.events"), Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0, Leader: 1}}}}}, nil
	case *kmsg.ListOffsetsRequest:
		return &kmsg.ListOffsetsResponse{Topics: []kmsg.ListOffsetsResponseTopic{{Topic: "orders.events", Partitions: []kmsg.ListOffsetsResponseTopicPartition{{Partition: 0, Offset: 10}}}}}, nil
	case *kmsg.OffsetFetchRequest:
		return &kmsg.OffsetFetchResponse{Groups: []kmsg.OffsetFetchResponseGroup{{Group: "demo.payments", Topics: []kmsg.OffsetFetchResponseGroupTopic{{Topic: "orders.events", Partitions: []kmsg.OffsetFetchResponseGroupTopicPartition{{Partition: 0, Offset: -1}}}}}}}, nil
	}
	panic("unexpected request in test")
}
func TestKafkaUnknownCommittedOffset(t *testing.T) {
	t.Parallel()
	snapshot, err := FetchKafka(t.Context(), metricsRequester{})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range snapshot.Items {
		if item.Group == "demo.payments" {
			if item.CommittedNext != nil || item.Lag != nil || item.LogEnd == nil || *item.LogEnd != 10 {
				t.Fatalf("unknown committed became zero: %+v", item)
			}
			return
		}
	}
	t.Fatal("missing payments metrics")
}
