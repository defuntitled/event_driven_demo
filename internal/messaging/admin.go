package messaging

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/defuntitled/event_driven_demo/internal/contracts"
)

type Requester interface {
	Request(context.Context, kmsg.Request) (kmsg.Response, error)
}
type KafkaItem struct {
	Topic         string `json:"topic"`
	Partition     int32  `json:"partition"`
	Group         string `json:"group_id"`
	CommittedNext *int64 `json:"committed_next_offset"`
	LogEnd        *int64 `json:"log_end_offset"`
	Lag           *int64 `json:"lag"`
}
type KafkaSnapshot struct {
	OK          bool        `json:"ok"`
	SampledAtMS int64       `json:"sampled_at_ms"`
	Items       []KafkaItem `json:"items"`
	Error       string      `json:"error,omitempty"`
}
type coordinate struct {
	topic     string
	partition int32
}

func FetchKafka(ctx context.Context, client Requester) (KafkaSnapshot, error) {
	snapshot := KafkaSnapshot{Items: []KafkaItem{}, SampledAtMS: time.Now().UnixMilli()}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	parts, err := metadata(ctx, client)
	if err != nil {
		return snapshot, err
	}
	ends, err := endOffsets(ctx, client, parts)
	if err != nil {
		return snapshot, err
	}
	committed, err := groupOffsets(ctx, client, parts)
	if err != nil {
		return snapshot, err
	}
	for _, role := range []string{"orders", "payments", "reservations", "journal"} {
		group := "demo." + role
		for _, topic := range contracts.Subscriptions(role) {
			for _, partition := range parts[topic] {
				key := coordinate{topic, partition}
				item := KafkaItem{Topic: topic, Partition: partition, Group: group}
				if n, ok := ends[key]; ok {
					item.LogEnd = new(n)
				}
				if n, ok := committed[group][key]; ok && n >= 0 {
					item.CommittedNext = new(n)
					if item.LogEnd != nil && *item.LogEnd >= n {
						item.Lag = new(*item.LogEnd - n)
					}
				}
				snapshot.Items = append(snapshot.Items, item)
			}
		}
	}
	snapshot.OK = true
	return snapshot, nil
}

func metadata(ctx context.Context, client Requester) (map[string][]int32, error) {
	req := kmsg.NewPtrMetadataRequest()
	for _, topic := range contracts.Topics {
		req.Topics = append(req.Topics, kmsg.MetadataRequestTopic{Topic: new(topic)})
	}
	raw, err := client.Request(ctx, req)
	if err != nil {
		return nil, err
	}
	resp, ok := raw.(*kmsg.MetadataResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected metadata response")
	}
	parts := make(map[string][]int32, len(resp.Topics))
	for _, topic := range resp.Topics {
		if err = kerr.ErrorForCode(topic.ErrorCode); err != nil {
			return nil, err
		}
		if topic.Topic == nil {
			continue
		}
		for _, p := range topic.Partitions {
			if err = kerr.ErrorForCode(p.ErrorCode); err != nil {
				return nil, err
			}
			parts[*topic.Topic] = append(parts[*topic.Topic], p.Partition)
		}
		slices.Sort(parts[*topic.Topic])
	}
	return parts, nil
}

func endOffsets(ctx context.Context, client Requester, parts map[string][]int32) (map[coordinate]int64, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	for _, topic := range contracts.Topics {
		t := kmsg.ListOffsetsRequestTopic{Topic: topic}
		for _, id := range parts[topic] {
			p := kmsg.NewListOffsetsRequestTopicPartition()
			p.Partition = id
			p.Timestamp = -1
			t.Partitions = append(t.Partitions, p)
		}
		if len(t.Partitions) > 0 {
			req.Topics = append(req.Topics, t)
		}
	}
	raw, err := client.Request(ctx, req)
	if err != nil {
		return nil, err
	}
	resp, ok := raw.(*kmsg.ListOffsetsResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected list offsets response")
	}
	offsets := make(map[coordinate]int64)
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if err = kerr.ErrorForCode(p.ErrorCode); err != nil {
				return nil, err
			}
			if p.Offset >= 0 {
				offsets[coordinate{t.Topic, p.Partition}] = p.Offset
			}
		}
	}
	return offsets, nil
}

func groupOffsets(ctx context.Context, client Requester, parts map[string][]int32) (map[string]map[coordinate]int64, error) {
	req := kmsg.NewPtrOffsetFetchRequest()
	for _, role := range []string{"orders", "payments", "reservations", "journal"} {
		g := kmsg.NewOffsetFetchRequestGroup()
		g.Group = "demo." + role
		for _, topic := range contracts.Subscriptions(role) {
			if len(parts[topic]) > 0 {
				g.Topics = append(g.Topics, kmsg.OffsetFetchRequestGroupTopic{Topic: topic, Partitions: parts[topic]})
			}
		}
		req.Groups = append(req.Groups, g)
	}
	raw, err := client.Request(ctx, req)
	if err != nil {
		return nil, err
	}
	resp, ok := raw.(*kmsg.OffsetFetchResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected group offsets response")
	}
	out := make(map[string]map[coordinate]int64, len(resp.Groups))
	for _, g := range resp.Groups {
		if err = kerr.ErrorForCode(g.ErrorCode); err != nil {
			return nil, err
		}
		out[g.Group] = make(map[coordinate]int64)
		for _, t := range g.Topics {
			for _, p := range t.Partitions {
				if err = kerr.ErrorForCode(p.ErrorCode); err != nil {
					return nil, err
				}
				out[g.Group][coordinate{t.Topic, p.Partition}] = p.Offset
			}
		}
	}
	return out, nil
}
