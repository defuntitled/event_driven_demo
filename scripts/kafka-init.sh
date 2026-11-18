#!/bin/bash
set -euo pipefail
for topic in orders.events payments.events reservations.events; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic "$topic" --partitions 2 --replication-factor 1 --config cleanup.policy=delete --config retention.ms=86400000
  description=$(/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic "$topic")
  if ! grep -q 'PartitionCount: 2' <<< "$description"; then
    printf 'unexpected partition count for %s\n' "$topic" >&2
    exit 1
  fi
  printf '%s\n' "$description"
done
