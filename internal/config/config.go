package config

import (
	"cmp"
	"fmt"
	"strings"
	"time"
)

type Config struct {
	Service, HTTPAddr, DatabasePath                           string
	Brokers                                                   []string
	URLs                                                      map[string]string
	OutboxPoll, HTTPTimeout, InternalTimeout, MetricsInterval time.Duration
}

func Load(role string, getenv func(string) string) (Config, error) {
	port := map[string]string{"demo-web": "8080", "orders": "8081", "payments": "8082", "reservations": "8083"}[role]
	if port == "" {
		return Config{}, fmt.Errorf("unknown service %q", role)
	}
	c := Config{Service: role, HTTPAddr: cmp.Or(getenv("HTTP_ADDR"), ":"+port), DatabasePath: cmp.Or(getenv("DATABASE_PATH"), "/data/"+role+".db"), Brokers: strings.Split(cmp.Or(getenv("KAFKA_BROKERS"), "kafka:9092"), ","), URLs: map[string]string{}}
	for _, r := range []string{"orders", "payments", "reservations"} {
		c.URLs[r] = cmp.Or(getenv(strings.ToUpper(r)+"_URL"), "http://"+r+":"+map[string]string{"orders": "8081", "payments": "8082", "reservations": "8083"}[r])
	}
	for _, d := range []struct {
		key, def string
		target   *time.Duration
	}{
		{"OUTBOX_POLL_INTERVAL", "100ms", &c.OutboxPoll}, {"HTTP_REQUEST_TIMEOUT", "5s", &c.HTTPTimeout}, {"INTERNAL_HTTP_TIMEOUT", "3s", &c.InternalTimeout}, {"KAFKA_METRICS_INTERVAL", "1s", &c.MetricsInterval},
	} {
		value, err := time.ParseDuration(cmp.Or(getenv(d.key), d.def))
		if err != nil || value <= 0 {
			return Config{}, fmt.Errorf("invalid duration %s", d.key)
		}
		*d.target = value
	}
	return c, nil
}
