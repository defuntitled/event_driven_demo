package config

import "testing"

func TestLoad(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		role   string
		values map[string]string
		bad    bool
	}{
		{"orders", map[string]string{}, false},
		{"wrong", nil, true},
		{"payments", map[string]string{"INTERNAL_HTTP_TIMEOUT": "0s"}, true},
		{"reservations", map[string]string{"OUTBOX_POLL_INTERVAL": "oops"}, true},
	} {
		t.Run(tc.role, func(t *testing.T) {
			t.Parallel()
			c, err := Load(tc.role, func(k string) string { return tc.values[k] })
			if (err != nil) != tc.bad {
				t.Fatalf("config=%+v err=%v", c, err)
			}
			if !tc.bad && c.HTTPAddr != ":8081" {
				t.Fatal(c.HTTPAddr)
			}
		})
	}
}
