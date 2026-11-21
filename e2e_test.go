package lyrelog

import (
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"
)

// Against a running Lyrelog (in the lyrelog repo: make up, then make project for a key):
//
//	LYRELOG_E2E_URL=http://localhost:8090 LYRELOG_E2E_KEY=lyr_ing_... go test -run EndToEnd -v
//
// Skipped when either variable is missing.
func TestEndToEnd(t *testing.T) {
	endpoint, key := os.Getenv("LYRELOG_E2E_URL"), os.Getenv("LYRELOG_E2E_KEY")
	if endpoint == "" || key == "" {
		t.Skip("set LYRELOG_E2E_URL and LYRELOG_E2E_KEY to run against a Lyrelog server")
	}
	client, err := New(Config{Endpoint: endpoint, APIKey: key, Environment: "development", Release: "0.9.0-e2e", FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	marker := newUUID()[:8]
	client.SetUser("e2e-user")
	client.Info("e2e_go_"+marker, Category("test"), Attrs{"latency_ms": 412, "gc": map[string]any{"ok": true}, "tags": []string{"a", "b"}, "password": "hunter2"},
		Payload(map[string]any{"note": "from go test", "headers": map[string]any{"Authorization": "Bearer e2e"}}))
	client.CaptureError(errors.Join(errors.New("first"), errors.New("second")), Category("test"))
	slog.New(NewSlogHandler(client, &SlogOptions{Category: "test"})).Warn("e2e_go_slog_"+marker, slog.Group("req", "route", "/x"))
	if err := client.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if s := client.Stats(); s.Sent != 3 || s.Rejected != 0 || s.Queued != 0 {
		t.Fatalf("%+v", s)
	}
	t.Logf("sent e2e_go_%s", marker)
}
