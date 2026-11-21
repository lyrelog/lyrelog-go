package lyrelog

import (
	"log/slog"
	"strings"
	"testing"
	"testing/slogtest"
	"time"
)

// TestSlogtest runs the standard library's conformance suite for slog handlers against
// SlogHandler, reading back what reached the (fake) server.
func TestSlogtest(t *testing.T) {
	var current *fakeServer
	var client *Client
	newHandler := func(t *testing.T) slog.Handler {
		current = newFakeServer(t)
		client = newTestClient(t, current, func(c *Config) { c.MinimumLevel = LevelTrace })
		return NewSlogHandler(client, nil)
	}
	result := func(t *testing.T) map[string]any {
		if strings.HasSuffix(t.Name(), "/zero-time") {
			// Lyrelog requires a ts: a zero Record.Time is stamped with time.Now().
			t.Skip("a zero time is stamped with the current time on purpose")
		}
		if err := client.Flush(t.Context()); err != nil {
			t.Fatal(err)
		}
		batches := current.received()
		if len(batches) != 1 || len(batches[0].lines) != 1 {
			t.Fatalf("%d batches", len(batches))
		}
		l := batches[0].lines[0]
		out := map[string]any{slog.MessageKey: l["message"], slog.LevelKey: l["level"]}
		if ts, err := time.Parse(time.RFC3339, l["ts"].(string)); err == nil {
			out[slog.TimeKey] = ts
		}
		if attrs, ok := l["attrs"].(map[string]any); ok {
			for key, value := range attrs {
				out[key] = value
			}
		}
		return out
	}
	slogtest.Run(t, newHandler, result)
}

func TestSlogValueKinds(t *testing.T) {
	when := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	cases := []struct {
		value slog.Value
		want  any
	}{
		{slog.StringValue("s"), "s"},
		{slog.Int64Value(-3), int64(-3)},
		{slog.Uint64Value(7), uint64(7)},
		{slog.Float64Value(1.5), 1.5},
		{slog.BoolValue(true), true},
		{slog.DurationValue(1500 * time.Millisecond), "1.5s"},
		{slog.TimeValue(when), when},
		{slog.AnyValue(point{1, "a"}), point{1, "a"}},
	}
	for _, c := range cases {
		if got := slogValue(c.value); got != c.want {
			t.Errorf("%v: got %#v, want %#v", c.value.Kind(), got, c.want)
		}
	}
}

// The SDK's own diagnostics start with "lyrelog: ". If ErrorLog is a log.Logger over this
// handler (slog.NewLogLogger), they must not come back as events.
func TestSlogHandlerSkipsTheSDKsOwnLines(t *testing.T) {
	f := newFakeServer(t)
	client := newTestClient(t, f)
	handler := NewSlogHandler(client, nil)
	slog.NewLogLogger(handler, slog.LevelError).Printf("lyrelog: the server refused a batch")
	slog.New(handler).Info("kept")
	if err := client.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := f.messages(); len(got) != 1 || got[0] != "kept" {
		t.Fatalf("%v", got)
	}
}

func TestLevelWrappers(t *testing.T) {
	f := newFakeServer(t)
	client := newTestClient(t, f, func(c *Config) { c.MinimumLevel = LevelTrace })
	logger := client.Logger("area")
	for _, log := range []func(string, ...Option){
		client.Trace, client.Debug, client.Info, client.Notice, client.Warning, client.Error, client.Critical,
		logger.Trace, logger.Debug, logger.Info, logger.Notice, logger.Warning, logger.Error, logger.Critical,
	} {
		log("e")
	}
	if err := client.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	lines := f.received()[0].lines
	want := []string{"trace", "debug", "info", "notice", "warning", "error", "critical"}
	if len(lines) != 14 {
		t.Fatalf("%d lines", len(lines))
	}
	for i, l := range lines {
		category := ""
		if i >= 7 {
			category = "area"
		}
		if l["level"] != want[i%7] || str(l["category"]) != category {
			t.Errorf("line %d: %v", i, l)
		}
	}
}
