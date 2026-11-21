package lyrelog

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"
)

func TestSlogLevels(t *testing.T) {
	cases := map[slog.Level]Level{
		slog.LevelDebug - 4: LevelTrace, slog.LevelDebug - 1: LevelTrace,
		slog.LevelDebug: LevelDebug, slog.LevelInfo - 1: LevelDebug,
		slog.LevelInfo: LevelInfo, slog.LevelInfo + 2: LevelNotice,
		slog.LevelWarn: LevelWarning, slog.LevelError: LevelError, slog.LevelError + 3: LevelError,
		slog.LevelError + 4: LevelCritical, slog.LevelError + 8: LevelCritical,
	}
	for in, want := range cases {
		if got := SlogLevel(in); got != want {
			t.Errorf("%v: got %v, want %v", in, got, want)
		}
	}
}

func TestSlogHandler(t *testing.T) {
	f := newFakeServer(t)
	client := newTestClient(t, f)
	logger := slog.New(NewSlogHandler(client, &SlogOptions{Category: "server"}))

	logger.Info("request_done", "status", 200, "took", 1500*time.Millisecond, "err", errors.New("x"))
	logger.Warn("checkout_failed", "category", "billing", slog.Group("card", "brand", "visa", "last4", "4242"))
	logger.With("region", "eu").WithGroup("req").With("id", "r1").Error("slow", "ms", 900, slog.Group("empty"))
	logger.WithGroup("g").Info("nested_category", "category", "stays_an_attr")
	logger.Log(context.Background(), slog.LevelDebug-4, "very_fine")
	when := time.Now().Add(-time.Minute).Truncate(time.Millisecond).UTC()
	_ = logger.Handler().Handle(context.Background(), slog.NewRecord(when, slog.LevelInfo, "with_time", 0))

	if err := client.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	lines := f.received()[0].lines
	if len(lines) != 5 { // very_fine is below the client's debug minimum
		t.Fatalf("%d lines: %v", len(lines), f.messages())
	}

	first := lines[0]
	attrs := first["attrs"].(map[string]any)
	if first["category"] != "server" || first["level"] != "info" || attrs["status"] != float64(200) ||
		attrs["took"] != "1.5s" || attrs["err"] != "x" {
		t.Errorf("first %v", first)
	}

	second := lines[1]
	card := second["attrs"].(map[string]any)["card"].(map[string]any)
	if second["category"] != "billing" || second["level"] != "warning" || card["last4"] != "4242" {
		t.Errorf("second %v", second)
	}
	if _, ok := second["attrs"].(map[string]any)["category"]; ok {
		t.Error("the category attr is not sent as an attr")
	}

	third := lines[2]
	attrs = third["attrs"].(map[string]any)
	req := attrs["req"].(map[string]any)
	if third["level"] != "error" || attrs["region"] != "eu" || req["id"] != "r1" || req["ms"] != float64(900) {
		t.Errorf("third %v", third)
	}
	if _, ok := req["empty"]; ok {
		t.Error("an empty group is dropped")
	}

	fourth := lines[3]
	if fourth["category"] != "server" || fourth["attrs"].(map[string]any)["g"].(map[string]any)["category"] != "stays_an_attr" {
		t.Errorf("fourth %v", fourth)
	}
	if lines[4]["ts"] != when.Format(timestampLayout) {
		t.Errorf("the record's time is the event's: %v", lines[4]["ts"])
	}
}

func TestSlogHandlerEnabled(t *testing.T) {
	f := newFakeServer(t)
	client := newTestClient(t, f, func(c *Config) { c.MinimumLevel = LevelInfo })
	h := NewSlogHandler(client, &SlogOptions{Level: slog.LevelWarn})
	if h.Enabled(context.Background(), slog.LevelInfo) || !h.Enabled(context.Background(), slog.LevelError) {
		t.Error("the handler's level")
	}
	loose := NewSlogHandler(client, nil)
	if loose.Enabled(context.Background(), slog.LevelDebug) || !loose.Enabled(context.Background(), slog.LevelInfo) {
		t.Error("the client's minimum")
	}
	if NewSlogHandler(nil, nil).Enabled(context.Background(), slog.LevelError) {
		t.Error("a nil client is never enabled")
	}
}

func TestSlogSource(t *testing.T) {
	f := newFakeServer(t)
	client := newTestClient(t, f)
	slog.New(NewSlogHandler(client, &SlogOptions{AddSource: true})).Info("here")
	if err := client.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	source := f.received()[0].lines[0]["attrs"].(map[string]any)["source"].(map[string]any)
	if source["function"] != "go.lyrelog.com/lyrelog.TestSlogSource" {
		t.Errorf("source %v", source)
	}
}
