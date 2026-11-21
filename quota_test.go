package lyrelog

import (
	"log"
	"log/slog"
	"strings"
	"testing"
	"time"
)

const (
	quotaBody     = `{"error": "this organization's free plan includes 1000000 events a month and they're used: upgrade it, or events resume on 1 December", "plan": "free", "limit": 1000000, "used": 1000123}`
	rateLimitBody = `{"error": "rate limit for this project"}`
)

// The quota 429 warns once, stays quiet until a batch is accepted, then warns again. The
// rate-limit 429 and a non-JSON 429 never warn. Retrying is unchanged: the events are kept.
func TestQuotaStopWarnsOnceUntilABatchIsAccepted(t *testing.T) {
	f := newFakeServer(t,
		response{status: 429, body: rateLimitBody},
		response{status: 429, body: "Too Many Requests"},
		response{status: 429, body: quotaBody},
		response{status: 429, body: quotaBody},
		response{status: 200},
		response{status: 429, body: quotaBody},
		response{status: 200},
	)
	var logs strings.Builder
	client := newTestClient(t, f, func(c *Config) { c.ErrorLog = log.New(&logs, "", 0) })
	warnings := func() int { return strings.Count(logs.String(), "Events are kept and sent when sending resumes") }

	client.Info("a")
	for i, want := range []int{0, 0, 1, 1} { // rate limit, plain 429, quota, quota
		if err := client.Flush(t.Context()); err == nil {
			t.Fatalf("flush %d: a 429 must be retried", i)
		}
		if got := warnings(); got != want {
			t.Fatalf("after response %d: %d warnings, want %d\n%s", i, got, want, logs.String())
		}
		if s := client.Stats(); s.Queued != 1 || s.Dropped != 0 {
			t.Fatalf("the events are kept: %+v", s)
		}
	}
	if err := client.Flush(t.Context()); err != nil { // 200
		t.Fatal(err)
	}
	client.Info("b")
	if err := client.Flush(t.Context()); err == nil { // quota again
		t.Fatal("a 429 must be retried")
	}
	if got := warnings(); got != 2 {
		t.Fatalf("an accepted batch re-arms the warning: %d\n%s", got, logs.String())
	}
	if err := client.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := f.messages(); strings.Join(got, ",") != "a,a,a,a,a,b,b" {
		t.Fatalf("sent %v", got)
	}

	want := "Lyrelog: this organization's free plan includes 1000000 events a month and they're used: upgrade it, or events resume on 1 December. " +
		"Events are kept and sent when sending resumes; how many wait depends on the platform (here: in memory, up to 10000 events and 16 MiB, oldest dropped first; lost when the process ends)."
	if first := strings.SplitN(logs.String(), "\n", 2)[0]; first != want {
		t.Fatalf("warning\n got %q\nwant %q", first, want)
	}
	if strings.Contains(logs.String(), "lyr_ing_test") {
		t.Fatal("the key is never logged")
	}
}

func TestQuotaStopKeepsRetryAfter(t *testing.T) {
	f := newFakeServer(t, response{status: 429, retryAfter: "3600", body: quotaBody})
	client := newTestClient(t, f)
	client.Info("a")
	_ = client.Flush(t.Context())
	if wait := time.Until(client.serverWait); wait < 3599*time.Second {
		t.Fatalf("Retry-After ignored: %v", wait)
	}
	if s := client.Stats(); s.Queued != 1 {
		t.Fatalf("kept: %+v", s)
	}
}

// Without an ErrorLog the warning still reaches stderr, and it doesn't use up the one-time
// stderr line kept for a refused key or a dropped batch.
func TestQuotaStopReachesStderrWithoutAnErrorLog(t *testing.T) {
	f := newFakeServer(t, response{status: 429, body: quotaBody}, response{status: 404})
	client := newTestClient(t, f, func(c *Config) { c.MaxQueue = 50; c.MaxQueueBytes = 1 << 20 })
	var stderr strings.Builder
	client.stderr = &stderr
	client.Info("a")
	_ = client.Flush(t.Context())
	if got := stderr.String(); !strings.Contains(got, "up to 50 events and 1 MiB") {
		t.Fatalf("stderr %q", got)
	}
	_ = client.Flush(t.Context())
	if got := stderr.String(); !strings.Contains(got, "404") {
		t.Fatalf("the dropped batch is still printed: %q", got)
	}
}

// The quota line starts with "Lyrelog: ": through slog.NewLogLogger it must not loop back.
func TestSlogHandlerSkipsTheQuotaWarning(t *testing.T) {
	f := newFakeServer(t, response{status: 429, body: quotaBody})
	client := newTestClient(t, f)
	handler := NewSlogHandler(client, nil)
	client.errorLog = slog.NewLogLogger(handler, slog.LevelWarn)
	client.Info("a")
	_ = client.Flush(t.Context())
	if err := client.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := f.messages(); strings.Join(got, ",") != "a,a" {
		t.Fatalf("sent %v", got)
	}
}

func TestQuotaClassify(t *testing.T) {
	now := time.Now()
	cases := map[string]bool{
		quotaBody:                      true,
		rateLimitBody:                  false,
		``:                             false,
		`not json`:                     false,
		`{"plan": null}`:               false,
		`{"plan": "free"}`:             true,
		`[1,2]`:                        false,
		`{"error": 5, "plan": "free"}`: true,
	}
	for body, want := range cases {
		got := classify(429, []byte(body), "", now)
		if got.kind != retryLater || got.quota != want {
			t.Errorf("%q: %+v", body, got)
		}
	}
	if classify(503, []byte(quotaBody), "", now).quota {
		t.Error("only a 429 is the quota stop")
	}
}
