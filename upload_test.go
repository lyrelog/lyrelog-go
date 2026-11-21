package lyrelog

import (
	"net/http"
	"testing"
	"time"
)

func TestClassify(t *testing.T) {
	now := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	cases := []struct {
		status     int
		body       string
		retryAfter string
		kind       outcomeKind
		wait       time.Duration
	}{
		{200, `{"accepted":3,"rejected":[]}`, "", delivered, 0},
		{200, `{"accepted":2,"rejected":[{"line":1,"error":"bad"}]}`, "", delivered, 0},
		{202, `{"accepted":1}`, "", delivered, 0},
		{200, `garbage`, "", retryLater, 0},
		{200, `{}`, "", retryLater, 0},
		{400, ``, "", dropBatch, 0},
		{413, ``, "", dropBatch, 0},
		{415, ``, "", dropBatch, 0},
		{404, ``, "", dropBatch, 0},
		{401, ``, "", unauthorized, 0},
		{403, ``, "", unauthorized, 0},
		{408, ``, "", retryLater, 0},
		{429, ``, "30", retryLater, 30 * time.Second},
		{429, ``, now.Add(90 * time.Second).Format(http.TimeFormat), retryLater, 90 * time.Second},
		{500, ``, "", retryLater, 0},
		{503, ``, "2.5", retryLater, 2500 * time.Millisecond},
		{599, ``, "", retryLater, 0},
	}
	for _, c := range cases {
		got := classify(c.status, []byte(c.body), c.retryAfter, now)
		if got.kind != c.kind || got.retryAfter != c.wait {
			t.Errorf("%d %q %q: got kind %d wait %v", c.status, c.body, c.retryAfter, got.kind, got.retryAfter)
		}
	}
	if got := classify(200, []byte(`{"accepted":2,"rejected":[{"line":1,"error":"bad"}]}`), "", now); got.accepted != 2 || len(got.rejected) != 1 {
		t.Errorf("%+v", got)
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	cases := map[string]time.Duration{
		"":                              0,
		"  7 ":                          7 * time.Second,
		"-3":                            0,
		"soon":                          0,
		"Wed, 18 Nov 2026 10:01:00 GMT": time.Minute,
		"Wed, 18 Nov 2026 09:00:00 GMT": 0, // in the past
	}
	for value, want := range cases {
		if got := parseRetryAfter(value, now); got != want {
			t.Errorf("%q: got %v, want %v", value, got, want)
		}
	}
}

func TestBackoff(t *testing.T) {
	top := func() float64 { return 0.999999 }
	bottom := func() float64 { return 0 }
	expected := []time.Duration{1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300}
	for attempt, ceiling := range expected {
		got := backoff(attempt, 0, top)
		if got > ceiling*time.Second || got < ceiling*time.Second-time.Millisecond {
			t.Errorf("attempt %d: got %v, ceiling %ds", attempt, got, ceiling)
		}
		if backoff(attempt, 0, bottom) != 0 {
			t.Error("full jitter starts at 0")
		}
	}
	if backoff(1000, 0, top) > 5*time.Minute {
		t.Error("capped at 5 min")
	}
	if got := backoff(0, 45*time.Second, top); got != 45*time.Second {
		t.Errorf("never sooner than Retry-After: %v", got)
	}
}
