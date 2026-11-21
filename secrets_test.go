package lyrelog

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"testing"
)

// The shared rule (lyrelog docs/sdk-spec.md, rule 9; the Python SDK's _prepare.py): a field whose
// name, lowercased and without "-", "_", "." or spaces, is one of the redact keys or ends with
// one has its value replaced by "[redacted]" before the event is queued.

func TestSecretNames(t *testing.T) {
	keys := redactSet(nil)
	secret := []string{
		"password", "PASSWORD", "X-Api-Key", "api_key", "apiKey", "csrf_token", "Set-Cookie", "cookie",
		"user_token", "Authorization", "proxy-authorization", "client_secret", "passwd", "Private Key",
		"aws.access_key", "Credentials", "credential", "x-signature", "refresh token",
	}
	plain := []string{
		"token_count", "max_tokens", "tokens", "author", "keyboard", "cookie_consent", "signatures",
		"secret_santa", "passwords_reset_at", "plan", "",
	}
	for _, name := range secret {
		if !isSecret(name, keys) {
			t.Errorf("%q should be redacted", name)
		}
	}
	for _, name := range plain {
		if isSecret(name, keys) {
			t.Errorf("%q should not be redacted", name)
		}
	}
}

func TestDefaultRedactKeys(t *testing.T) {
	want := []string{"authorization", "cookie", "password", "passwd", "secret", "token", "apikey",
		"privatekey", "credential", "credentials", "accesskey", "signature"}
	got := DefaultRedactKeys()
	if !slices.Equal(got, want) {
		t.Fatalf("got %v", got)
	}
	got[0] = "changed" // a copy: the defaults can't be changed by accident
	if DefaultRedactKeys()[0] != "authorization" {
		t.Fatal("DefaultRedactKeys returned the shared slice")
	}
}

func TestAttrsSecretsAreRedacted(t *testing.T) {
	got := line(t, event{message: "x", attrs: map[string]any{
		"password": "hunter2", "authorization": "Bearer abc", "X-Api-Key": "sk_live", "plan": "pro",
		"token_count": 12, "max_tokens": 4096,
	}})
	attrs := got["attrs"].(map[string]any)
	for _, name := range []string{"password", "authorization", "X-Api-Key"} {
		if attrs[name] != redacted {
			t.Errorf("%s: %v", name, attrs[name])
		}
	}
	if attrs["plan"] != "pro" || attrs["token_count"] != json.Number("12") || attrs["max_tokens"] != json.Number("4096") {
		t.Errorf("plain attrs changed: %v", attrs)
	}
}

type loginForm struct {
	User     string `json:"user"`
	Password string `json:"password"`
	Inner    struct {
		ClientSecret string `json:"client_secret"`
		Region       string `json:"region"`
	} `json:"inner"`
}

func TestNestedAttrsAreRedactedAtAnyDepth(t *testing.T) {
	form := loginForm{User: "ana", Password: "hunter2"}
	form.Inner.ClientSecret, form.Inner.Region = "cs_live", "eu"
	deep := map[string]any{"a": map[string]any{"b": map[string]any{"c": map[string]any{"d": map[string]any{"api_key": "sk_deep", "ok": 1}}}}}
	got := line(t, event{message: "x", attrs: map[string]any{
		"req":    map[string]any{"headers": map[string]any{"Authorization": "Bearer abc", "Accept": "json"}},
		"form":   form,
		"items":  []any{map[string]any{"token": "t1", "id": 1}},
		"header": http.Header{"Cookie": {"session=abc"}, "Accept": {"json"}},
		"deep":   deep,
		"auth":   map[string]any{"secret": map[string]any{"nested": "whole map hidden"}},
	}})
	data, _ := json.Marshal(got)
	for _, leaked := range []string{"Bearer abc", "hunter2", "cs_live", "t1", "session=abc", "sk_deep", "whole map hidden"} {
		if strings.Contains(string(data), leaked) {
			t.Errorf("%q leaked: %s", leaked, data)
		}
	}
	attrs := got["attrs"].(map[string]any)
	headers := attrs["req"].(map[string]any)["headers"].(map[string]any)
	if headers["Authorization"] != redacted || headers["Accept"] != "json" {
		t.Errorf("headers %v", headers)
	}
	inner := attrs["form"].(map[string]any)["inner"].(map[string]any)
	if inner["client_secret"] != redacted || inner["region"] != "eu" || attrs["form"].(map[string]any)["user"] != "ana" {
		t.Errorf("form %v", attrs["form"])
	}
	if attrs["auth"].(map[string]any)["secret"] != redacted {
		t.Errorf("auth %v", attrs["auth"])
	}
	// Past the depth limit and inside arrays, values become JSON strings: still redacted.
	if !strings.Contains(string(data), `\"api_key\":\"[redacted]\"`) || !strings.Contains(string(data), `\"token\":\"[redacted]\"`) {
		t.Errorf("stringified values not redacted: %s", data)
	}
}

func TestPayloadSecretsAreRedacted(t *testing.T) {
	form := loginForm{User: "ana", Password: "hunter2"}
	form.Inner.ClientSecret = "cs_live"
	got := line(t, event{message: "x", hasPayload: true, payload: map[string]any{
		"api_key": "sk_live",
		"request": map[string]any{"headers": http.Header{"Authorization": {"Bearer abc"}, "Set-Cookie": {"a=b"}}},
		"users":   []any{map[string]any{"name": "ana", "access_key": "AKIA"}, []any{map[string]any{"csrf_token": "c"}}},
		"form":    form,
		"usage":   map[string]any{"token_count": 3, "max_tokens": 9},
	}})
	data, _ := json.Marshal(got["payload"])
	for _, leaked := range []string{"sk_live", "Bearer abc", "a=b", "AKIA", `"c"`, "hunter2", "cs_live"} {
		if strings.Contains(string(data), leaked) {
			t.Errorf("%q leaked: %s", leaked, data)
		}
	}
	payload := got["payload"].(map[string]any)
	if payload["api_key"] != redacted {
		t.Errorf("api_key %v", payload["api_key"])
	}
	usage := payload["usage"].(map[string]any)
	if usage["token_count"] != json.Number("3") || usage["max_tokens"] != json.Number("9") {
		t.Errorf("usage %v", usage)
	}
	if payload["users"].([]any)[0].(map[string]any)["name"] != "ana" {
		t.Errorf("users %v", payload["users"])
	}
}

func TestRawJSONPayloadIsRedacted(t *testing.T) {
	got := line(t, event{message: "x", hasPayload: true, payload: json.RawMessage(`{"password":"hunter2","n":1}`)})
	payload := got["payload"].(map[string]any)
	if payload["password"] != redacted || payload["n"] != json.Number("1") {
		t.Fatalf("%v", payload)
	}
}

func TestRedactionHappensBeforeTruncation(t *testing.T) {
	// A long secret is replaced whole, never cut to a prefix.
	long := strings.Repeat("s", 5000)
	got := line(t, event{message: "x", attrs: map[string]any{"token": long}})
	if got["attrs"].(map[string]any)["token"] != redacted {
		t.Fatal("long secret attr was not redacted")
	}

	// An oversized payload's preview never shows a secret.
	big := line(t, event{message: "x", hasPayload: true, payload: map[string]any{
		"a_password": "hunter2", "body": strings.Repeat("x", 100*1024)}})
	summary := big["payload"].(map[string]any)
	if summary["lyrelog_truncated"] != true {
		t.Fatalf("expected a summary: %v", summary)
	}
	preview := summary["preview"].(string)
	if strings.Contains(preview, "hunter2") || !strings.Contains(preview, `"a_password":"[redacted]"`) {
		t.Fatalf("preview %s", preview)
	}

	// A payload that is only too big because of a secret fits once it is redacted.
	fits := line(t, event{message: "x", hasPayload: true, payload: map[string]any{
		"secret": strings.Repeat("k", 100*1024), "id": "p1"}})
	payload := fits["payload"].(map[string]any)
	if payload["secret"] != redacted || payload["id"] != "p1" {
		t.Fatalf("payload %v", payload)
	}
}

func TestRedactKeysOption(t *testing.T) {
	f := newFakeServer(t)
	defaults := newTestClient(t, f)
	replaced := newTestClient(t, f, func(c *Config) { c.RedactKeys = []string{"Card-Number", "ssn"} })
	off := newTestClient(t, f, func(c *Config) { c.RedactKeys = []string{} })

	for _, client := range []*Client{defaults, replaced, off} {
		client.Info("e", Attrs{"password": "hunter2", "billing.card_number": "4242", "ssn": "123"},
			Payload(map[string]any{"password": "hunter2", "customer_ssn": "123"}))
		if err := client.Flush(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	batches := f.received()
	if len(batches) != 3 {
		t.Fatalf("%d batches", len(batches))
	}
	check := func(name string, line map[string]any, want map[string]any, wantPayload map[string]any) {
		t.Helper()
		attrs := line["attrs"].(map[string]any)
		for key, value := range want {
			if attrs[key] != value {
				t.Errorf("%s: attrs[%s] = %v, want %v", name, key, attrs[key], value)
			}
		}
		payload := line["payload"].(map[string]any)
		for key, value := range wantPayload {
			if payload[key] != value {
				t.Errorf("%s: payload[%s] = %v, want %v", name, key, payload[key], value)
			}
		}
	}
	check("defaults", batches[0].lines[0],
		map[string]any{"password": redacted, "billing_card_number": "4242", "ssn": "123"},
		map[string]any{"password": redacted, "customer_ssn": "123"})
	check("replaced", batches[1].lines[0],
		map[string]any{"password": "hunter2", "billing_card_number": redacted, "ssn": redacted},
		map[string]any{"password": "hunter2", "customer_ssn": redacted})
	check("off", batches[2].lines[0],
		map[string]any{"password": "hunter2", "billing_card_number": "4242", "ssn": "123"},
		map[string]any{"password": "hunter2", "customer_ssn": "123"})
}

func TestCaptureErrorAndSlogAreRedacted(t *testing.T) {
	f := newFakeServer(t)
	client := newTestClient(t, f)
	client.CaptureError(errors.New("boom"), Attrs{"authorization": "Bearer abc"})
	slog.New(NewSlogHandler(client, nil)).Info("login", "password", "hunter2",
		slog.Group("req", "X-Api-Key", "sk_live", "route", "/login"))
	if err := client.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	lines := f.received()[0].lines
	if len(lines) != 2 {
		t.Fatalf("%d lines", len(lines))
	}
	if lines[0]["attrs"].(map[string]any)["authorization"] != redacted {
		t.Errorf("capture %v", lines[0]["attrs"])
	}
	attrs := lines[1]["attrs"].(map[string]any)
	req := attrs["req"].(map[string]any)
	if attrs["password"] != redacted || req["X-Api-Key"] != redacted || req["route"] != "/login" {
		t.Errorf("slog %v", attrs)
	}
	if s := client.Stats(); s.Rejected != 0 || s.Sent != 2 {
		t.Errorf("%+v", s)
	}
}

// A secret's value is never looked at: its String method can't panic into the event.
type panickyStringer struct{}

func (panickyStringer) String() string { panic("never called") }

func TestSecretValueIsNotEvaluated(t *testing.T) {
	got := line(t, event{message: "x", attrs: map[string]any{"token": panickyStringer{}}})
	if got["attrs"].(map[string]any)["token"] != redacted {
		t.Fatalf("%v", got["attrs"])
	}
}
