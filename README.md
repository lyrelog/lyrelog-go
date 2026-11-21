# Lyrelog for Go

Structured logs from your Go services to [Lyrelog](https://lyrelog.com), the hosted log
service. Standard library only, Go 1.24+.

## Install

```bash
go get go.lyrelog.com/lyrelog@v0.9.0
```

The source is at [github.com/lyrelog/lyrelog-go](https://github.com/lyrelog/lyrelog-go); the
docs at [docs.lyrelog.com](https://docs.lyrelog.com).

## Use

```go
import "go.lyrelog.com/lyrelog"

client, err := lyrelog.New(lyrelog.Config{
    APIKey:   "lyr_ing_...",            // ingest-only: it can append events, never read them
    Platform: "api",                    // optional; "go" by default
    // Endpoint defaults to https://ingest.lyrelog.com
})
if err != nil {
    log.Fatal(err)                      // only for a bad config: logging itself never fails
}
defer client.Close()                    // sends what is left (waits at most 5 s)

client.Info("checkout_done", lyrelog.Category("checkout"), lyrelog.Attrs{"latency_ms": 412, "plan": "pro"})

// A Payload is encoded with encoding/json: pass a value, a map, or json.RawMessage for
// bytes that already are JSON (a plain []byte would arrive as a base64 string).
client.Error("charge_failed", lyrelog.Attrs{"code": 402}, lyrelog.Payload(json.RawMessage(responseBody)))

if err := charge(ctx, order); err != nil {
    client.CaptureError(err, lyrelog.User(accountID)) // "exception", with the wrapped chain and the stack
}

// One logger per area of the program.
billing := client.Logger("billing")
billing.Warning("invoice_retry", lyrelog.Attrs{"attempt": 2})

client.SetUser("u_123")                 // attached to every following event; "" clears it
err = client.Flush(ctx)                 // rarely needed: the client sends every 5 s and on Close
```

`Flush` returns nil when everything reached the server; `ctx.Err()`; `ErrClosed` after
`Close`; `ErrUnauthorized` once the key was refused; an error saying a batch was refused and
dropped; or an error saying delivery failed and will be retried (the events stay queued).
Inside a `Retry-After` the server sent, `Flush` returns that retry error at once without
sending. `Close` returns the same values for its last flush.

To bound the last flush by your own deadline, as with `http.Server.Shutdown`, call
`Shutdown(ctx)` instead of `Close()`: it waits until everything is sent or `ctx` is done, then
aborts a request still in flight and returns `ctx.Err()`. `Close()` is `Shutdown` with
`CloseTimeout`. After either, both return the first result.

```go
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()
_ = server.Shutdown(ctx)
_ = client.Shutdown(ctx)                // what's left of the same 2 s
```

- `message`: a stable event name (`checkout_done`), not prose.
- `Category`: the area of the program (`billing`, `auth`, `http`); the dashboard filters by it.
- `Attrs`: small fields to filter and chart by: ids, durations, codes, flags.
- `Payload`: the big stuff you only read when you open one event: responses, request bodies.
- `User`: the user of this one event, in place of the one from `SetUser`.

Levels: `Trace`, `Debug`, `Info`, `Notice`, `Warning`, `Error`, `Critical` (and `Log(level, ...)`;
a level outside that range is clamped to the nearest one).
A `nil` `*Client` does nothing, so code can log before (or without) one.

## log/slog

```go
logger := slog.New(lyrelog.NewSlogHandler(client, &lyrelog.SlogOptions{Category: "server"}))
logger.Info("request_done", "status", 200, slog.Group("req", "route", "/users/{id}"))
logger.Warn("card_declined", "category", "billing", "brand", "visa")   // category: billing
```

The record's message becomes the event's message and its time the event's time. Attrs and groups
become nested attrs (`req.route`). A top-level `category` attr sets the category, else the
handler's `Category` applies. Levels: below `slog.LevelDebug` → trace, debug, info, between info
and warn → notice, warn → warning, error, `slog.LevelError+4` and above → critical.
`SlogOptions.Level` adds a minimum of its own; `AddSource` adds a `source` attr with the file
path the binary was built with: build with `go build -trimpath` so it holds module paths, not
your build machine's directories. `NewSlogHandler` returns a `*lyrelog.SlogHandler` (a
`slog.Handler`). It passes `testing/slogtest`, except that a zero record time
is stamped with the current time (every Lyrelog event has one).

To send both to the terminal and to Lyrelog, wrap the two handlers in your own fan-out, or keep
`slog.Default()` for the terminal and use the Lyrelog logger for events.

## net/http

A middleware that records every request as `http_request_served` in the `http` area, with the
route pattern (not the path, so ids don't explode the cardinality), at info, warning for a 4xx
and error for a 5xx. `Request.Pattern` needs Go 1.23+ and an `http.ServeMux` route:

```go
func logRequests(client *lyrelog.Client, next http.Handler) http.Handler {
    log := client.Logger("http")
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        start := time.Now()
        recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
        next.ServeHTTP(recorder, r)
        level := lyrelog.LevelInfo
        switch {
        case recorder.status >= 500:
            level = lyrelog.LevelError
        case recorder.status >= 400:
            level = lyrelog.LevelWarning
        }
        log.Log(level, "http_request_served", lyrelog.Attrs{
            "method":      r.Method,
            "route":       r.Pattern, // set by http.ServeMux (Go 1.23+), e.g. "GET /users/{id}"
            "status":      recorder.status,
            "duration_ms": time.Since(start).Milliseconds(),
        })
    })
}

type statusRecorder struct {
    http.ResponseWriter
    status int
}

func (s *statusRecorder) WriteHeader(code int) {
    s.status = code
    s.ResponseWriter.WriteHeader(code)
}

// http.ListenAndServe(":8080", logRequests(client, mux))
```

## Configuration

| Field | Default | Meaning |
|---|---|---|
| `APIKey` | required | the project's ingest key `lyr_ing_...` (or a server key `lyr_srv_...`); surrounding spaces and a trailing newline, as from a file, are removed |
| `Endpoint` | `https://ingest.lyrelog.com` | the ingest's base URL; a pasted `/v1` or `/v1/events:batch` is removed; plain `http://` only for localhost |
| `Environment` | `$LYRELOG_ENVIRONMENT`, else `production` | any name matching `^[a-z0-9][a-z0-9_-]{0,31}$`; a bad `Environment` fails `New`, a bad `$LYRELOG_ENVIRONMENT` falls back to `production` (reported to `ErrorLog`) |
| `Platform` | `go` | which part of the product sent it (`api`, `worker`...), same rule |
| `Release` | the main module's version from the build info | `resource.app_version` |
| `AppID` | the main module's path | `resource.app_id` |
| `MinimumLevel` | `LevelDebug` | events below are dropped before queueing |
| `FlushInterval` | 5 s | how often the background goroutine sends; at least 1 s (`MinFlushInterval`), a smaller value is raised to it |
| `MaxQueue` | 10,000 events | past it the oldest are dropped (and counted) |
| `MaxQueueBytes` | 16 MiB | the same, by encoded size |
| `CloseTimeout` | 5 s | how long `Close` waits for the last flush (`Shutdown` takes a context instead) |
| `HTTPClient` | 30 s timeout | sends the batches; the SDK uses a copy that never follows redirects |
| `ErrorLog` | silent, except the first refused key or dropped batch (printed once), the quota warning and the failing-uploads warning, to stderr | a `*log.Logger` for the SDK's own diagnostics (each line starts with `lyrelog: `) |
| `RedactKeys` | `DefaultRedactKeys()` | field names whose values become `"[redacted]"` (below); replaces the list, an empty non-nil slice turns redaction off |

`New` returns an error only for a bad configuration. Don't point `ErrorLog` at a logger that
feeds the same client (a diagnostic about failed delivery can't be delivered by it); if you do
it through `slog.NewLogLogger` over the Lyrelog handler, or through `log.Default()` after
`slog.SetDefault`, the handler skips those lines (any message containing `lyrelog: `). Printing
a `Config` or a `*Client` with `fmt`, or a `Config` as JSON, redacts the API key. `fmt` can't
call methods on an unexported struct field, so a `Config` printed with `%+v` inside your own
struct's unexported field still shows the key: keep it out of such fields or print it alone.
Every event also carries `os_name` (`runtime.GOOS`), `sdk` (`lyrelog-go/0.9.0`), the VCS revision as `app_build` when the binary
has one, and an install id and session id, both per process.

## What it does for you

| | |
|---|---|
| Never blocks | A log call fits and encodes the event and appends it to an in-memory queue; one background goroutine sends. Safe for concurrent use. |
| Batches | Up to 500 events or 512 KiB per request, gzip. A full batch is sent right away. |
| When it sends | Every `FlushInterval`, on `Flush(ctx)` and on `Close()` or `Shutdown(ctx)`. |
| Retries | Exponential backoff with full jitter (1 s doubling to 5 min), honours `Retry-After` (seconds or a date, at most an hour) and resends the same bytes; the server collapses duplicates. |
| Bad batches | Lines the server rejects in a 2xx are never resent. A 400, 413 or other 4xx drops the batch (a 404 says to check `Endpoint`), and so does a 3xx (redirects are not followed: they would turn the POST into a GET). A 401/403 stops sending until the process restarts and keeps the queue. 408, 429, 5xx and network errors retry. When the organization's monthly events are used up (a 429 with a JSON `plan`), one warning goes to `ErrorLog` (or stderr), and again only after a batch has been accepted; the batch waits for the `Retry-After`, but that 429 is not a failed attempt: it adds nothing to the backoff. |
| Network down | After 3 failed uploads in a row (network errors, 408, 429 other than the quota, 5xx), one line goes to `ErrorLog` (or stderr), never to the client's own events, and nothing more until an upload succeeds. |
| Caps | `MaxQueue` events and `MaxQueueBytes` in memory, oldest dropped first; `Stats()` counts sent, rejected and dropped. `Close` discards what it couldn't send, counted as dropped. |
| Secrets | Redacted before queueing (below). |
| Limits | Fits every event to the server's limits before queueing (below). |

## Secrets

Before an event is queued, every field named like a secret has its value replaced by
`"[redacted]"`, in `attrs` and in the payload, at any depth: nested maps, slices of maps,
structs (by their JSON names), `http.Header`, a `json.RawMessage`. A name matches when,
lowercased and without `-`, `_`, `.` or spaces, it equals or ends with one of `authorization`,
`cookie`, `password`, `passwd`, `secret`, `token`, `apikey`, `privatekey`, `credential`,
`credentials`, `accesskey`, `signature` (the rule every Lyrelog SDK shares). So `X-Api-Key`,
`csrf_token`, `Set-Cookie` and `user_token` are redacted; `token_count` and `max_tokens` are
not. The whole value goes, even an object, and a secret's value is never evaluated (its
`String` is not called). Redaction comes before any limit: a long secret is never cut to a
prefix, and an oversized payload's preview never shows one.

To change the list, set `Config.RedactKeys` (normalised the same way):

```go
lyrelog.Config{APIKey: key, RedactKeys: append(lyrelog.DefaultRedactKeys(), "ssn", "card_number")}
lyrelog.Config{APIKey: key, RedactKeys: []string{}} // no redaction at all
```

Only names are checked: a secret inside a message, an error's text or a value under an
innocent name is sent as it is. A payload with a secret is re-encoded after redaction, so its
object keys come sorted.

## Limits

Applied before an event is queued, so the server never rejects a line for its shape:

- `message` up to 2,048 bytes (empty becomes `(empty)`), `category` 64, `user_id` 128, all cut on a character boundary.
- `attrs`: strings up to 1,024 bytes, string arrays up to 64 items (other items become strings),
  4 levels of nesting (a deeper object becomes its JSON string), 200 leaves and 256 KiB encoded:
  the rest are dropped and counted in `lyrelog_dropped_attrs`. Names lose dots (`http.status` → `http_status`)
  and leading underscores. Errors, times, durations and `Stringer`s become strings, structs their
  JSON form, NaN and infinities `null`. A typed-nil error or `Stringer` is `null`, a method that
  panics becomes `"<panic: ...>"`, and a cyclic value is cut (`"(too deep)"`): the event is
  still sent and the caller never panics.
- `payload` up to 64 KiB encoded; a bigger one is replaced by
  `{"lyrelog_truncated": true, "bytes": N, "preview": "<first 1 KiB>"}`, `N` its encoded size,
  unless it fits once its secrets are redacted. It is redacted as a stream, never decoded
  whole, so a huge payload costs its encoding and little more. One that can't be encoded
  becomes `{"encoding_error": "..."}`.
- An event that still can't fit in a 512 KiB batch is dropped and counted before it is queued.

## Tests

```bash
go test -race ./...
LYRELOG_E2E_URL=https://ingest.lyrelog.com LYRELOG_E2E_KEY=lyr_ing_... go test -run EndToEnd -v
```

The unit tests run every line the client makes through a copy of the ingest's parser
(`internal/serverevents`).

## Licence

MIT, Copyright (c) 2026 Lyrelog.
