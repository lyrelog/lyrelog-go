// Package lyrelog sends structured logs from a Go program to Lyrelog, the hosted log service
// (https://ingest.lyrelog.com by default).
//
//	client, err := lyrelog.New(lyrelog.Config{APIKey: "lyr_ing_..."})
//	if err != nil { ... }
//	defer client.Close()
//
//	client.Info("checkout_done", lyrelog.Category("checkout"), lyrelog.Attrs{"latency_ms": 412})
//
// Logging never blocks and never panics: the event is fitted to the server's limits, encoded
// and put in a bounded in-memory queue. One background goroutine sends it in gzip batches and
// retries with backoff. A Client is safe for concurrent use. The methods of a nil *Client do
// nothing, so a program can run without one.
package lyrelog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Version is this SDK's version, sent as resource.sdk ("lyrelog-go/1.0.0").
const Version = "1.0.0"

const sdkName = "lyrelog-go/" + Version

// Defaults.
const (
	DefaultEndpoint      = "https://ingest.lyrelog.com"
	DefaultFlushInterval = 5 * time.Second
	DefaultMaxQueue      = 10_000
	DefaultMaxQueueBytes = 16 << 20
	DefaultCloseTimeout  = 5 * time.Second
	// MinFlushInterval is the shortest FlushInterval: a smaller one is raised to it.
	MinFlushInterval = time.Second
	maxBatchEvents   = 500
	maxBatchBytes    = 512 * 1024
)

var (
	// ErrClosed is returned by Flush after Close.
	ErrClosed = errors.New("lyrelog: client is closed")
	// ErrUnauthorized means the server refused the key (401/403): the client stops sending
	// until the process restarts and keeps what it has queued.
	ErrUnauthorized = errors.New("lyrelog: the server refused the API key; sending stopped")
)

// Config is what New needs. Only APIKey is required.
type Config struct {
	// Endpoint is the ingest's base URL. Default DefaultEndpoint
	// ("https://ingest.lyrelog.com"). A pasted "/v1" or "/v1/events:batch" is removed. Plain
	// http:// is accepted only for localhost and loopback addresses.
	Endpoint string
	// APIKey is the project's ingest key (lyr_ing_...), or a server key (lyr_srv_...) on a
	// server you run. Surrounding spaces and newlines are removed.
	APIKey string
	// Environment: "production", "development", "staging" or any name matching
	// ^[a-z0-9][a-z0-9_-]{0,31}$. Default: $LYRELOG_ENVIRONMENT, else "production". An
	// invalid Environment makes New fail; an invalid $LYRELOG_ENVIRONMENT is reported to
	// ErrorLog and replaced by "production", so a typo in a deploy never stops a service.
	Environment string
	// Platform says which part of the product sent the event. Default "go"; override it
	// with "api", "worker"... (same naming rule as Environment).
	Platform string
	// Release goes to resource.app_version. Default: the main module's version from the
	// build info, when it has one.
	Release string
	// AppID goes to resource.app_id (a service name). Default: the main module's path.
	AppID string
	// MinimumLevel drops events below it before they are queued. Default LevelDebug.
	MinimumLevel Level
	// FlushInterval is how often the background goroutine sends. Default 5 s, at least
	// MinFlushInterval (1 s): a smaller value is raised to it.
	FlushInterval time.Duration
	// MaxQueue caps the events waiting to be sent; past it the oldest are dropped and
	// counted in Stats().Dropped. Default 10,000.
	MaxQueue int
	// MaxQueueBytes caps the encoded size of the events waiting to be sent; past it the
	// oldest are dropped and counted in Stats().Dropped. Default 16 MiB.
	MaxQueueBytes int
	// CloseTimeout bounds how long Close waits for the last flush. Default 5 s. Shutdown takes
	// its bound from its context instead.
	CloseTimeout time.Duration
	// HTTPClient sends the batches. Default: a client with a 30 s timeout. The SDK uses a
	// copy that never follows redirects (a 301/302 would turn the POST into a GET); a 3xx
	// drops the batch and is logged.
	HTTPClient *http.Client
	// ErrorLog receives the SDK's own diagnostics (rejected lines, dropped batches, a
	// refused key, the monthly quota), each starting with "lyrelog: " (the quota warning with
	// "Lyrelog: "). Don't point it at a logger that feeds this same client: a diagnostic about
	// failed delivery can't be delivered by it (SlogHandler skips lines with that prefix in any
	// case, so slog.NewLogLogger over it at least can't loop). Default: nothing is printed,
	// except the first refused key or dropped batch, which goes to os.Stderr once so a wrong
	// endpoint or key can't fail silently, and the quota warning (once per quota stop).
	ErrorLog *log.Logger
	// RedactKeys are the field names whose values are replaced by "[redacted]" before an event
	// is queued, in attrs and in the payload, at any depth (nested maps, slices, structs). A
	// name matches when the field's name, lowercased and without "-", "_", "." or spaces,
	// equals or ends with one of them (normalised the same way). nil means DefaultRedactKeys();
	// a non-nil empty slice turns redaction off.
	RedactKeys []string
}

// Stats counts what the client did since New.
type Stats struct {
	Queued   int    // waiting to be sent, including a batch being retried
	Sent     uint64 // accepted by the server
	Rejected uint64 // lines the server refused (never resent)
	Dropped  uint64 // dropped by the client: queue overflow, a refused batch, or left at Close
}

// Client logs to one Lyrelog project. Make it with New.
type Client struct {
	endpoint     string
	apiKey       string
	minimum      Level
	interval     time.Duration
	maxQueue     int
	maxBytes     int
	closeTimeout time.Duration
	http         *http.Client
	errorLog     *log.Logger
	stderr       io.Writer // where the first alarm goes without an ErrorLog
	alarmOnce    sync.Once
	resource     resource
	installID    string
	sessionID    string
	redact       []string // normalised names to redact (redactSet); empty means off

	user atomic.Pointer[string]

	mu         sync.Mutex
	queue      [][]byte
	queueBytes int    // the sum of len(line) over queue
	pending    *batch // a cut batch waiting for a retry: the same bytes are resent
	closed     atomic.Bool

	sent, rejected, dropped atomic.Uint64

	// Owned by the worker goroutine.
	attempt     int
	notBefore   time.Time // no timer round before it (backoff)
	serverWait  time.Time // no round at all before it, not even a Flush (Retry-After)
	retryErr    error     // why the last round is waiting, returned by a Flush inside serverWait
	disabled    bool
	quotaWarned bool // the monthly-quota warning was logged; cleared by an accepted batch
	downWarned  bool // the "uploads keep failing" warning was logged; cleared by an accepted batch

	wake    chan struct{}
	flushes chan chan error
	stop    chan struct{}
	done    chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc

	closeOnce sync.Once
	closeErr  error
}

var nameRule = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,31}$`)

// New checks the configuration, fills the defaults and starts the background goroutine.
// Call Close before the program exits so the last events are sent.
func New(cfg Config) (*Client, error) {
	endpoint, err := baseURL(cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	apiKey := strings.TrimSpace(cfg.APIKey) // a key read from a file ends with a newline
	if apiKey == "" {
		return nil, errors.New("lyrelog: Config.APIKey is required")
	}

	environment := cfg.Environment
	var environmentWarning string
	if environment == "" {
		environment = strings.ToLower(strings.TrimSpace(os.Getenv("LYRELOG_ENVIRONMENT")))
		if environment != "" && !nameRule.MatchString(environment) {
			environmentWarning = fmt.Sprintf("lyrelog: LYRELOG_ENVIRONMENT %q must be lowercase letters, digits, - and _, up to 32; using production", environment)
			environment = ""
		}
	}
	if environment == "" {
		environment = "production"
	}
	if !nameRule.MatchString(environment) {
		return nil, fmt.Errorf("lyrelog: Config.Environment %q must be lowercase letters, digits, - and _, up to 32", environment)
	}
	platform := cfg.Platform
	if platform == "" {
		platform = "go"
	}
	if !nameRule.MatchString(platform) {
		return nil, fmt.Errorf("lyrelog: Config.Platform %q must be lowercase letters, digits, - and _, up to 32", platform)
	}
	if cfg.MinimumLevel != 0 && !cfg.MinimumLevel.valid() {
		return nil, fmt.Errorf("lyrelog: Config.MinimumLevel %d is not a level", cfg.MinimumLevel)
	}
	if cfg.FlushInterval < 0 || cfg.MaxQueue < 0 || cfg.MaxQueueBytes < 0 || cfg.CloseTimeout < 0 {
		return nil, errors.New("lyrelog: FlushInterval, MaxQueue, MaxQueueBytes and CloseTimeout can't be negative")
	}

	c := &Client{
		endpoint:     endpoint + ingestPath,
		apiKey:       apiKey,
		minimum:      orDefault(cfg.MinimumLevel, LevelDebug),
		interval:     max(orDefault(cfg.FlushInterval, DefaultFlushInterval), MinFlushInterval),
		maxQueue:     orDefault(cfg.MaxQueue, DefaultMaxQueue),
		maxBytes:     orDefault(cfg.MaxQueueBytes, DefaultMaxQueueBytes),
		closeTimeout: orDefault(cfg.CloseTimeout, DefaultCloseTimeout),
		http:         cfg.HTTPClient,
		errorLog:     cfg.ErrorLog,
		stderr:       os.Stderr,
		installID:    newUUID(),
		sessionID:    newUUID(),
		redact:       redactSet(cfg.RedactKeys),
		wake:         make(chan struct{}, 1),
		flushes:      make(chan chan error),
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}
	if c.http == nil {
		c.http = &http.Client{Timeout: 30 * time.Second}
	} else {
		own := *c.http // a copy: the caller's client keeps its own redirect policy
		c.http = &own
	}
	c.http.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if environmentWarning != "" {
		c.logf("%s", environmentWarning)
	}
	c.resource = detectResource(cfg, environment, platform)
	c.ctx, c.cancel = context.WithCancel(context.Background())
	go c.run()
	return c, nil
}

const ingestPath = "/v1/events:batch"

// baseURL checks an endpoint and returns it without a trailing slash or a pasted API path.
func baseURL(raw string) (string, error) {
	endpoint := strings.TrimSpace(raw)
	if endpoint == "" {
		endpoint = DefaultEndpoint
	}
	endpoint = strings.TrimRight(endpoint, "/")
	for _, suffix := range []string{ingestPath, "/v1"} {
		if strings.HasSuffix(endpoint, suffix) {
			endpoint = strings.TrimRight(strings.TrimSuffix(endpoint, suffix), "/")
			break
		}
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return "", fmt.Errorf("lyrelog: Config.Endpoint must be an https URL, got %q", raw)
	}
	if parsed.Scheme == "http" && !isLoopback(parsed.Hostname()) {
		return "", fmt.Errorf("lyrelog: Config.Endpoint %q must use https (plain http only for localhost): the API key would travel in clear", raw)
	}
	return endpoint, nil
}

func isLoopback(host string) bool {
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func orDefault[T comparable](value, fallback T) T {
	var zero T
	if value == zero {
		return fallback
	}
	return value
}

func detectResource(cfg Config, environment, platform string) resource {
	r := resource{
		AppID:       cfg.AppID,
		AppVersion:  cfg.Release,
		OSName:      runtime.GOOS,
		SDK:         sdkName,
		Environment: environment,
		Platform:    platform,
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		if r.AppID == "" {
			r.AppID = info.Main.Path
		}
		if r.AppVersion == "" && info.Main.Version != "(devel)" {
			r.AppVersion = info.Main.Version
		}
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && len(setting.Value) >= 12 {
				r.AppBuild = setting.Value[:12]
			}
		}
	}
	r.AppID = truncate(r.AppID, maxResourceBytes)
	r.AppVersion = truncate(r.AppVersion, maxResourceBytes)
	return r
}

// SetUser attaches your user or account id to every following event; "" clears it.
func (c *Client) SetUser(id string) {
	if c == nil {
		return
	}
	if id == "" {
		c.user.Store(nil)
		return
	}
	c.user.Store(&id)
}

// Logger returns a logger whose events carry category (the area of the program,
// e.g. "billing"). A Category option on one event still wins.
func (c *Client) Logger(category string) *Logger {
	return &Logger{client: c, category: category}
}

// Log records an event at level. message is a stable event name ("checkout_done").
func (c *Client) Log(level Level, message string, opts ...Option) {
	c.record(time.Time{}, level, "", message, opts)
}

// Trace records an event at LevelTrace.
func (c *Client) Trace(message string, opts ...Option) { c.Log(LevelTrace, message, opts...) }

// Debug records an event at LevelDebug.
func (c *Client) Debug(message string, opts ...Option) { c.Log(LevelDebug, message, opts...) }

// Info records an event at LevelInfo.
func (c *Client) Info(message string, opts ...Option) { c.Log(LevelInfo, message, opts...) }

// Notice records an event at LevelNotice.
func (c *Client) Notice(message string, opts ...Option) { c.Log(LevelNotice, message, opts...) }

// Warning records an event at LevelWarning.
func (c *Client) Warning(message string, opts ...Option) { c.Log(LevelWarning, message, opts...) }

// Error records an event at LevelError.
func (c *Client) Error(message string, opts ...Option) { c.Log(LevelError, message, opts...) }

// Critical records an event at LevelCritical.
func (c *Client) Critical(message string, opts ...Option) { c.Log(LevelCritical, message, opts...) }

// CaptureError records err as an "exception" event (the same name in every Lyrelog SDK) at level error: attrs error_type and
// error_message, and a payload with the chain of wrapped errors and the stack of the calling
// goroutine. Options add attrs, a category or a user (a Payload option replaces the payload).
// A nil err does nothing.
func (c *Client) CaptureError(err error, opts ...Option) {
	c.captureError("", err, opts)
}

func (c *Client) captureError(category string, err error, opts []Option) {
	if c == nil || err == nil || LevelError < c.minimum {
		return
	}
	// err's methods are the caller's code: a typed nil (var e *MyErr) or a panicking Error
	// or Unwrap must not reach the caller, so errorMessage and errorChain guard every call.
	base := []Option{
		Attrs{"error_type": fmt.Sprintf("%T", err), "error_message": errorMessage(err)},
		Payload(map[string]any{"chain": errorChain(err), "stack": string(debug.Stack())}),
	}
	c.record(time.Time{}, LevelError, category, "exception", append(base, opts...))
}

func errorChain(err error) []map[string]string {
	var chain []map[string]string
	queue := []error{err}
	for len(queue) > 0 && len(chain) < 32 {
		current := queue[0]
		queue = queue[1:]
		if current == nil {
			continue
		}
		chain = append(chain, map[string]string{"type": fmt.Sprintf("%T", current), "message": errorMessage(current)})
		queue = append(queue, unwrap(current)...)
	}
	return chain
}

// errorMessage is err.Error(), or "<nil>" for a typed nil, or "<panic: ...>".
func errorMessage(err error) string {
	if isNil(err) {
		return "<nil>"
	}
	return safeCall(err.Error)
}

// unwrap is the errors err wraps; none for a typed nil or an Unwrap that panics.
func unwrap(err error) (wrapped []error) {
	if isNil(err) {
		return nil
	}
	defer func() {
		if recover() != nil {
			wrapped = nil
		}
	}()
	switch e := err.(type) {
	case interface{ Unwrap() []error }:
		return e.Unwrap()
	case interface{ Unwrap() error }:
		return []error{e.Unwrap()}
	}
	return nil
}

// record shapes, encodes and queues one event. It never blocks on I/O.
func (c *Client) record(ts time.Time, level Level, category, message string, opts []Option) {
	level = level.clamp()
	if c == nil || c.closed.Load() || level < c.minimum {
		return
	}
	var o eventOptions
	if category != "" {
		o.category = category
	}
	for _, opt := range opts {
		if opt != nil {
			opt.apply(&o)
		}
	}
	user := ""
	if o.userSet {
		user = o.user
	} else if stored := c.user.Load(); stored != nil {
		user = *stored
	}
	if ts.IsZero() {
		ts = time.Now()
	}
	line, err := safeEncode(event{
		ts: ts, level: level, category: o.category, message: message,
		user: user, installID: c.installID, sessionID: c.sessionID,
		attrs: o.attrs, payload: o.payload, hasPayload: o.payloadSet, redact: c.redact,
	}, &c.resource)
	if err != nil {
		c.logf("lyrelog: event %q could not be encoded: %v", message, err)
		return
	}
	c.enqueue(line)
}

// safeEncode keeps a misbehaving value (a MarshalJSON or String that panics) from
// reaching the caller.
func safeEncode(e event, r *resource) (line []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			line, err = nil, fmt.Errorf("panic while encoding: %v", recovered)
		}
	}()
	return encodeEvent(e, r)
}

func (c *Client) enqueue(line []byte) {
	// A line that can't fit in a batch would be sent alone and refused (413) forever.
	if len(line)+1 > maxBatchBytes {
		c.dropped.Add(1)
		c.logf("lyrelog: an event of %d bytes is too large for a batch; dropped", len(line))
		return
	}
	c.mu.Lock()
	if c.closed.Load() { // Close has started: it will not send this one
		c.mu.Unlock()
		return
	}
	c.queue = append(c.queue, line)
	c.queueBytes += len(line)
	over := 0
	for over < len(c.queue)-1 && (len(c.queue)-over > c.maxQueue || c.queueBytes > c.maxBytes) {
		c.queueBytes -= len(c.queue[over])
		over++
	}
	if over > 0 {
		clear(c.queue[:over])
		c.queue = c.queue[over:]
		c.dropped.Add(uint64(over))
	}
	full := len(c.queue) >= maxBatchEvents
	c.mu.Unlock()
	if full {
		select {
		case c.wake <- struct{}{}:
		default:
		}
	}
}

// Stats reports what the client has done so far.
func (c *Client) Stats() Stats {
	if c == nil {
		return Stats{}
	}
	c.mu.Lock()
	queued := len(c.queue)
	if c.pending != nil {
		queued += c.pending.events
	}
	c.mu.Unlock()
	return Stats{Queued: queued, Sent: c.sent.Load(), Rejected: c.rejected.Load(), Dropped: c.dropped.Load()}
}

// Flush sends everything queued before the call, ignoring the client's own retry backoff for
// one attempt, and waits for it or for ctx. A Retry-After the server sent is not ignored:
// inside it Flush returns at once with the retry error, sending nothing.
//
// It returns nil when everything reached the server; ctx's error; ErrClosed after Close;
// ErrUnauthorized once the key was refused; an error saying a batch was refused and dropped
// (the rest was still sent); or an error saying delivery failed and will be retried, with
// the events kept in the queue.
func (c *Client) Flush(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if c.closed.Load() {
		return ErrClosed
	}
	return c.flush(ctx)
}

func (c *Client) flush(ctx context.Context) error {
	reply := make(chan error, 1)
	select {
	case c.flushes <- reply:
	case <-c.done:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops accepting events, flushes what is queued (waiting at most CloseTimeout) and
// stops the background goroutine. Events still unsent are discarded and counted as dropped.
// It is Shutdown with a context that ends after CloseTimeout. Calling Close or Shutdown again
// returns the first result.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.closeTimeout)
	defer cancel()
	return c.Shutdown(ctx)
}

// Shutdown stops accepting events, sends what is queued and stops the background goroutine,
// waiting until the last flush ends or ctx is done, whichever comes first (as
// http.Server.Shutdown does). A request still in flight when ctx ends is aborted; events still
// unsent are discarded and counted as dropped. It returns what the last flush returned (see
// Flush), ctx's error if ctx ended first, or the first result when the client was already
// closed by Close or Shutdown.
func (c *Client) Shutdown(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.mu.Lock() // under mu, so no enqueue can slip in between the check and the flush
		c.closed.Store(true)
		c.mu.Unlock()
		c.closeErr = c.flush(ctx)
		c.cancel() // abort a request still in flight
		close(c.stop)
		<-c.done
		c.mu.Lock()
		left := len(c.queue)
		if c.pending != nil {
			left += c.pending.events
		}
		c.queue, c.queueBytes, c.pending = nil, 0, nil
		c.mu.Unlock()
		if left > 0 {
			c.dropped.Add(uint64(left))
			c.logf("lyrelog: closed with %d events unsent", left)
		}
	})
	return c.closeErr
}

func (c *Client) logf(format string, args ...any) {
	if c.errorLog != nil {
		c.errorLog.Printf(format, args...)
	}
}

// alarm is logf for what means nothing will arrive (a refused key, a dropped batch): without
// an ErrorLog the first one still goes to stderr, once.
func (c *Client) alarm(format string, args ...any) {
	if c.errorLog != nil {
		c.errorLog.Printf(format, args...)
		return
	}
	c.alarmOnce.Do(func() {
		fmt.Fprintf(c.stderr, format+" (set Config.ErrorLog to see every SDK diagnostic)\n", args...)
	})
}

// warnQuota tells the developer once that the organization's monthly quota is used up.
// round calls it only on the first quota 429 since the last accepted batch. Without an
// ErrorLog it goes to stderr, without using up alarm's one line.
func (c *Client) warnQuota(serverText string) {
	c.warn(fmt.Sprintf("Lyrelog: %s. Events are kept and sent when sending resumes; how many wait depends on the platform (here: in memory, up to %d events and %s, oldest dropped first; lost when the process ends).",
		serverText, c.maxQueue, formatBytes(c.maxBytes)))
}

// failuresBeforeWarning is how many failed uploads in a row (network errors, 408, 429 other
// than the quota, 5xx) make warnDown speak.
const failuresBeforeWarning = 3

// warnDown tells the developer once that uploads keep failing. round calls it after
// failuresBeforeWarning failed attempts in a row, and again only after a batch was accepted.
func (c *Client) warnDown(cause error) {
	c.warn(fmt.Sprintf("lyrelog: %d uploads in a row failed (last: %v); events are kept in memory (up to %d events and %s) and retried with backoff. Not repeated until an upload succeeds",
		failuresBeforeWarning, cause, c.maxQueue, formatBytes(c.maxBytes)))
}

// warn writes one line to ErrorLog, or to stderr without one, never to the client's own
// events. It doesn't use up alarm's one stderr line.
func (c *Client) warn(text string) {
	if c.errorLog != nil {
		c.errorLog.Print(text)
		return
	}
	fmt.Fprintln(c.stderr, text)
}

func formatBytes(n int) string {
	switch {
	case n >= 1<<20 && n%(1<<20) == 0:
		return fmt.Sprintf("%d MiB", n>>20)
	case n >= 1<<10 && n%(1<<10) == 0:
		return fmt.Sprintf("%d KiB", n>>10)
	default:
		return fmt.Sprintf("%d bytes", n)
	}
}

// Logger is a Client bound to one category. Make it with Client.Logger.
type Logger struct {
	client   *Client
	category string
}

// Log records an event at level with the logger's category.
func (l *Logger) Log(level Level, message string, opts ...Option) {
	l.client.record(time.Time{}, level, l.category, message, opts)
}

// Trace records an event at LevelTrace.
func (l *Logger) Trace(message string, opts ...Option) { l.Log(LevelTrace, message, opts...) }

// Debug records an event at LevelDebug.
func (l *Logger) Debug(message string, opts ...Option) { l.Log(LevelDebug, message, opts...) }

// Info records an event at LevelInfo.
func (l *Logger) Info(message string, opts ...Option) { l.Log(LevelInfo, message, opts...) }

// Notice records an event at LevelNotice.
func (l *Logger) Notice(message string, opts ...Option) { l.Log(LevelNotice, message, opts...) }

// Warning records an event at LevelWarning.
func (l *Logger) Warning(message string, opts ...Option) { l.Log(LevelWarning, message, opts...) }

// Error records an event at LevelError.
func (l *Logger) Error(message string, opts ...Option) { l.Log(LevelError, message, opts...) }

// Critical records an event at LevelCritical.
func (l *Logger) Critical(message string, opts ...Option) { l.Log(LevelCritical, message, opts...) }

// CaptureError is Client.CaptureError with this logger's category.
func (l *Logger) CaptureError(err error, opts ...Option) {
	l.client.captureError(l.category, err, opts)
}
