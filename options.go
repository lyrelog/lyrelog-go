package lyrelog

import "fmt"

// Level is an event's severity. The zero value means "not set" (Config.MinimumLevel then
// defaults to LevelDebug).
type Level int

const (
	LevelTrace Level = iota + 1
	LevelDebug
	LevelInfo
	LevelNotice
	LevelWarning
	LevelError
	LevelCritical
)

var levelNames = [...]string{"", "trace", "debug", "info", "notice", "warning", "error", "critical"}

func (l Level) valid() bool { return l >= LevelTrace && l <= LevelCritical }

// clamp maps an out-of-range level to the nearest valid one (below trace is trace, above
// critical is critical), so the server never sees a level it would reject.
func (l Level) clamp() Level {
	return min(max(l, LevelTrace), LevelCritical)
}

// String is the wire name: "trace" ... "critical".
func (l Level) String() string {
	if l.valid() {
		return levelNames[l]
	}
	return fmt.Sprintf("Level(%d)", int(l))
}

// Option adds detail to one event: Category, Attrs, Payload or User.
type Option interface{ apply(*eventOptions) }

type eventOptions struct {
	category   string
	attrs      map[string]any
	payload    any
	payloadSet bool
	user       string
	userSet    bool
}

// Category is the area of the program ("billing", "auth"); the dashboard filters and counts
// by it. Up to 64 bytes.
type Category string

func (c Category) apply(o *eventOptions) { o.category = string(c) }

// Attrs are small fields to filter and chart by: strings, numbers, booleans, nil, string
// slices and nested maps (4 levels). Names with dots are rewritten with "_". Several Attrs
// options merge; a later key wins. Fields named like a secret are redacted (Config.RedactKeys).
//
//	client.Info("checkout_done", lyrelog.Attrs{"latency_ms": 412, "plan": "pro"})
type Attrs map[string]any

func (a Attrs) apply(o *eventOptions) {
	if len(a) == 0 {
		return
	}
	if o.attrs == nil {
		o.attrs = make(map[string]any, len(a))
	}
	for key, value := range a {
		o.attrs[key] = value
	}
}

// User sets the user id of this one event, replacing the one from SetUser ("" sends none).
type User string

func (u User) apply(o *eventOptions) { o.user, o.userSet = string(u), true }

type payloadOption struct{ value any }

func (p payloadOption) apply(o *eventOptions) { o.payload, o.payloadSet = p.value, true }

// Payload is any JSON-encodable value for detail you only read when you open one event
// (a response, a request body). Up to 64 KiB encoded; bigger ones are replaced by
// {"lyrelog_truncated": true, "bytes": N, "preview": "<first 1 KiB>"}. It is encoded when the event is logged.
// Fields named like a secret are redacted first, at any depth (Config.RedactKeys).
func Payload(value any) Option { return payloadOption{value} }
