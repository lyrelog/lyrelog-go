package lyrelog

import (
	"context"
	"log/slog"
	"runtime"
	"slices"
	"strings"
)

// diagnosticPrefix starts every line the SDK writes to Config.ErrorLog.
const diagnosticPrefix = "lyrelog: "

// SlogOptions configures NewSlogHandler. The zero value (or nil) is fine.
type SlogOptions struct {
	// Category for records without a "category" attribute. Default: none.
	Category string
	// Level is the handler's minimum, on top of the client's MinimumLevel. Default: none
	// (only the client's minimum applies).
	Level slog.Leveler
	// AddSource adds a "source" attr with the caller's function, file and line. The file is
	// the path the binary was built with: build with -trimpath to send module-relative paths
	// instead of the build machine's (which may include a user name).
	AddSource bool
}

// SlogHandler sends log/slog records to a Client. Make it with NewSlogHandler. It implements
// slog.Handler; WithAttrs and WithGroup return a *SlogHandler too.
type SlogHandler struct {
	client *Client
	opts   SlogOptions
	attrs  []groupedAttr // from WithAttrs, each under the groups open at the time
	groups []string
}

type groupedAttr struct {
	groups []string
	attr   slog.Attr
}

// NewSlogHandler returns a handler that logs through client.
//
// Levels map as slog.LevelDebug-4 and below → trace, Debug → debug, Info → info, between Info
// and Warn → notice, Warn → warning, Error → error, slog.LevelError+4 and above → critical.
// The record's message is the event's message, its time the event's time, and its attrs and
// groups become nested attrs. A top-level "category" string attr sets the category (and is
// not sent as an attr); without one the handler's Category is used.
//
// Records whose message contains "lyrelog: " or "Lyrelog: " (the quota warning) are the SDK's
// own diagnostics and are skipped, also after a log.Logger's date, file or prefix, so pointing
// Config.ErrorLog at slog.NewLogLogger(handler, ...), or at log.Default() once
// slog.SetDefault has sent the log package to this handler, can't feed them back as events.
// Still, prefer an ErrorLog that writes elsewhere (stderr): diagnostics about a failing
// delivery can't be delivered by the same client.
func NewSlogHandler(client *Client, opts *SlogOptions) *SlogHandler {
	h := &SlogHandler{client: client}
	if opts != nil {
		h.opts = *opts
	}
	return h
}

// SlogLevel maps a slog level to a Lyrelog level.
func SlogLevel(level slog.Level) Level {
	switch {
	case level < slog.LevelDebug:
		return LevelTrace
	case level < slog.LevelInfo:
		return LevelDebug
	case level == slog.LevelInfo:
		return LevelInfo
	case level < slog.LevelWarn:
		return LevelNotice
	case level < slog.LevelError:
		return LevelWarning
	case level < slog.LevelError+4:
		return LevelError
	default:
		return LevelCritical
	}
}

// Enabled reports whether a record at level would be sent: false after Close, below
// SlogOptions.Level or below the client's MinimumLevel.
func (h *SlogHandler) Enabled(_ context.Context, level slog.Level) bool {
	if h.client == nil || h.client.closed.Load() {
		return false
	}
	if h.opts.Level != nil && level < h.opts.Level.Level() {
		return false
	}
	return SlogLevel(level) >= h.client.minimum
}

// Handle turns the record into an event and queues it. It never blocks on the network and
// always returns nil.
func (h *SlogHandler) Handle(_ context.Context, record slog.Record) error {
	if isDiagnostic(record.Message) {
		return nil // the SDK's own diagnostics, looping back through ErrorLog
	}
	attrs := slogGroup{}
	category := h.opts.Category
	for _, grouped := range h.attrs {
		if len(grouped.groups) == 0 && grouped.attr.Key == "category" {
			if text, ok := categoryValue(grouped.attr); ok {
				category = text
				continue
			}
		}
		addAttr(target(attrs, grouped.groups), grouped.attr)
	}
	recordTarget := target(attrs, h.groups)
	record.Attrs(func(attr slog.Attr) bool {
		if len(h.groups) == 0 && attr.Key == "category" {
			if text, ok := categoryValue(attr); ok {
				category = text
				return true
			}
		}
		addAttr(recordTarget, attr)
		return true
	})
	if h.opts.AddSource && record.PC != 0 {
		frame, _ := runtime.CallersFrames([]uintptr{record.PC}).Next()
		attrs["source"] = map[string]any{"function": frame.Function, "file": frame.File, "line": frame.Line}
	}
	pruneEmpty(attrs)
	h.client.record(record.Time, SlogLevel(record.Level), "", record.Message,
		[]Option{Category(category), Attrs(attrs)})
	return nil
}

// isDiagnostic reports a line the SDK wrote to Config.ErrorLog, wherever a log.Logger put its
// date, file or prefix before it.
func isDiagnostic(message string) bool {
	return strings.Contains(message, diagnosticPrefix) || strings.Contains(message, "Lyrelog: ")
}

// slogGroup is a map the handler made for a group. Only these are filled and pruned: a map the
// caller logged (slog.Any("settings", m)) is the caller's and is never changed.
type slogGroup map[string]any

func categoryValue(attr slog.Attr) (string, bool) {
	value := attr.Value.Resolve()
	if value.Kind() != slog.KindString {
		return "", false
	}
	return value.String(), true
}

// WithAttrs returns a handler whose events also carry attrs, under the groups open now.
func (h *SlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	next := *h
	next.attrs = slices.Clip(h.attrs)
	for _, attr := range attrs {
		next.attrs = append(next.attrs, groupedAttr{groups: h.groups, attr: attr})
	}
	return &next
}

// WithGroup returns a handler that nests the attrs that follow under name.
func (h *SlogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	next := *h
	next.groups = append(slices.Clip(h.groups), name)
	return &next
}

// target returns the nested map for a group path, creating it.
func target(root slogGroup, groups []string) slogGroup {
	current := root
	for _, group := range groups {
		child, ok := current[group].(slogGroup)
		if !ok {
			child = slogGroup{}
			current[group] = child
		}
		current = child
	}
	return current
}

// addAttr follows slog's rules: an empty attr is ignored, a group with an empty key is
// inlined, an empty group is dropped.
func addAttr(into slogGroup, attr slog.Attr) {
	value := attr.Value.Resolve()
	if attr.Equal(slog.Attr{}) {
		return
	}
	switch value.Kind() {
	case slog.KindGroup:
		members := value.Group()
		if len(members) == 0 {
			return
		}
		dest := into
		if attr.Key != "" {
			dest = target(into, []string{attr.Key})
		}
		for _, member := range members {
			addAttr(dest, member)
		}
	default:
		if attr.Key == "" {
			return
		}
		into[attr.Key] = slogValue(value)
	}
}

func slogValue(value slog.Value) any {
	switch value.Kind() {
	case slog.KindString:
		return value.String()
	case slog.KindInt64:
		return value.Int64()
	case slog.KindUint64:
		return value.Uint64()
	case slog.KindFloat64:
		return value.Float64()
	case slog.KindBool:
		return value.Bool()
	case slog.KindDuration:
		return value.Duration().String()
	case slog.KindTime:
		return value.Time()
	default:
		return value.Any()
	}
}

func pruneEmpty(attrs slogGroup) {
	for key, value := range attrs {
		if nested, ok := value.(slogGroup); ok {
			pruneEmpty(nested)
			if len(nested) == 0 {
				delete(attrs, key)
			}
		}
	}
}
