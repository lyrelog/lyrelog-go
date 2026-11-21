package lyrelog

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// batch is a cut, compressed request body. A retry resends exactly these bytes, so the
// server can collapse a duplicate by event id and ts.
type batch struct {
	body   []byte
	gzip   bool
	events int
}

type outcomeKind int

const (
	delivered    outcomeKind = iota // stored; rejected lines are gone for good
	retryLater                      // temporary: keep the batch, back off
	dropBatch                       // the batch itself is wrong (400, 413, 415...)
	unauthorized                    // 401/403: stop sending until restart, keep the data
)

type outcome struct {
	kind       outcomeKind
	accepted   int
	rejected   []rejectedLine
	retryAfter time.Duration // 0 when the server didn't say
	status     int
	err        error // the network error, for retryLater
	// quota: a 429 because the organization's monthly events are used up (its JSON body has
	// a "plan"), not the per-project rate limit. Retried the same; quotaText is its "error".
	quota     bool
	quotaText string
}

type rejectedLine struct {
	Line  int    `json:"line"`
	Error string `json:"error"`
}

// classify decides what to do with a batch after a response. Pure, so it can be tested.
func classify(status int, body []byte, retryAfter string, now time.Time) outcome {
	switch {
	case status >= 200 && status < 300:
		var answer struct {
			Accepted *int           `json:"accepted"`
			Rejected []rejectedLine `json:"rejected"`
		}
		// A 2xx we can't read may not mean "stored": retry; the server drops duplicates.
		if json.Unmarshal(body, &answer) != nil || answer.Accepted == nil {
			return outcome{kind: retryLater, status: status}
		}
		return outcome{kind: delivered, accepted: *answer.Accepted, rejected: answer.Rejected, status: status}
	case status == 401 || status == 403:
		return outcome{kind: unauthorized, status: status}
	case status == 408 || status == 425 || status == 429 || status >= 500:
		result := outcome{kind: retryLater, status: status, retryAfter: parseRetryAfter(retryAfter, now)}
		if status == 429 {
			result.quota, result.quotaText = quotaStop(body)
		}
		return result
	default:
		return outcome{kind: dropBatch, status: status}
	}
}

const defaultQuotaText = "this organization's monthly events are used up"

// quotaStop reads a 429 body: the quota stop is a JSON object with a non-null "plan". Anything
// else (the rate limit's {"error": ...}, HTML, garbage) is a plain 429. The server's "error"
// text is kept to one printable line of at most 300 bytes.
func quotaStop(body []byte) (bool, string) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil {
		return false, ""
	}
	if plan, ok := fields["plan"]; !ok || string(plan) == "null" {
		return false, ""
	}
	var text string
	_ = json.Unmarshal(fields["error"], &text)
	text = strings.Map(func(r rune) rune {
		if r < ' ' || r == 0x7f {
			return ' '
		}
		return r
	}, text)
	text = strings.TrimRight(strings.TrimSpace(text), ".")
	if len(text) > 300 {
		text = strings.ToValidUTF8(text[:300], "")
	}
	if text == "" {
		text = defaultQuotaText
	}
	return true, text
}

// maxRetryAfter caps the server's Retry-After: the ingest never asks for more than an hour, and
// a proxy's "come back tomorrow" (or a number too large for a Duration) must not stop sending
// for longer.
const maxRetryAfter = time.Hour

// parseRetryAfter reads Retry-After as seconds or an HTTP date; 0 when absent or unreadable,
// at most maxRetryAfter.
func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseFloat(value, 64); err == nil {
		if !(seconds > 0) { // also NaN
			return 0
		}
		return time.Duration(min(seconds, maxRetryAfter.Seconds()) * float64(time.Second))
	}
	if date, err := http.ParseTime(value); err == nil {
		return min(max(0, date.Sub(now)), maxRetryAfter)
	}
	return 0
}

const (
	backoffBase = time.Second
	backoffCap  = 5 * time.Minute
)

// backoff is full jitter, uniform(0, min(cap, base * 2^attempt)), but never sooner than the
// server asked with Retry-After.
func backoff(attempt int, retryAfter time.Duration, random func() float64) time.Duration {
	ceiling := backoffCap
	if attempt < 20 {
		ceiling = min(backoffCap, backoffBase<<attempt)
	}
	return max(time.Duration(random()*float64(ceiling)), retryAfter)
}

func newBatch(lines [][]byte) *batch {
	var raw bytes.Buffer
	for i, line := range lines {
		if i > 0 {
			raw.WriteByte('\n')
		}
		raw.Write(line)
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(raw.Bytes()); err == nil && writer.Close() == nil {
		return &batch{body: compressed.Bytes(), gzip: true, events: len(lines)}
	}
	return &batch{body: raw.Bytes(), events: len(lines)}
}

// run is the background goroutine: it alone sends, so there is never more than one request.
func (c *Client) run() {
	defer close(c.done)
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.round(false)
		case <-c.wake:
			c.round(false)
		case reply := <-c.flushes:
			reply <- c.round(true)
		case <-c.stop:
			return
		}
	}
}

// round sends the batches covering what was queued when it started. force (a flush)
// ignores the backoff for one attempt. It returns why something is still waiting, or nil.
func (c *Client) round(force bool) error {
	if c.disabled {
		return ErrUnauthorized
	}
	if now := time.Now(); now.Before(c.serverWait) {
		if force {
			return c.retryErr
		}
		return nil
	} else if !force && now.Before(c.notBefore) {
		return nil
	}
	c.mu.Lock()
	remaining := len(c.queue)
	if c.pending != nil {
		remaining += c.pending.events
	}
	c.mu.Unlock()

	var firstErr error
	for remaining > 0 {
		current := c.nextBatch()
		if current == nil {
			break
		}
		remaining -= current.events
		result := c.send(current)
		switch result.kind {
		case delivered:
			c.clearPending()
			c.attempt, c.notBefore, c.serverWait, c.retryErr = 0, time.Time{}, time.Time{}, nil
			c.quotaWarned, c.downWarned = false, false
			c.sent.Add(uint64(result.accepted))
			if n := len(result.rejected); n > 0 {
				c.rejected.Add(uint64(n))
				c.logf("lyrelog: the server rejected %d of %d events (line %d: %s)",
					n, current.events, result.rejected[0].Line, result.rejected[0].Error)
			}
		case dropBatch:
			c.clearPending()
			c.dropped.Add(uint64(current.events))
			if result.status >= 300 && result.status < 400 {
				c.alarm("lyrelog: the endpoint answered a batch of %d events with a redirect (status %d); dropped. Point Config.Endpoint at the final URL", current.events, result.status)
			} else if result.status == http.StatusNotFound {
				c.alarm("lyrelog: the endpoint answered a batch of %d events with status 404; dropped. Check Config.Endpoint: it should be the ingest's base URL, like %s", current.events, DefaultEndpoint)
			} else {
				c.alarm("lyrelog: the server refused a batch of %d events with status %d; dropped", current.events, result.status)
			}
			if firstErr == nil {
				firstErr = fmt.Errorf("lyrelog: a batch of %d events was refused with status %d and dropped", current.events, result.status)
			}
		case unauthorized:
			c.disabled = true
			c.alarm("lyrelog: status %d: the API key was refused; sending stopped until restart", result.status)
			return ErrUnauthorized
		case retryLater:
			if result.quota && !c.quotaWarned {
				c.quotaWarned = true
				c.warnQuota(result.quotaText)
			}
			// The monthly quota is not a failure (sdk-spec rule 4): it waits for its Retry-After
			// but adds nothing to the backoff, nor to the failures that warn.
			delay := backoff(c.attempt, result.retryAfter, rand.Float64)
			c.notBefore = time.Now().Add(delay)
			cause := result.err
			if cause == nil {
				cause = fmt.Errorf("status %d", result.status)
			}
			if !result.quota {
				c.attempt++
				if c.attempt >= failuresBeforeWarning && !c.downWarned {
					c.downWarned = true
					c.warnDown(cause)
				}
			}
			c.retryErr = fmt.Errorf("lyrelog: delivery failed (%w); retrying in %v", cause, delay.Round(time.Millisecond))
			if result.retryAfter > 0 {
				c.serverWait = time.Now().Add(result.retryAfter)
			}
			return c.retryErr
		}
	}
	return firstErr
}

// nextBatch is the batch being retried, or a new one cut from the head of the queue:
// up to 500 events and 512 KiB before compression (always at least one event).
func (c *Client) nextBatch() *batch {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pending != nil {
		return c.pending
	}
	if len(c.queue) == 0 {
		return nil
	}
	count, size := 0, 0
	for count < len(c.queue) && count < maxBatchEvents {
		next := size + len(c.queue[count]) + 1
		if count > 0 && next > maxBatchBytes {
			break
		}
		size = next
		count++
	}
	lines := make([][]byte, count)
	copy(lines, c.queue[:count])
	c.queueBytes -= size - count
	clear(c.queue[:count])
	c.queue = c.queue[count:]
	c.pending = newBatch(lines)
	return c.pending
}

func (c *Client) clearPending() {
	c.mu.Lock()
	c.pending = nil
	c.mu.Unlock()
}

func (c *Client) send(b *batch) outcome {
	request, err := http.NewRequestWithContext(c.ctx, http.MethodPost, c.endpoint, bytes.NewReader(b.body))
	if err != nil {
		return outcome{kind: dropBatch, err: err}
	}
	request.Header.Set("Authorization", "Bearer "+c.apiKey)
	request.Header.Set("Content-Type", "application/x-ndjson")
	request.Header.Set("User-Agent", sdkName)
	if b.gzip {
		request.Header.Set("Content-Encoding", "gzip")
	}
	response, err := c.http.Do(request)
	if err != nil {
		if errors.Is(err, c.ctx.Err()) && c.ctx.Err() != nil {
			err = errors.New("closed during the request")
		}
		return outcome{kind: retryLater, err: err}
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return outcome{kind: retryLater, err: err}
	}
	return classify(response.StatusCode, body, response.Header.Get("Retry-After"), time.Now())
}
