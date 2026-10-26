package logiq

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	gofakes3 "github.com/logiqai/s32http"
	"github.com/sirupsen/logrus"
)

const reset = -1

// fakeFlash is a /v1/json_batch endpoint. respond picks the outcome of the
// n-th request (1-based) from its events; reset closes the connection without
// a response. Events are recorded as delivered only when the answer is 2xx.
type fakeFlash struct {
	t        *testing.T
	srv      *httptest.Server
	mu       sync.Mutex
	requests int
	got      map[string]int
	respond  func(n int, events []string, w http.ResponseWriter) int
}

func newFakeFlash(t *testing.T, respond func(n int, events []string, w http.ResponseWriter) int) *fakeFlash {
	f := &fakeFlash{t: t, got: map[string]int{}, respond: respond}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeFlash) serve(w http.ResponseWriter, r *http.Request) {
	var batch []map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
		f.t.Errorf("bad batch: %v", err)
	}
	events := make([]string, 0, len(batch))
	for _, e := range batch {
		events = append(events, e["message"].(string))
	}
	f.mu.Lock()
	f.requests++
	n := f.requests
	f.mu.Unlock()

	status := f.respond(n, events, w)
	if status == reset {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			f.t.Fatalf("hijack: %v", err)
		}
		conn.(*net.TCPConn).SetLinger(0)
		conn.Close()
		return
	}
	if status/100 == 2 {
		f.mu.Lock()
		for _, e := range events {
			f.got[e]++
		}
		f.mu.Unlock()
	}
	w.WriteHeader(status)
	io.WriteString(w, `{"error":"rate limited"}`)
}

func (f *fakeFlash) count(event string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.got[event]
}

func (f *fakeFlash) requestCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests
}

func always(status int) func(int, []string, http.ResponseWriter) int {
	return func(int, []string, http.ResponseWriter) int { return status }
}

func newTestBackend(t *testing.T, f *fakeFlash, opts ...Option) *Backend {
	u, err := url.Parse(f.srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	opts = append([]Option{WithLogiqHost(u.Hostname()), WithLogiqPort(u.Port()), WithMaxBatch(10), WithMaxWorkers(4)}, opts...)
	b := New(opts...)
	b.retryBudget = 300 * time.Millisecond
	b.attemptTimeout = 200 * time.Millisecond
	b.minAttempt = 100 * time.Millisecond
	b.backoffBase = time.Millisecond
	b.backoffMax = 5 * time.Millisecond
	if err := b.CreateBucket("lq"); err != nil {
		t.Fatal(err)
	}
	return b
}

// object builds a Splunk rfs:s3 object of n events named prefix-0..prefix-(n-1).
func object(prefix string, n int) []byte {
	events := make([]map[string]string, n)
	for i := range events {
		events[i] = map[string]string{"event": fmt.Sprintf("%s-%d", prefix, i), "sourcetype": "kong:json"}
	}
	b, _ := json.Marshal(events)
	return b
}

func put(b *Backend, name string, obj []byte) error {
	_, err := b.PutObject("lq", name, nil, bytes.NewReader(obj), int64(len(obj)))
	return err
}

func assertEachDelivered(t *testing.T, f *fakeFlash, prefix string, n, want int) {
	t.Helper()
	for i := 0; i < n; i++ {
		e := fmt.Sprintf("%s-%d", prefix, i)
		if got := f.count(e); got != want {
			t.Errorf("event %s delivered %d times, want %d", e, got, want)
		}
	}
}

// logHook captures entries written to the package logger at warn and above.
type logHook struct {
	mu      sync.Mutex
	entries []*logrus.Entry
}

func (h *logHook) Levels() []logrus.Level {
	return []logrus.Level{logrus.PanicLevel, logrus.FatalLevel, logrus.ErrorLevel, logrus.WarnLevel}
}

func (h *logHook) Fire(e *logrus.Entry) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.entries = append(h.entries, e)
	return nil
}

func (h *logHook) errorCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, e := range h.entries {
		if e.Level == logrus.ErrorLevel {
			n++
		}
	}
	return n
}

func (h *logHook) hasError(substrs ...string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, e := range h.entries {
		if e.Level != logrus.ErrorLevel {
			continue
		}
		all := true
		for _, s := range substrs {
			if !strings.Contains(e.Message, s) {
				all = false
			}
		}
		if all {
			return true
		}
	}
	return false
}

func captureLog(t *testing.T) *logHook {
	h := &logHook{}
	log.AddHook(h)
	out := log.Out
	log.SetOutput(io.Discard)
	t.Cleanup(func() {
		log.ReplaceHooks(logrus.LevelHooks{})
		log.SetOutput(out)
	})
	return h
}

func TestPutObjectDeliversEveryEventOnce(t *testing.T) {
	f := newFakeFlash(t, always(http.StatusOK))
	b := newTestBackend(t, f)

	if err := put(b, "obj", object("e", 25)); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	assertEachDelivered(t, f, "e", 25, 1)
	if got := f.requestCount(); got != 3 {
		t.Errorf("requests = %d, want 3 batches of up to 10", got)
	}
}

func TestPutObjectRetriesRateLimitHonouringRetryAfter(t *testing.T) {
	f := newFakeFlash(t, func(n int, _ []string, w http.ResponseWriter) int {
		if n == 1 {
			w.Header().Set("Retry-After", "1")
			return http.StatusTooManyRequests
		}
		return http.StatusOK
	})
	b := newTestBackend(t, f)
	b.retryBudget = 3 * time.Second

	start := time.Now()
	if err := put(b, "obj", object("e", 5)); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if elapsed := time.Since(start); elapsed < time.Second {
		t.Errorf("retried after %v, want at least the 1s Retry-After", elapsed)
	}
	assertEachDelivered(t, f, "e", 5, 1)
	if got := f.requestCount(); got != 2 {
		t.Errorf("requests = %d, want 2", got)
	}
}

func TestPutObjectRetriesServerErrors(t *testing.T) {
	for _, status := range []int{http.StatusInternalServerError, http.StatusServiceUnavailable} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			f := newFakeFlash(t, func(n int, _ []string, _ http.ResponseWriter) int {
				if n == 1 {
					return status
				}
				return http.StatusOK
			})
			b := newTestBackend(t, f)

			if err := put(b, "obj", object("e", 5)); err != nil {
				t.Fatalf("PutObject: %v", err)
			}
			assertEachDelivered(t, f, "e", 5, 1)
		})
	}
}

func TestPutObjectRetriesAfterConnectionReset(t *testing.T) {
	f := newFakeFlash(t, func(n int, _ []string, _ http.ResponseWriter) int {
		if n == 1 {
			return reset
		}
		return http.StatusOK
	})
	b := newTestBackend(t, f)

	if err := put(b, "obj", object("e", 5)); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	assertEachDelivered(t, f, "e", 5, 1)
}

func TestPutObjectFailsWhenRateLimitOutlastsBudget(t *testing.T) {
	logs := captureLog(t)
	f := newFakeFlash(t, always(http.StatusTooManyRequests))
	b := newTestBackend(t, f)

	start := time.Now()
	if err := put(b, "obj", object("e", 5)); err == nil {
		t.Fatal("PutObject succeeded although flash never accepted the batch")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("gave up after %v, want within the 300ms budget", elapsed)
	}
	if got := f.requestCount(); got < 2 {
		t.Errorf("requests = %d, want the batch retried", got)
	}
	if !logs.hasError("429", "rate limited") {
		t.Error("no error log with the status and body flash answered")
	}
}

func TestPutObjectOutOfBudgetLogsOneError(t *testing.T) {
	logs := captureLog(t)
	f := newFakeFlash(t, always(http.StatusTooManyRequests))
	b := newTestBackend(t, f, WithMaxWorkers(1))

	if err := put(b, "obj", object("e", 25)); err == nil {
		t.Fatal("PutObject succeeded although flash never accepted a batch")
	}
	if got := logs.errorCount(); got != 1 {
		t.Errorf("logged %d errors, want 1 for the failed PUT however many batches it had", got)
	}
	if !logs.hasError("429", "rate limited") {
		t.Error("the error does not carry the status and body flash answered")
	}
}

// slowFlash accepts every batch after delay, and records it even when the
// client has given up waiting, as flash does once it has started publishing.
func slowFlash(t *testing.T, delay func() time.Duration) *fakeFlash {
	return newFakeFlash(t, func(int, []string, http.ResponseWriter) int {
		time.Sleep(delay())
		return http.StatusOK
	})
}

func TestPutObjectDoesNotStartBatchWithTooLittleTimeLeft(t *testing.T) {
	var mu sync.Mutex
	delay := 120 * time.Millisecond
	f := slowFlash(t, func() time.Duration {
		mu.Lock()
		defer mu.Unlock()
		return delay
	})
	b := newTestBackend(t, f, WithMaxWorkers(1))
	obj := object("e", 35)

	// Batches 1 and 2 take 240ms of the 300ms budget, leaving batch 3 less
	// than the 100ms minimum attempt.
	if err := put(b, "obj", obj); err == nil {
		t.Fatal("first PutObject succeeded although flash was too slow for the budget")
	}
	mu.Lock()
	delay = 0
	mu.Unlock()
	if err := put(b, "obj", obj); err != nil {
		t.Fatalf("retried PutObject: %v", err)
	}
	// Wait for any request the first PUT gave up on.
	f.srv.Close()
	assertEachDelivered(t, f, "e", 35, 1)
}

func TestPutObjectDoesNotRetryBatchWithTooLittleTimeLeft(t *testing.T) {
	start := time.Now()
	f := newFakeFlash(t, func(int, []string, http.ResponseWriter) int {
		// Unavailable for 250ms of the 300ms budget, then slow to accept.
		if time.Since(start) < 250*time.Millisecond {
			return http.StatusServiceUnavailable
		}
		time.Sleep(150 * time.Millisecond)
		return http.StatusOK
	})
	b := newTestBackend(t, f)
	obj := object("e", 5)

	if err := put(b, "obj", obj); err == nil {
		t.Fatal("first PutObject succeeded although flash was unavailable for the budget")
	}
	if err := put(b, "obj", obj); err != nil {
		t.Fatalf("retried PutObject: %v", err)
	}
	// Wait for any request the first PUT gave up on.
	f.srv.Close()
	assertEachDelivered(t, f, "e", 5, 1)
}

func TestPutObjectFailsFastOnAuthAndRoutingErrors(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			logs := captureLog(t)
			f := newFakeFlash(t, always(status))
			b := newTestBackend(t, f, WithMaxWorkers(1))

			if err := put(b, "obj", object("e", 25)); err == nil {
				t.Fatal("PutObject succeeded although flash refused it")
			}
			if got := f.requestCount(); got != 1 {
				t.Errorf("requests = %d, want 1: no retry, and the object's other batches not sent", got)
			}
			if !logs.hasError(fmt.Sprint(status)) {
				t.Errorf("no error log with status %d", status)
			}
		})
	}
}

func TestPutObjectDropsBatchFlashCanNeverAccept(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusRequestEntityTooLarge} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			logs := captureLog(t)
			f := newFakeFlash(t, func(_ int, events []string, _ http.ResponseWriter) int {
				for _, e := range events {
					if e == "e-12" {
						return status
					}
				}
				return http.StatusOK
			})
			b := newTestBackend(t, f)

			if err := put(b, "obj", object("e", 25)); err != nil {
				t.Fatalf("PutObject: %v, want success with the rejected batch dropped", err)
			}
			for i := 0; i < 25; i++ {
				want := 1
				if i >= 10 && i < 20 {
					want = 0
				}
				if got := f.count(fmt.Sprintf("e-%d", i)); got != want {
					t.Errorf("event e-%d delivered %d times, want %d", i, got, want)
				}
			}
			if got := f.requestCount(); got != 3 {
				t.Errorf("requests = %d, want 3: the rejected batch is not retried", got)
			}
			if !logs.hasError(fmt.Sprint(status)) {
				t.Errorf("no error log for the dropped batch with status %d", status)
			}
		})
	}
}

func TestPutObjectRetriedBySplunkSendsOnlyFailedBatches(t *testing.T) {
	var mu sync.Mutex
	down := true
	f := newFakeFlash(t, func(_ int, events []string, _ http.ResponseWriter) int {
		mu.Lock()
		defer mu.Unlock()
		if down && events[0] == "e-10" {
			return http.StatusServiceUnavailable
		}
		return http.StatusOK
	})
	b := newTestBackend(t, f)
	obj := object("e", 25)

	if err := put(b, "obj", obj); err == nil {
		t.Fatal("first PutObject succeeded although batch 2 never got through")
	}
	mu.Lock()
	down = false
	mu.Unlock()
	before := f.requestCount()

	if err := put(b, "obj", obj); err != nil {
		t.Fatalf("retried PutObject: %v", err)
	}
	assertEachDelivered(t, f, "e", 25, 1)
	if got := f.requestCount() - before; got != 1 {
		t.Errorf("retried PUT sent %d requests, want 1 (only the failed batch)", got)
	}
}

func TestPutObjectRetryOfDeliveredObjectSendsNothing(t *testing.T) {
	f := newFakeFlash(t, always(http.StatusOK))
	b := newTestBackend(t, f)
	obj := object("e", 25)

	if err := put(b, "obj", obj); err != nil {
		t.Fatal(err)
	}
	if err := put(b, "obj", obj); err != nil {
		t.Fatalf("retried PutObject: %v", err)
	}
	assertEachDelivered(t, f, "e", 25, 1)
	if got := f.requestCount(); got != 3 {
		t.Errorf("requests = %d, want 3: nothing resent", got)
	}
}

func TestPutObjectSameBytesUnderAnotherNameIsSent(t *testing.T) {
	f := newFakeFlash(t, always(http.StatusOK))
	b := newTestBackend(t, f)
	obj := object("e", 25)

	if err := put(b, "a", obj); err != nil {
		t.Fatal(err)
	}
	if err := put(b, "b", obj); err != nil {
		t.Fatal(err)
	}
	assertEachDelivered(t, f, "e", 25, 2)
}

func TestPutObjectChangedBytesUnderSameNameIsSent(t *testing.T) {
	f := newFakeFlash(t, always(http.StatusOK))
	b := newTestBackend(t, f)

	if err := put(b, "obj", object("e", 25)); err != nil {
		t.Fatal(err)
	}
	if err := put(b, "obj", object("f", 25)); err != nil {
		t.Fatal(err)
	}
	assertEachDelivered(t, f, "e", 25, 1)
	assertEachDelivered(t, f, "f", 25, 1)
}

func TestPutObjectForgetsDeliveredObjectsAfter15Minutes(t *testing.T) {
	clock := gofakes3.FixedTimeSource(time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC))
	f := newFakeFlash(t, always(http.StatusOK))
	b := newTestBackend(t, f, WithTimeSource(clock))
	obj := object("e", 25)

	if err := put(b, "obj", obj); err != nil {
		t.Fatal(err)
	}
	clock.Advance(14 * time.Minute)
	if err := put(b, "obj", obj); err != nil {
		t.Fatal(err)
	}
	assertEachDelivered(t, f, "e", 25, 1)

	// The retry above refreshed the entry, so it lives 15 minutes from there.
	clock.Advance(15*time.Minute + time.Second)
	if err := put(b, "obj", obj); err != nil {
		t.Fatal(err)
	}
	assertEachDelivered(t, f, "e", 25, 2)
}

func TestLedgerHoldsSecondSendOfSameObjectUntilFirstFinishes(t *testing.T) {
	l := newLedger(15*time.Minute, 100)
	now := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)

	first := l.acquire("lq/obj/md5", 10, 3, now)
	acquired := make(chan *ledgerEntry)
	go func() { acquired <- l.acquire("lq/obj/md5", 10, 3, now) }()

	select {
	case <-acquired:
		t.Fatal("second send of the same object started while the first was still running")
	case <-time.After(50 * time.Millisecond):
	}
	first.done[1] = true
	l.release(first, now)

	select {
	case second := <-acquired:
		if !second.done[1] {
			t.Error("second send does not see the batch the first one delivered")
		}
		l.release(second, now)
	case <-time.After(time.Second):
		t.Fatal("second send never started after the first finished")
	}
}

func TestLedgerEvictsOldestEntryWhenFull(t *testing.T) {
	l := newLedger(15*time.Minute, 2)
	now := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)

	for i, key := range []string{"a", "b", "c"} {
		e := l.acquire(key, 10, 1, now.Add(time.Duration(i)*time.Second))
		e.done[0] = true
		l.release(e, now.Add(time.Duration(i)*time.Second))
	}
	if e := l.acquire("a", 10, 1, now.Add(3*time.Second)); e.done[0] {
		t.Error("oldest entry survived although the ledger holds 2")
	} else {
		l.release(e, now.Add(3*time.Second))
	}
	if e := l.acquire("c", 10, 1, now.Add(4*time.Second)); !e.done[0] {
		t.Error("newest entry was evicted")
	} else {
		l.release(e, now.Add(4*time.Second))
	}
}
