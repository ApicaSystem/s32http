package logiq

import (
	"container/list"
	"sync"
	"time"
)

// ledger remembers which batches of an object Ascent has accepted. When a PUT
// fails, Splunk retries the same object with the same key and identical
// bytes, and only the batches that were not accepted are sent again. It lives
// in memory, so after a restart a retried object is sent in full.
type ledger struct {
	mu      sync.Mutex
	ttl     time.Duration
	max     int
	entries map[string]*list.Element
	order   *list.List // least recently used first
}

type ledgerEntry struct {
	key       string
	batchSize int
	done      []bool
	touched   time.Time
	// busy is held while a PUT of this object is being sent, so a retry that
	// overlaps an abandoned PUT waits for it and sees what it delivered.
	busy sync.Mutex
}

func newLedger(ttl time.Duration, max int) *ledger {
	return &ledger{ttl: ttl, max: max, entries: map[string]*list.Element{}, order: list.New()}
}

// acquire returns the entry for key, held for the caller until release. An
// entry untouched for longer than the ttl, or recorded with a different
// batching, starts afresh.
func (l *ledger) acquire(key string, batchSize, nBatches int, now time.Time) *ledgerEntry {
	l.mu.Lock()
	l.expire(now)
	var e *ledgerEntry
	if el, ok := l.entries[key]; ok {
		e = el.Value.(*ledgerEntry)
		l.order.MoveToBack(el)
	} else {
		e = &ledgerEntry{key: key}
		l.entries[key] = l.order.PushBack(e)
		for l.order.Len() > l.max {
			l.remove(l.order.Front())
		}
	}
	e.touched = now
	l.mu.Unlock()

	e.busy.Lock()
	if e.batchSize != batchSize || len(e.done) != nBatches {
		e.batchSize = batchSize
		e.done = make([]bool, nBatches)
	}
	return e
}

func (l *ledger) release(e *ledgerEntry, now time.Time) {
	l.mu.Lock()
	e.touched = now
	if el, ok := l.entries[e.key]; ok && el.Value == e {
		l.order.MoveToBack(el)
	}
	l.mu.Unlock()
	e.busy.Unlock()
}

func (l *ledger) expire(now time.Time) {
	for el := l.order.Front(); el != nil; el = l.order.Front() {
		if now.Sub(el.Value.(*ledgerEntry).touched) <= l.ttl {
			return
		}
		l.remove(el)
	}
}

func (l *ledger) remove(el *list.Element) {
	delete(l.entries, el.Value.(*ledgerEntry).key)
	l.order.Remove(el)
}
