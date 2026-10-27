// Package queue holds requests that can't be dispatched right now because
// every local replica is saturated, and releases them in a fair order once
// capacity returns: critical before standard, and within a class, tenants
// take turns weighted by their configured share (deficit round robin).
package queue

import (
	"context"
	"sort"
	"sync"
	"time"
)

type Class int

const (
	Standard Class = iota
	Critical
)

type waiter struct {
	tenant   string
	class    Class
	enqueued time.Time
	ready    chan struct{}
}

type Queue struct {
	mu      sync.Mutex
	waiters []*waiter
	weights map[string]int
	credit  map[string]float64
	maxWait time.Duration
	// canDispatch is polled by the dispatcher; it returns true when a
	// replica has capacity for one more request.
	canDispatch func() bool
	wake        chan struct{}
}

func New(weights map[string]int, maxWait time.Duration, canDispatch func() bool) *Queue {
	q := &Queue{weights: weights, credit: map[string]float64{}, maxWait: maxWait, canDispatch: canDispatch, wake: make(chan struct{}, 1)}
	return q
}

// Run is the dispatcher loop: whenever woken (or every tick), it releases
// as many waiters as capacity allows, in fairness order.
func (q *Queue) Run(ctx context.Context, tick time.Duration) {
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-q.wake:
		case <-t.C:
		}
		for q.releaseOne() {
		}
	}
}

// Kick asks the dispatcher to re-check capacity now (e.g. a request ended).
func (q *Queue) Kick() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// Depth reports waiting requests per class.
func (q *Queue) Depth() (standard, critical int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, w := range q.waiters {
		if w.class == Critical {
			critical++
		} else {
			standard++
		}
	}
	return
}

// Wait blocks until the request is released or the wait limit or context
// expires. It returns the time spent waiting and whether it was released.
func (q *Queue) Wait(ctx context.Context, tenant string, class Class) (time.Duration, bool) {
	w := &waiter{tenant: tenant, class: class, enqueued: time.Now(), ready: make(chan struct{})}
	q.mu.Lock()
	q.waiters = append(q.waiters, w)
	q.mu.Unlock()
	q.Kick()

	timer := time.NewTimer(q.maxWait)
	defer timer.Stop()
	select {
	case <-w.ready:
		return time.Since(w.enqueued), true
	case <-timer.C:
	case <-ctx.Done():
	}
	q.remove(w)
	return time.Since(w.enqueued), false
}

func (q *Queue) remove(w *waiter) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, x := range q.waiters {
		if x == w {
			q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
			return
		}
	}
}

// releaseOne picks the next waiter by class then tenant credit and releases
// it if there is capacity. Tenants with higher weight accumulate credit
// faster, so they get proportionally more of the freed slots, but a
// starving tenant always catches up.
func (q *Queue) releaseOne() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.waiters) == 0 || !q.canDispatch() {
		return false
	}
	// Candidates: the highest class present, then one waiter per tenant (its oldest).
	top := Standard
	for _, w := range q.waiters {
		if w.class > top {
			top = w.class
		}
	}
	oldestByTenant := map[string]*waiter{}
	for _, w := range q.waiters {
		if w.class != top {
			continue
		}
		if cur, ok := oldestByTenant[w.tenant]; !ok || w.enqueued.Before(cur.enqueued) {
			oldestByTenant[w.tenant] = w
		}
	}
	tenants := make([]string, 0, len(oldestByTenant))
	for t := range oldestByTenant {
		tenants = append(tenants, t)
		q.credit[t] += float64(q.weightOf(t))
	}
	sort.Slice(tenants, func(i, j int) bool {
		if q.credit[tenants[i]] != q.credit[tenants[j]] {
			return q.credit[tenants[i]] > q.credit[tenants[j]]
		}
		return tenants[i] < tenants[j]
	})
	chosen := oldestByTenant[tenants[0]]
	q.credit[tenants[0]] -= float64(q.totalWeight(tenants))
	for i, w := range q.waiters {
		if w == chosen {
			q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
			break
		}
	}
	close(chosen.ready)
	return true
}

func (q *Queue) weightOf(t string) int {
	if w, ok := q.weights[t]; ok && w > 0 {
		return w
	}
	return 1
}

func (q *Queue) totalWeight(tenants []string) int {
	n := 0
	for _, t := range tenants {
		n += q.weightOf(t)
	}
	return n
}
