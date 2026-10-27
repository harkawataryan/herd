package queue

import (
	"context"
	"sync"
	"testing"
	"time"
)

// recorder collects the order in which waiters are released. Releases are
// driven one at a time with releaseOne so the recorded order is the
// dispatcher's order, not goroutine wake-up order.
type recorder struct {
	mu    sync.Mutex
	order []string
}

func (r *recorder) add(s string) {
	r.mu.Lock()
	r.order = append(r.order, s)
	r.mu.Unlock()
}

func (r *recorder) waitLen(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		l := len(r.order)
		r.mu.Unlock()
		if l >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d releases", n)
}

func (q *Queue) depthTotal() int {
	s, c := q.Depth()
	return s + c
}

func waitDepth(t *testing.T, q *Queue, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if q.depthTotal() == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("queue depth never reached %d (is %d)", n, q.depthTotal())
}

func TestCriticalReleasedBeforeStandard(t *testing.T) {
	q := New(nil, time.Second, func() bool { return true })
	rec := &recorder{}
	ctx := context.Background()
	go func() {
		if _, ok := q.Wait(ctx, "t", Standard); ok {
			rec.add("std")
		}
	}()
	waitDepth(t, q, 1)
	go func() {
		if _, ok := q.Wait(ctx, "t", Critical); ok {
			rec.add("crit")
		}
	}()
	waitDepth(t, q, 2)

	if !q.releaseOne() {
		t.Fatal("expected a release")
	}
	rec.waitLen(t, 1)
	if rec.order[0] != "crit" {
		t.Fatalf("critical should go first, got %v", rec.order)
	}
	q.releaseOne()
	rec.waitLen(t, 2)
}

func TestWeightedFairnessAcrossTenants(t *testing.T) {
	q := New(map[string]int{"big": 3, "small": 1}, time.Second, func() bool { return true })
	rec := &recorder{}
	ctx := context.Background()
	for i := 0; i < 8; i++ {
		for _, tenant := range []string{"big", "small"} {
			go func(tenant string) {
				if _, ok := q.Wait(ctx, tenant, Standard); ok {
					rec.add(tenant)
				}
			}(tenant)
		}
	}
	waitDepth(t, q, 16)
	for i := 1; i <= 8; i++ {
		if !q.releaseOne() {
			t.Fatalf("release %d failed", i)
		}
		rec.waitLen(t, i)
	}
	big := 0
	for _, x := range rec.order[:8] {
		if x == "big" {
			big++
		}
	}
	// Weight 3:1 should give the big tenant about three quarters of the slots.
	if big < 5 || big > 7 {
		t.Fatalf("expected big tenant to get ~6 of the first 8 slots, got %d (%v)", big, rec.order[:8])
	}
	if rec.order[0] != "big" {
		t.Fatalf("heavier tenant should be served first, got %v", rec.order)
	}
}

func TestWaitTimesOut(t *testing.T) {
	q := New(nil, 20*time.Millisecond, func() bool { return false })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go q.Run(ctx, 5*time.Millisecond)
	waited, ok := q.Wait(ctx, "t", Standard)
	if ok || waited < 20*time.Millisecond {
		t.Fatalf("expected timeout after 20ms, got ok=%v waited=%v", ok, waited)
	}
	if s, c := q.Depth(); s != 0 || c != 0 {
		t.Fatalf("timed-out waiter should be removed, depth %d/%d", s, c)
	}
}

func TestRunReleasesWhenCapacityReturns(t *testing.T) {
	var open bool
	var mu sync.Mutex
	q := New(nil, time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return open })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go q.Run(ctx, 5*time.Millisecond)
	done := make(chan bool)
	go func() {
		_, ok := q.Wait(ctx, "t", Standard)
		done <- ok
	}()
	waitDepth(t, q, 1)
	mu.Lock()
	open = true
	mu.Unlock()
	q.Kick()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("waiter should have been released")
		}
	case <-time.After(time.Second):
		t.Fatal("dispatcher never released the waiter")
	}
}
