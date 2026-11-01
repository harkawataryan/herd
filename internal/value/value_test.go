package value

import (
	"math"
	"testing"
	"time"
)

func fixedClock(t *Tracker) *time.Time {
	now := time.Unix(1_000_000, 0)
	t.now = func() time.Time { return now }
	return &now
}

func TestFrequencyDecaysByHalfLife(t *testing.T) {
	tr := New(10*time.Second, time.Second)
	now := fixedClock(tr)
	tr.Observe([]uint64{1, 2})
	tr.Observe([]uint64{1})
	if f := tr.Frequency(1); f != 2 {
		t.Fatalf("frequency = %v, want 2", f)
	}
	*now = now.Add(10 * time.Second)
	if f := tr.Frequency(1); math.Abs(f-1) > 1e-9 {
		t.Fatalf("after one half-life frequency = %v, want 1", f)
	}
	if f := tr.Frequency(99); f != 0 {
		t.Fatalf("unknown block frequency = %v, want 0", f)
	}
}

func TestValueIsAProbabilityCappedAtOne(t *testing.T) {
	tr := New(10*time.Second, time.Second)
	fixedClock(tr)
	for i := 0; i < 50; i++ {
		tr.Observe([]uint64{7})
	}
	hot := tr.Value(7, 1)
	if hot < 0.99 || hot > 1 {
		t.Fatalf("hot block value = %v, want ~1", hot)
	}
	tr.Observe([]uint64{8})
	once := tr.Value(8, 0.25) // requested once, would survive a quarter half-life
	if want := 1 - math.Exp(-0.25); math.Abs(once-want) > 1e-9 {
		t.Fatalf("one-off block value = %v, want %v", once, want)
	}
	if v := tr.Value(9, 1); v != 0 {
		t.Fatalf("unseen block value = %v, want 0", v)
	}
}

func TestSessionAddsReturnProbabilityAndDecays(t *testing.T) {
	tr := New(time.Minute, 4*time.Second)
	now := fixedClock(tr)
	chain := []uint64{10, 11, 12}
	tr.BeginTurn("s1", "sim-1", chain)
	if v := tr.Value(12, 1); v < 0.89 {
		t.Fatalf("in-flight session block value = %v, want ~0.9", v)
	}
	tr.EndTurn("s1")
	just := tr.Value(12, 1)
	*now = now.Add(8 * time.Second) // two default gaps later
	later := tr.Value(12, 1)
	if !(later < just) || later < 0.3 || later > 0.4 {
		t.Fatalf("return probability should decay: just=%v later=%v (want ~0.33)", just, later)
	}
	// A second turn teaches the session its own gap.
	tr.BeginTurn("s1", "sim-1", append(chain, 13))
	if s := tr.Lookup("s1"); s == nil || s.Turns != 2 || math.Abs(s.MeanGap-8) > 1e-9 {
		t.Fatalf("session = %+v, want 2 turns with 8s gap", s)
	}
	tr.EndTurn("s1")
	*now = now.Add(8 * time.Second)
	if v := tr.Value(13, 1); v < 0.5 {
		t.Fatalf("after learning a 8s gap, 8s idle should still be likely to return: %v", v)
	}
}

func TestInferFindsTheSessionAPromptExtends(t *testing.T) {
	tr := New(time.Minute, time.Second)
	fixedClock(tr)
	tr.BeginTurn("s1", "sim-1", []uint64{1, 2, 3})
	tr.EndTurn("s1")
	if id := tr.Infer([]uint64{1, 2, 3, 4, 5}); id != "s1" {
		t.Fatalf("inferred %q, want s1", id)
	}
	if id := tr.Infer([]uint64{1, 2}); id != "" {
		t.Fatalf("a shorter prompt is not a continuation, got %q", id)
	}
	if id := tr.Infer([]uint64{1, 9, 3, 4}); id != "" {
		t.Fatalf("a diverged prompt is not a continuation, got %q", id)
	}
}

func TestExpireDropsIdleSessions(t *testing.T) {
	tr := New(time.Minute, time.Second)
	now := fixedClock(tr)
	tr.BeginTurn("s1", "sim-1", []uint64{1})
	tr.EndTurn("s1")
	*now = now.Add(time.Hour)
	tr.Expire(time.Minute)
	if tr.Lookup("s1") != nil || tr.Value(1, 1) != 0 {
		t.Fatal("expired session should be gone and its blocks unowned")
	}
}
