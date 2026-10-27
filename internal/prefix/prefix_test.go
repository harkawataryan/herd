package prefix

import (
	"strings"
	"testing"
)

func TestChainIsPrefixSensitive(t *testing.T) {
	a := strings.Repeat("a", BlockChars*3)
	b := strings.Repeat("a", BlockChars*2) + strings.Repeat("b", BlockChars)
	ca, cb := Chain(a), Chain(b)
	if len(ca) != 3 || len(cb) != 3 {
		t.Fatalf("expected 3 blocks each, got %d and %d", len(ca), len(cb))
	}
	if ca[0] != cb[0] || ca[1] != cb[1] {
		t.Fatal("shared prefix blocks should hash identically")
	}
	if ca[2] == cb[2] {
		t.Fatal("diverging block should hash differently")
	}
	// A different first block changes every later hash, even if later text matches.
	c := strings.Repeat("c", BlockChars) + strings.Repeat("a", BlockChars*2)
	if Chain(c)[1] == ca[1] {
		t.Fatal("chain must fold the previous hash in")
	}
	if len(Chain(strings.Repeat("x", BlockChars-1))) != 0 {
		t.Fatal("partial blocks are not cached")
	}
}

func TestIndexMatchAndEviction(t *testing.T) {
	idx := NewIndex(4)
	long := Chain(strings.Repeat("s", BlockChars*4))
	idx.Add(long)
	if got := idx.Match(long); got != 4 {
		t.Fatalf("full match expected 4, got %d", got)
	}
	// Same system prompt, different user turn: first 2 blocks match.
	other := Chain(strings.Repeat("s", BlockChars*2) + strings.Repeat("u", BlockChars*2))
	if got := idx.Match(other); got != 2 {
		t.Fatalf("partial match expected 2, got %d", got)
	}
	// Adding another chain evicts the least recently used blocks.
	idx.Add(other)
	if idx.Len() != 4 {
		t.Fatalf("capacity should hold, got %d", idx.Len())
	}
	if got := idx.Match(long); got == 4 {
		t.Fatal("old tail blocks should have been evicted")
	}
}
