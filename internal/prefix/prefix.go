// Package prefix approximates a model server's prefix cache from the outside.
//
// Servers such as vLLM cache KV blocks keyed by a hash chain over fixed-size
// token blocks, so a request whose prompt starts with the same blocks as an
// earlier one skips prefill for those blocks. The gateway can't see that
// cache, but it can remember which block chains it has sent to which replica
// and route a new request to the replica most likely to still hold them.
package prefix

import (
	"container/list"
	"encoding/binary"
	"hash/fnv"
	"sync"
	"time"
)

// BlockChars is the block size in characters; at ~4 chars per token this is
// 64 tokens, in the range real servers use (16–128).
const BlockChars = 256

// BlockTokens is the token estimate per block, for converting matched blocks
// into skipped prefill work.
const BlockTokens = BlockChars / 4

// Chain returns one hash per full block of text, each folded over the
// previous hash so a block hash identifies its whole prefix, not just its
// own bytes. Partial trailing blocks are not cached, matching vLLM.
func Chain(text string) []uint64 {
	n := len(text) / BlockChars
	out := make([]uint64, 0, n)
	var prev uint64
	var buf [8]byte
	for i := 0; i < n; i++ {
		h := fnv.New64a()
		binary.LittleEndian.PutUint64(buf[:], prev)
		h.Write(buf[:])
		h.Write([]byte(text[i*BlockChars : (i+1)*BlockChars]))
		prev = h.Sum64()
		out = append(out, prev)
	}
	return out
}

// Index is an LRU set of block hashes believed to be cached on one replica.
// Its order mirrors the server's own eviction order, so the back of the list
// is what the server will drop next when it needs room.
type Index struct {
	mu    sync.Mutex
	cap   int
	order *list.List
	set   map[uint64]*list.Element

	// Recent insertions, for estimating how fast the cache turns over.
	inserts []insertEvent
	now     func() time.Time
}

type insertEvent struct {
	at time.Time
	n  int
}

// rateWindow bounds how far back InsertRate looks.
const rateWindow = 2 * time.Minute

func NewIndex(capBlocks int) *Index {
	if capBlocks < 1 {
		capBlocks = 1
	}
	return &Index{cap: capBlocks, order: list.New(), set: make(map[uint64]*list.Element), now: time.Now}
}

// Add records a chain as recently used, evicting the least recently used
// blocks beyond capacity.
func (x *Index) Add(chain []uint64) {
	x.mu.Lock()
	defer x.mu.Unlock()
	inserted := 0
	for _, h := range chain {
		if el, ok := x.set[h]; ok {
			x.order.MoveToFront(el)
			continue
		}
		x.set[h] = x.order.PushFront(h)
		inserted++
		for x.order.Len() > x.cap {
			last := x.order.Back()
			delete(x.set, last.Value.(uint64))
			x.order.Remove(last)
		}
	}
	if inserted > 0 {
		now := x.now()
		x.inserts = append(x.inserts, insertEvent{at: now, n: inserted})
		x.pruneLocked(now)
	}
}

func (x *Index) pruneLocked(now time.Time) {
	cut := 0
	for cut < len(x.inserts) && now.Sub(x.inserts[cut].at) > rateWindow {
		cut++
	}
	if cut > 0 {
		x.inserts = append(x.inserts[:0], x.inserts[cut:]...)
	}
}

// InsertRate is the number of new blocks per second that entered the index
// over the last window (capped at rateWindow). Divided into the capacity it
// gives the cache's turnover time: how long a block that is never touched
// again survives before the LRU reaches it.
func (x *Index) InsertRate(window time.Duration) float64 {
	x.mu.Lock()
	defer x.mu.Unlock()
	now := x.now()
	x.pruneLocked(now)
	if window <= 0 || window > rateWindow {
		window = rateWindow
	}
	n := 0
	for i := len(x.inserts) - 1; i >= 0; i-- {
		if now.Sub(x.inserts[i].at) > window {
			break
		}
		n += x.inserts[i].n
	}
	return float64(n) / window.Seconds()
}

// Match returns how many leading blocks of the chain are present and marks
// them recently used, as the server does when it serves the request. Because
// hashes are chained, a hit on block i implies hits on all blocks before it,
// so the scan stops at the first miss.
func (x *Index) Match(chain []uint64) int {
	x.mu.Lock()
	defer x.mu.Unlock()
	n := 0
	for _, h := range chain {
		el, ok := x.set[h]
		if !ok {
			break
		}
		x.order.MoveToFront(el)
		n++
	}
	return n
}

// Peek is Match without the side effect: it reports the hit length but
// leaves the recency order alone. Scoring every candidate must use Peek,
// otherwise merely considering a replica would make its mirror think the
// blocks were just used.
func (x *Index) Peek(chain []uint64) int {
	x.mu.Lock()
	defer x.mu.Unlock()
	n := 0
	for _, h := range chain {
		if _, ok := x.set[h]; !ok {
			break
		}
		n++
	}
	return n
}

// Evictions returns the hashes that adding a chain with newBlocks unseen
// blocks would push out: the oldest entries beyond the free space. Hashes
// in keep are treated as already present (they are about to be touched, so
// they would move to the front rather than fall off the back).
func (x *Index) Evictions(newBlocks int, keep []uint64) []uint64 {
	x.mu.Lock()
	defer x.mu.Unlock()
	over := newBlocks - (x.cap - x.order.Len())
	if over <= 0 {
		return nil
	}
	skip := make(map[uint64]struct{}, len(keep))
	for _, h := range keep {
		skip[h] = struct{}{}
	}
	out := make([]uint64, 0, over)
	for el := x.order.Back(); el != nil && len(out) < over; el = el.Prev() {
		h := el.Value.(uint64)
		if _, ok := skip[h]; ok {
			continue
		}
		out = append(out, h)
	}
	return out
}

func (x *Index) Len() int {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.order.Len()
}

// Cap is the number of blocks the index (and, by assumption, the server's
// prefix cache) can hold.
func (x *Index) Cap() int { return x.cap }
