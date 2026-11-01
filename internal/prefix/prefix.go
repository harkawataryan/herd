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
type Index struct {
	mu    sync.Mutex
	cap   int
	order *list.List
	set   map[uint64]*list.Element
}

func NewIndex(capBlocks int) *Index {
	if capBlocks < 1 {
		capBlocks = 1
	}
	return &Index{cap: capBlocks, order: list.New(), set: make(map[uint64]*list.Element)}
}

// Add records a chain as recently used, evicting the least recently used
// blocks beyond capacity.
func (x *Index) Add(chain []uint64) {
	x.mu.Lock()
	defer x.mu.Unlock()
	for _, h := range chain {
		if el, ok := x.set[h]; ok {
			x.order.MoveToFront(el)
			continue
		}
		x.set[h] = x.order.PushFront(h)
		for x.order.Len() > x.cap {
			last := x.order.Back()
			delete(x.set, last.Value.(uint64))
			x.order.Remove(last)
		}
	}
}

// Match returns how many leading blocks of the chain are present. Because
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

func (x *Index) Len() int {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.order.Len()
}
