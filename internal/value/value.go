// Package value estimates how much a cached prefix block is worth keeping.
//
// A prefix cache is only useful for blocks that will be asked for again.
// Two things predict that: how often a block has been requested recently
// (a shared system prompt is requested constantly, a one-off document never
// again), and whether the block belongs to a conversation that is still in
// progress (an agent between tool calls will be back in a few seconds with
// the same prefix plus a little more). This package tracks both, fleet-wide,
// so the scheduler can price what a placement would evict.
package value

import (
	"math"
	"sync"
	"time"
)

// Tracker holds decayed per-block request counts and live sessions.
type Tracker struct {
	mu sync.Mutex

	halfLife time.Duration       // request frequency halves every halfLife of inactivity
	blocks   map[uint64]blockRec // block hash -> decayed count
	sessions map[string]*Session
	owner    map[uint64]string // block hash -> session that last used it
	now      func() time.Time

	// Session gap estimate shared by all sessions until one has its own:
	// starts at the configured default and follows the fleet-wide mean of
	// observed gaps.
	defaultGap time.Duration
	fleetGap   float64 // seconds, EWMA over every observed gap
	lastPrune  time.Time
	maxBlocks  int
}

type blockRec struct {
	count float64
	seen  time.Time
}

// Session is one conversation as seen by the gateway: an agent loop or a
// multi-turn chat whose prompts extend one another.
type Session struct {
	ID       string
	Replica  string    // where its prefix currently lives
	Chain    []uint64  // prompt blocks of its latest turn
	Turns    int       // turns seen so far
	LastSeen time.Time // when its latest turn finished
	MeanGap  float64   // seconds between the end of one turn and the start of the next
	InFlight bool      // a turn is being served right now
}

func New(halfLife, defaultGap time.Duration) *Tracker {
	if halfLife <= 0 {
		halfLife = 60 * time.Second
	}
	if defaultGap <= 0 {
		defaultGap = 5 * time.Second
	}
	return &Tracker{
		halfLife: halfLife, defaultGap: defaultGap, maxBlocks: 500_000,
		blocks: map[uint64]blockRec{}, sessions: map[string]*Session{}, owner: map[uint64]string{},
		now: time.Now,
	}
}

func (t *Tracker) decayed(r blockRec, now time.Time) float64 {
	age := now.Sub(r.seen)
	if age <= 0 {
		return r.count
	}
	return r.count * math.Exp2(-float64(age)/float64(t.halfLife))
}

// Observe records that a prompt with this chain was requested.
func (t *Tracker) Observe(chain []uint64) {
	now := t.now()
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, h := range chain {
		r := t.blocks[h]
		t.blocks[h] = blockRec{count: t.decayed(r, now) + 1, seen: now}
	}
	if len(t.blocks) > t.maxBlocks && now.Sub(t.lastPrune) > t.halfLife {
		t.lastPrune = now
		for h, r := range t.blocks {
			if t.decayed(r, now) < 0.25 {
				delete(t.blocks, h)
			}
		}
	}
}

// Frequency is the decayed request count of one block: roughly how many
// times it has been asked for in the last half-life.
func (t *Tracker) Frequency(h uint64) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.decayed(t.blocks[h], t.now())
}

// Value prices a block in units of "prefill blocks saved in the future":
// the probability that evicting it now causes one re-prefill later. A block
// can only be missed once before it is cached again, so the value is capped
// at 1 per block.
//
// The decayed count is requests per half-life; horizon is the fraction of a
// half-life the block would survive on the replica in question if left
// alone (its cache turnover time over the half-life, at most 1). Their
// product is the expected number of requests for the block before it would
// have fallen out anyway; the chance of at least one is 1 − e^(−that).
// A conversation that is still going adds its own chance of coming back.
func (t *Tracker) Value(h uint64, horizon float64) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	pFreq := 1 - math.Exp(-t.decayed(t.blocks[h], now)*horizon)
	pSession := 0.0
	if id, ok := t.owner[h]; ok {
		if s := t.sessions[id]; s != nil {
			pSession = t.returnProbLocked(s, now)
		}
	}
	return 1 - (1-pFreq)*(1-pSession)
}

// Sum prices a set of blocks on a replica with the given horizon.
func (t *Tracker) Sum(hs []uint64, horizon float64) float64 {
	total := 0.0
	for _, h := range hs {
		total += t.Value(h, horizon)
	}
	return total
}

// Own prices the blocks a request would newly insert (those past hit): the
// probability each will be wanted again, from its request frequency and,
// for a conversation, the chance of a further turn. A session that has only
// just started has no history, so it is assumed likely to continue.
func (t *Tracker) Own(chain []uint64, hit int, sessionID string) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	pSession := 0.0
	if sessionID != "" {
		pSession = 0.8
		if s := t.sessions[sessionID]; s != nil {
			pSession = t.returnProbLocked(s, now)
		}
	}
	total := 0.0
	for i := hit; i < len(chain); i++ {
		pFreq := 1 - math.Exp(-t.decayed(t.blocks[chain[i]], now))
		total += 1 - (1-pFreq)*(1-pSession)
	}
	return total
}

// Gap is the expected time, in seconds, before the session sends its next
// turn: its own mean gap if it has one, else the fleet-wide mean, else the
// configured default.
func (t *Tracker) Gap(sessionID string) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if s := t.sessions[sessionID]; s != nil && s.MeanGap > 0 {
		return s.MeanGap
	}
	if t.fleetGap > 0 {
		return t.fleetGap
	}
	return t.defaultGap.Seconds()
}

// HalfLife is the popularity decay period.
func (t *Tracker) HalfLife() time.Duration { return t.halfLife }

// returnProbLocked estimates the chance a session sends another turn soon.
// A turn in flight will be followed by one almost certainly; a finished
// turn is followed with a probability that decays over a couple of typical
// gaps, since agents stop without saying so.
func (t *Tracker) returnProbLocked(s *Session, now time.Time) float64 {
	if s.InFlight {
		return 0.9
	}
	gap := s.MeanGap
	if gap <= 0 {
		gap = t.fleetGap
	}
	if gap <= 0 {
		gap = t.defaultGap.Seconds()
	}
	idle := now.Sub(s.LastSeen).Seconds()
	return 0.9 * math.Exp(-idle/(2*gap))
}

// BeginTurn records that a session's turn has been dispatched to a replica.
// The chain becomes the session's current prefix.
func (t *Tracker) BeginTurn(id, replica string, chain []uint64) {
	if id == "" {
		return
	}
	now := t.now()
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.sessions[id]
	if s == nil {
		s = &Session{ID: id}
		t.sessions[id] = s
	} else if !s.LastSeen.IsZero() {
		gap := now.Sub(s.LastSeen).Seconds()
		if s.MeanGap == 0 {
			s.MeanGap = gap
		} else {
			s.MeanGap = 0.7*s.MeanGap + 0.3*gap
		}
		if t.fleetGap == 0 {
			t.fleetGap = gap
		} else {
			t.fleetGap = 0.95*t.fleetGap + 0.05*gap
		}
	}
	s.Replica, s.Chain, s.InFlight = replica, chain, true
	s.Turns++
	for _, h := range chain {
		t.owner[h] = id
	}
}

// EndTurn records that the session's current turn finished.
func (t *Tracker) EndTurn(id string) {
	if id == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if s := t.sessions[id]; s != nil {
		s.InFlight = false
		s.LastSeen = t.now()
	}
}

// Lookup returns a session by id, or nil.
func (t *Tracker) Lookup(id string) *Session {
	t.mu.Lock()
	defer t.mu.Unlock()
	if s := t.sessions[id]; s != nil {
		c := *s
		return &c
	}
	return nil
}

// Infer finds the session a prompt continues when the client sent no
// session id: the session whose latest chain is a proper prefix of this
// one. Agent loops and chats grow their prompt each turn, so the previous
// turn's whole prompt is a prefix of the next. Returns "" if none.
func (t *Tracker) Infer(chain []uint64) string {
	if len(chain) == 0 {
		return ""
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	// The last block of the previous turn is inside this chain; the owner map
	// gives the candidate session in O(chain) instead of scanning sessions.
	for i := len(chain) - 1; i >= 0; i-- {
		id, ok := t.owner[chain[i]]
		if !ok {
			continue
		}
		s := t.sessions[id]
		if s == nil || len(s.Chain) == 0 || len(s.Chain) > len(chain) {
			continue
		}
		if equalPrefix(s.Chain, chain) {
			return id
		}
	}
	return ""
}

func equalPrefix(short, long []uint64) bool {
	for i, h := range short {
		if long[i] != h {
			return false
		}
	}
	return true
}

// Expire drops sessions idle for longer than maxIdle and their ownership.
func (t *Tracker) Expire(maxIdle time.Duration) {
	now := t.now()
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, s := range t.sessions {
		if s.InFlight || now.Sub(s.LastSeen) < maxIdle {
			continue
		}
		for _, h := range s.Chain {
			if t.owner[h] == id {
				delete(t.owner, h)
			}
		}
		delete(t.sessions, id)
	}
}

// Stats is a small summary for the admin API.
type Stats struct {
	TrackedBlocks  int `json:"tracked_blocks"`
	ActiveSessions int `json:"active_sessions"`
}

func (t *Tracker) Stats() Stats {
	t.mu.Lock()
	defer t.mu.Unlock()
	return Stats{TrackedBlocks: len(t.blocks), ActiveSessions: len(t.sessions)}
}
