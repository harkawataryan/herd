package scheduler

import (
	"math"
	"sync"
	"time"

	"github.com/harkawataryan/herd/internal/backend"
	"github.com/harkawataryan/herd/internal/value"
)

// Pack is Herd's cache-packing policy. Prompts that share a leading block
// (a system prompt, a tool list, the retrieved context of one app) form a
// family, and the policy keeps each family on as few replicas as its load
// allows, so the same shared prefix is not cached eight times over while
// conversations get evicted for lack of room.
//
// A family has an ordered list of member replicas. Requests go to the
// earliest member that holds the prefix and is under the load cap, so the
// later copies fall out of use and out of the replicas' caches on their own.
// Only when every member is over the cap does the family expand, and the
// new copy goes to the replica where it evicts the least valuable blocks
// (recent request frequency, plus the chance a conversation is coming back
// for them). A conversation that is mid-flight stays on the replica that
// served its last turn. Requests that hit nowhere are placed the same
// cheapest-eviction way.
type Pack struct {
	T Thresholds
	V *value.Tracker

	mu       sync.Mutex
	families map[uint64]*family
	now      func() time.Time
}

type family struct {
	members  []string // replica names, oldest first
	lastUsed time.Time
}

func NewPack(t Thresholds, v *value.Tracker) *Pack {
	return &Pack{T: t, V: v, families: map[uint64]*family{}, now: time.Now}
}

func (*Pack) Name() string { return "pack" }

type packOption struct {
	r    *backend.Replica
	hit  int
	wait float64 // expected wait before prefill, in prefill blocks
	loss float64 // expected future prefill caused by what this placement evicts
	cost float64 // blocks to prefill now + wait + EvictWeight × loss
}

func (p *Pack) Pick(req Request, cands []*backend.Replica) (Decision, bool) {
	if len(cands) == 0 {
		return Decision{}, false
	}
	sc := scores(cands, p.T)
	half := p.V.HalfLife()
	need := float64(req.PromptTokens + req.MaxTokens)
	gap := p.V.Gap(req.SessionID)

	opts := make(map[string]*packOption, len(cands))
	var best, plain *packOption // plain: cheapest ignoring evictions, what prefix+wait routing alone would pick
	for _, r := range cands {
		st := r.State()
		hit := r.Prefix.Peek(req.Chain)
		ev := r.Prefix.Evictions(len(req.Chain)-hit, req.Chain)
		horizon, turnover := 1.0, math.Inf(1)
		if rate := r.Prefix.InsertRate(half); rate > 0 {
			turnover = float64(r.Prefix.Cap()) / rate
			horizon = min(1, turnover/half.Seconds())
		}
		o := &packOption{r: r, hit: hit, loss: p.V.Sum(ev, horizon)}
		risk := 0.0
		if gap > 0 && !math.IsInf(turnover, 1) {
			risk = math.Exp(-turnover / gap)
		}
		o.loss += p.V.Own(req.Chain, hit, req.SessionID) * risk
		o.wait = WaitBlocks(st, r.KVCapacity, need, p.T.PrefillTPS)
		o.cost = float64(len(req.Chain)-hit) + o.wait + p.T.EvictWeight*o.loss
		opts[r.Name] = o
		if best == nil || o.cost < best.cost || (o.cost == best.cost && sc[r.Name] < sc[best.r.Name]) {
			best = o
		}
		now := o.cost - p.T.EvictWeight*o.loss
		if plain == nil || now < plain.cost-p.T.EvictWeight*plain.loss || (now == plain.cost-p.T.EvictWeight*plain.loss && sc[r.Name] < sc[plain.r.Name]) {
			plain = o
		}
	}
	decide := func(o *packOption, reason string) (Decision, bool) {
		d := Decision{Replica: o.r, Reason: reason, Scores: sc, PrefixBlocks: o.hit, EvictCost: o.loss, Protected: plain.loss - o.loss,
			Costs: map[string]float64{}, Waits: map[string]float64{}, Inflight: map[string]int{}}
		for name, c := range opts {
			d.Costs[name], d.Waits[name], d.Inflight[name] = c.cost, c.wait, c.r.State().Inflight
		}
		return d, true
	}
	limit := best.cost + p.T.PackSlack

	// 1. A conversation stays where its last turn ran, if that replica still
	// holds any of it and the cost of staying is within the slack.
	if req.SessionID != "" {
		if s := p.V.Lookup(req.SessionID); s != nil {
			if o := opts[s.Replica]; o != nil && o.hit > 0 && o.cost <= limit {
				return decide(o, "session_home")
			}
		}
	}

	// 2. The family's members, oldest first: the first one that still holds
	// the prefix and is within the slack. Later copies that go unused fall
	// out of their replicas' caches and are dropped here when that happens.
	var key uint64
	if len(req.Chain) > 0 {
		key = req.Chain[0]
		p.mu.Lock()
		if f := p.families[key]; f != nil {
			kept := f.members[:0]
			for _, name := range f.members {
				if o := opts[name]; o != nil && o.hit > 0 {
					kept = append(kept, name)
				}
			}
			f.members = kept
			for _, name := range f.members {
				if o := opts[name]; o.cost <= limit {
					f.lastUsed = p.now()
					p.mu.Unlock()
					return decide(o, "family_hit")
				}
			}
		}
		p.mu.Unlock()
	}

	// 3. Nothing to stick to: the cheapest replica overall, which becomes a
	// member of the family (an expansion if the family already had one).
	reason := "evict_cheapest"
	switch {
	case best.hit > 0:
		reason = "prefix_hit"
	case best.loss == 0:
		reason = "evict_free"
	}
	if key != 0 {
		p.mu.Lock()
		f := p.families[key]
		if f == nil {
			f = &family{}
			p.families[key] = f
		}
		if len(f.members) > 0 {
			reason = "family_expand"
		}
		f.members = append(f.members, best.r.Name)
		f.lastUsed = p.now()
		p.mu.Unlock()
	}
	return decide(best, reason)
}

// Expire forgets families unused for longer than maxIdle.
func (p *Pack) Expire(maxIdle time.Duration) {
	now := p.now()
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, f := range p.families {
		if now.Sub(f.lastUsed) > maxIdle {
			delete(p.families, k)
		}
	}
}

// Families reports how many families are tracked and how many replicas
// they occupy in total (duplication = occupancy / families).
func (p *Pack) Families() (families, occupancy int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, f := range p.families {
		families++
		occupancy += len(f.members)
	}
	return
}
