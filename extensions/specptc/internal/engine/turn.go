package engine

import (
	"context"
	"sync"
	"sync/atomic"
)

type entryState uint8

const (
	entryPending entryState = iota
	entryRunning
	entryReady
	entryFailed
	entryEvicted
)

type entry struct {
	ctx    context.Context
	cancel context.CancelFunc
	call   Call
	key    Key

	started   chan struct{}
	startOnce sync.Once
	handle    Handle
	startErr  error

	// The fields below are actor-owned.
	state            entryState
	order            uint64
	refs             int
	claims           int
	successfulClaims int
	callIDs          map[string]struct{}
	callOrder        []string
	reservedCallIDs  map[string]struct{}
	retractedCallIDs map[string]struct{}
	admitted         bool
	budgetReleased   bool
	budgetBytes      int64
}

func (e *entry) finishStart(handle Handle, err error) {
	e.startOnce.Do(func() {
		e.handle = handle
		e.startErr = err
		close(e.started)
	})
}

type observeMessage struct {
	seq  uint64
	call Call
	skip bool
}
type barrierMessage struct {
	seq   uint64
	reply chan<- bool
}
type barrierWaiter struct {
	seq   uint64
	reply chan<- bool
}
type claimSelection struct {
	entry  *entry
	callID string
}

type claimMessage struct {
	key    Key
	callID string
	reply  chan<- claimSelection
}
type retractMessage struct{ callID string }
type claimOutcomeMessage struct {
	entry    *entry
	callID   string
	hit      bool
	rollback bool
	applied  chan struct{}
}
type startResultMessage struct {
	entry       *entry
	handle      Handle
	err         error
	budgetBytes int64
	admitted    bool
	applied     chan struct{}
}
type completeMessage struct {
	handle     Handle
	completion Completion
}
type endMessage struct{ reply chan<- Metrics }

type turn struct {
	engine *Engine
	scope  Scope
	ctx    context.Context
	cancel context.CancelFunc
	events chan any
	done   chan struct{}

	byCall         map[string]*entry
	reservedByCall map[string]*entry
	queues         map[Key][]*entry
	handles        map[Handle]*entry
	completions    map[Handle]Completion
	entries        map[*entry]struct{}
	pending        map[uint64]observeMessage
	barriers       []barrierWaiter
	lastSeq        uint64
	firstDropped   atomic.Uint64
	nextOrder      uint64
	metrics        Metrics
}

func newTurn(engine *Engine, parent context.Context, scope Scope) *turn {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	return &turn{
		engine:         engine,
		scope:          scope,
		ctx:            ctx,
		cancel:         cancel,
		events:         make(chan any, engine.cfg.TurnQueueDepth),
		done:           make(chan struct{}),
		byCall:         make(map[string]*entry),
		reservedByCall: make(map[string]*entry),
		queues:         make(map[Key][]*entry),
		handles:        make(map[Handle]*entry),
		completions:    make(map[Handle]Completion),
		entries:        make(map[*entry]struct{}),
		pending:        make(map[uint64]observeMessage),
	}
}

func (t *turn) run() {
	defer close(t.done)
	for {
		select {
		case <-t.engine.ctx.Done():
			t.finish(nil)
			return
		case <-t.ctx.Done():
			t.finish(nil)
			return
		case message := <-t.events:
			switch msg := message.(type) {
			case observeMessage:
				t.observeSequenced(msg)
			case barrierMessage:
				t.barrier(msg)
			case claimMessage:
				msg.reply <- t.selectClaim(msg.key, msg.callID)
			case retractMessage:
				if candidate := t.byCall[msg.callID]; candidate != nil {
					t.detachCall(candidate, msg.callID, true)
				} else if candidate := t.reservedByCall[msg.callID]; candidate != nil {
					candidate.retractedCallIDs[msg.callID] = struct{}{}
				}
			case claimOutcomeMessage:
				t.claimOutcome(msg)
			case startResultMessage:
				t.startResult(msg)
			case completeMessage:
				t.complete(msg)
			case endMessage:
				t.finish(msg.reply)
				return
			}
		}
	}
}

func (t *turn) observeSequenced(msg observeMessage) {
	if msg.seq == 0 {
		t.observe(msg.call)
		return
	}
	if dropped := t.firstDropped.Load(); dropped != 0 && msg.seq >= dropped {
		for seq := range t.pending {
			if seq >= dropped {
				delete(t.pending, seq)
			}
		}
		t.releaseBarriers()
		return
	}
	if msg.seq <= t.lastSeq {
		return
	}
	t.pending[msg.seq] = msg
	for {
		next := t.lastSeq + 1
		observation, ok := t.pending[next]
		if !ok {
			break
		}
		delete(t.pending, next)
		if !observation.skip {
			t.observe(observation.call)
		}
		t.lastSeq = next
	}
	t.releaseBarriers()
}

func (t *turn) markDropped(seq uint64) {
	if seq == 0 {
		return
	}
	for {
		prior := t.firstDropped.Load()
		if prior != 0 && prior <= seq {
			return
		}
		if t.firstDropped.CompareAndSwap(prior, seq) {
			return
		}
	}
}

func (t *turn) droppedThrough(seq uint64) bool {
	dropped := t.firstDropped.Load()
	return dropped != 0 && dropped <= seq
}

func (t *turn) barrier(msg barrierMessage) {
	if t.droppedThrough(msg.seq) {
		msg.reply <- false
		return
	}
	if t.lastSeq >= msg.seq {
		msg.reply <- true
		return
	}
	t.barriers = append(t.barriers, barrierWaiter{seq: msg.seq, reply: msg.reply})
}

func (t *turn) releaseBarriers() {
	kept := t.barriers[:0]
	for _, waiter := range t.barriers {
		if t.droppedThrough(waiter.seq) {
			waiter.reply <- false
		} else if t.lastSeq >= waiter.seq {
			waiter.reply <- true
		} else {
			kept = append(kept, waiter)
		}
	}
	t.barriers = kept
}

func (t *turn) observe(call Call) {
	if call.CallID == "" || call.Tool == "" {
		t.metrics.Rejected++
		return
	}
	key, err := CanonicalKey(call.Tool, call.Arguments)
	if err != nil {
		t.metrics.Rejected++
		return
	}
	if prior := t.byCall[call.CallID]; prior != nil {
		if prior.key == key && prior.call.Deterministic == call.Deterministic {
			return
		}
		t.detachCall(prior, call.CallID, true)
	}
	if int(t.metrics.Dispatched) >= t.engine.cfg.MaxDispatchesPerTurn {
		t.metrics.Rejected++
		return
	}
	if call.Deterministic {
		if existing := t.reusable(key); existing != nil {
			existing.refs++
			existing.callIDs[call.CallID] = struct{}{}
			existing.callOrder = append(existing.callOrder, call.CallID)
			t.byCall[call.CallID] = existing
			return
		}
	}
	entryCtx, cancel := context.WithCancel(t.ctx)
	t.nextOrder++
	candidate := &entry{
		ctx:              entryCtx,
		cancel:           cancel,
		call:             call,
		key:              key,
		started:          make(chan struct{}),
		state:            entryPending,
		order:            t.nextOrder,
		refs:             1,
		callIDs:          map[string]struct{}{call.CallID: {}},
		callOrder:        []string{call.CallID},
		reservedCallIDs:  make(map[string]struct{}),
		retractedCallIDs: make(map[string]struct{}),
	}
	t.entries[candidate] = struct{}{}
	t.byCall[call.CallID] = candidate
	t.queues[key] = append(t.queues[key], candidate)
	if !t.engine.queueStart(t, candidate) {
		t.metrics.Rejected++
		t.evict(candidate, false)
		return
	}
	t.metrics.Dispatched++
}

func (t *turn) reusable(key Key) *entry {
	for _, candidate := range t.queues[key] {
		if candidate.call.Deterministic && candidate.state != entryFailed && candidate.state != entryEvicted {
			return candidate
		}
	}
	return nil
}

func (t *turn) selectClaim(key Key, callID string) claimSelection {
	if callID != "" {
		if t.reservedByCall[callID] != nil {
			t.metrics.Misses++
			return claimSelection{}
		}
		candidate := t.byCall[callID]
		if candidate == nil || candidate.key != key || candidate.state == entryFailed ||
			candidate.state == entryEvicted || candidate.claims >= candidate.refs {
			t.metrics.Misses++
			return claimSelection{}
		}
		return t.reserveClaim(candidate, callID)
	}
	queue := t.queues[key]
	for _, candidate := range queue {
		if candidate.state == entryFailed || candidate.state == entryEvicted || candidate.claims >= candidate.refs {
			continue
		}
		for _, id := range candidate.callOrder {
			if t.byCall[id] == candidate && t.reservedByCall[id] == nil {
				return t.reserveClaim(candidate, id)
			}
		}
	}
	t.metrics.Misses++
	return claimSelection{}
}

func (t *turn) reserveClaim(candidate *entry, callID string) claimSelection {
	candidate.claims++
	delete(t.byCall, callID)
	t.reservedByCall[callID] = candidate
	candidate.reservedCallIDs[callID] = struct{}{}
	if !candidate.call.Deterministic || candidate.claims >= candidate.refs {
		t.removeFromQueue(candidate)
	}
	return claimSelection{entry: candidate, callID: callID}
}

func (t *turn) claimOutcome(msg claimOutcomeMessage) {
	if msg.applied != nil {
		defer close(msg.applied)
	}
	candidate := msg.entry
	if !msg.rollback {
		if t.reservedByCall[msg.callID] == candidate {
			delete(t.reservedByCall, msg.callID)
		}
		delete(candidate.reservedCallIDs, msg.callID)
		delete(candidate.retractedCallIDs, msg.callID)
	}
	if _, exists := t.entries[candidate]; !exists {
		if msg.rollback {
			if t.reservedByCall[msg.callID] == candidate {
				delete(t.reservedByCall, msg.callID)
			}
			delete(candidate.reservedCallIDs, msg.callID)
			delete(candidate.retractedCallIDs, msg.callID)
		}
		if !msg.rollback {
			if msg.hit {
				t.metrics.Hits++
			} else {
				t.metrics.Misses++
			}
		}
		return
	}
	if msg.rollback {
		t.rollbackClaim(candidate, msg.callID)
		return
	}
	if msg.hit {
		candidate.successfulClaims++
		t.metrics.Hits++
		return
	}
	t.metrics.Misses++
	if !candidate.call.Deterministic || candidate.successfulClaims == 0 && candidate.claims >= candidate.refs {
		t.evict(candidate, false)
	}
}

func (t *turn) rollbackClaim(candidate *entry, callID string) {
	if candidate.claims > candidate.successfulClaims {
		candidate.claims--
	}
	if t.reservedByCall[callID] == candidate {
		delete(t.reservedByCall, callID)
	}
	delete(candidate.reservedCallIDs, callID)
	_, retracted := candidate.retractedCallIDs[callID]
	delete(candidate.retractedCallIDs, callID)
	current := t.byCall[callID]
	displaced := current != nil && current != candidate
	replacedOnSameEntry := current == candidate
	if retracted || displaced {
		if !replacedOnSameEntry {
			delete(candidate.callIDs, callID)
		}
		if candidate.refs > 0 {
			candidate.refs--
		}
	} else if _, exists := candidate.callIDs[callID]; exists && t.byCall[callID] == nil {
		t.byCall[callID] = candidate
	}
	if candidate.refs <= candidate.claims && candidate.successfulClaims == 0 {
		t.evict(candidate, retracted)
		return
	}
	queue := t.queues[candidate.key]
	for _, queued := range queue {
		if queued == candidate {
			return
		}
	}
	insertAt := len(queue)
	for i, queued := range queue {
		if queued.order > candidate.order {
			insertAt = i
			break
		}
	}
	queue = append(queue, nil)
	copy(queue[insertAt+1:], queue[insertAt:])
	queue[insertAt] = candidate
	t.queues[candidate.key] = queue
}

func (t *turn) startResult(msg startResultMessage) {
	if msg.applied != nil {
		defer close(msg.applied)
	}
	candidate := msg.entry
	candidate.finishStart(msg.handle, msg.err)
	candidate.admitted = msg.admitted
	candidate.budgetBytes = msg.budgetBytes
	if _, exists := t.entries[candidate]; !exists || candidate.state == entryEvicted {
		if msg.handle != "" {
			t.engine.queueCancel(msg.handle)
		}
		t.releaseBudget(candidate)
		return
	}
	if msg.err != nil || msg.handle == "" {
		candidate.state = entryFailed
		t.metrics.StartFailures++
		t.releaseBudget(candidate)
		t.remove(candidate)
		return
	}
	candidate.state = entryRunning
	t.handles[msg.handle] = candidate
	if completion, ok := t.completions[msg.handle]; ok {
		delete(t.completions, msg.handle)
		t.complete(completeMessage{handle: msg.handle, completion: completion})
	}
}

func (t *turn) complete(msg completeMessage) {
	candidate := t.handles[msg.handle]
	if candidate == nil {
		if len(t.completions) < t.engine.cfg.TurnQueueDepth {
			t.completions[msg.handle] = msg.completion
		}
		return
	}
	t.releaseBudget(candidate)
	if msg.completion == CompletionReady {
		candidate.state = entryReady
		return
	}
	candidate.state = entryFailed
	delete(t.handles, msg.handle)
	t.remove(candidate)
}

func (t *turn) detachCall(candidate *entry, callID string, countEviction bool) {
	delete(t.byCall, callID)
	delete(candidate.callIDs, callID)
	if candidate.refs > 0 {
		candidate.refs--
	}
	if candidate.refs <= candidate.claims && candidate.claims == candidate.successfulClaims && candidate.successfulClaims == 0 {
		t.evict(candidate, countEviction)
	}
}

func (t *turn) evict(candidate *entry, count bool) {
	if candidate == nil || candidate.state == entryEvicted {
		return
	}
	if candidate.state == entryReady && candidate.successfulClaims == 0 {
		t.metrics.Wasted++
	}
	candidate.state = entryEvicted
	candidate.cancel()
	candidate.finishStart("", context.Canceled)
	if count {
		t.metrics.Evictions++
	}
	if candidate.handle != "" && candidate.successfulClaims == 0 {
		t.engine.queueCancel(candidate.handle)
		t.metrics.Cancelled++
	}
	t.releaseBudget(candidate)
	t.remove(candidate)
}

func (t *turn) removeFromQueue(candidate *entry) {
	queue := t.queues[candidate.key]
	kept := queue[:0]
	for _, item := range queue {
		if item != candidate {
			kept = append(kept, item)
		}
	}
	if len(kept) == 0 {
		delete(t.queues, candidate.key)
	} else {
		t.queues[candidate.key] = kept
	}
}

func (t *turn) remove(candidate *entry) {
	delete(t.entries, candidate)
	if candidate.handle != "" {
		delete(t.handles, candidate.handle)
	}
	for callID := range candidate.callIDs {
		if t.byCall[callID] == candidate {
			delete(t.byCall, callID)
		}
		if t.reservedByCall[callID] == candidate {
			delete(t.reservedByCall, callID)
		}
	}
	t.removeFromQueue(candidate)
}

func (t *turn) releaseBudget(candidate *entry) {
	if candidate.admitted && !candidate.budgetReleased {
		candidate.budgetReleased = true
		t.engine.budget.release(candidate.budgetBytes)
	}
}

func (t *turn) finish(reply chan<- Metrics) {
	for candidate := range t.entries {
		if candidate.successfulClaims == 0 {
			t.evict(candidate, true)
		} else {
			candidate.cancel()
			candidate.finishStart("", context.Canceled)
			t.releaseBudget(candidate)
			t.remove(candidate)
		}
	}
	for _, waiter := range t.barriers {
		waiter.reply <- false
	}
	t.barriers = nil
	t.cancel()
	if reply != nil {
		reply <- t.metrics
	}
}

func (t *turn) trySend(message any) bool {
	select {
	case <-t.done:
		return false
	case t.events <- message:
		return true
	default:
		return false
	}
}

func (t *turn) send(ctx context.Context, message any) bool {
	select {
	case <-ctx.Done():
		return false
	case <-t.done:
		return false
	case t.events <- message:
		return true
	}
}

func (t *turn) sendInternal(message any) bool {
	select {
	case <-t.done:
		return false
	case <-t.engine.ctx.Done():
		return false
	case t.events <- message:
		return true
	}
}
