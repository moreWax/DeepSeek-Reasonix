package ptc

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"

	rlmgo "github.com/XiaoConstantine/rlm-go/pkg/rlm"
)

// PlanSink receives idempotent speculative candidates from a streamed RLM
// response. Upsert replaces a prior candidate with the same ID; Retract cancels
// a bet invalidated by later generated code.
type PlanSink interface {
	Upsert(CallPlan)
	Retract(id string)
}

// StreamBridge adapts rlm-go's root-model stream callback to the extension's
// statement segmenter and speculative planner. It does not execute model code;
// the sink owns bounded dispatch and the real REPL remains authoritative.
type StreamBridge struct {
	mu        sync.Mutex
	segmenter StreamSegmenter
	planner   Planner
	env       map[string]any
	sink      PlanSink
	blocks    map[uint64]map[string]CallPlan
	results   map[uint64]map[string]any
}

// NewStreamBridge creates one turn-local bridge. env must contain inert copies
// of variables visible at the beginning of the RLM turn.
func NewStreamBridge(planner Planner, env map[string]any, sink PlanSink) *StreamBridge {
	return &StreamBridge{
		planner: planner,
		env:     env,
		sink:    sink,
		blocks:  make(map[uint64]map[string]CallPlan),
		results: make(map[uint64]map[string]any),
	}
}

// Handler wraps an optional rlm-go stream handler. Speculation failures are
// deliberately fail-open and never suppress user-visible stream chunks.
func (b *StreamBridge) Handler(next rlmgo.StreamHandler) rlmgo.StreamHandler {
	return func(chunk string, done bool) error {
		b.mu.Lock()
		remaining := chunk
		for remaining != "" {
			piece := remaining
			if newline := strings.IndexByte(remaining, '\n'); newline >= 0 {
				piece = remaining[:newline+1]
				remaining = remaining[newline+1:]
			} else {
				remaining = ""
			}
			b.segmenter.Feed(piece)
			b.reconcileCurrentBlock()
		}
		if done {
			b.segmenter.Finish()
			b.reconcileCurrentBlock()
		}
		b.mu.Unlock()
		if next != nil {
			return next(chunk, done)
		}
		return nil
	}
}

// Resolve publishes a completed speculative value and immediately retries
// planning, allowing dependent calls in the still-streaming block to launch.
func (b *StreamBridge) Resolve(prefixedID string, arguments []byte, value any) {
	if b == nil {
		return
	}
	const prefix = "block:"
	rest, ok := strings.CutPrefix(prefixedID, prefix)
	if !ok {
		return
	}
	blockText, planID, ok := strings.Cut(rest, ":")
	if !ok || planID == "" {
		return
	}
	blockID, err := strconv.ParseUint(blockText, 10, 64)
	if err != nil || blockID == 0 {
		return
	}
	b.mu.Lock()
	plan, current := b.blocks[blockID][prefixedID]
	if !current || !bytes.Equal(plan.Arguments, arguments) {
		b.mu.Unlock()
		return
	}
	if b.results[blockID] == nil {
		b.results[blockID] = make(map[string]any)
	}
	b.results[blockID][planID] = cloneInert(value)
	_, currentID, _ := b.segmenter.CurrentBlock()
	if currentID == blockID {
		b.reconcileCurrentBlock()
	}
	b.mu.Unlock()
}

func (b *StreamBridge) reconcileCurrentBlock() {
	if b == nil || b.sink == nil {
		return
	}
	source, blockID, active := b.segmenter.CurrentBlock()
	if blockID == 0 || source == "" {
		return
	}
	planner := b.planner
	planner.Results = b.results[blockID]
	planned, state, valid := planner.PlanCheckedState(source, b.env)
	if !valid {
		return
	}
	if !active {
		b.env = state
	}
	next := make(map[string]CallPlan, len(planned))
	for _, plan := range planned {
		plan.ID = fmt.Sprintf("block:%d:%s", blockID, plan.ID)
		next[plan.ID] = plan
	}
	prior := b.blocks[blockID]
	for id := range prior {
		if _, retained := next[id]; !retained {
			b.sink.Retract(id)
		}
	}
	for id, plan := range next {
		if old, exists := prior[id]; !exists || !samePlan(old, plan) {
			b.sink.Upsert(plan)
		}
	}
	b.blocks[blockID] = next
}

func samePlan(left, right CallPlan) bool {
	return left.ID == right.ID && left.Name == right.Name && left.Deterministic == right.Deterministic &&
		bytes.Equal(left.Arguments, right.Arguments)
}
