package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	extension "github.com/esengine/DeepSeek-Reasonix/sdk/go"

	"github.com/esengine/DeepSeek-Reasonix/extensions/specptc/internal/engine"
	"github.com/esengine/DeepSeek-Reasonix/extensions/specptc/internal/ptc"
)

func TestUITraceRendersSpeculationAndAuthoritativeLanes(t *testing.T) {
	state := uiTraceState{cacheByID: make(map[string]*uiCallRow), actualByID: make(map[string]*uiCallRow)}
	state.apply(ptc.QueryTraceEvent{
		Kind: ptc.QueryTraceDispatch, ID: "q1", Name: "Query", Preview: "\"find the answer\"", Speculative: true,
	})
	running := state.markdown(time.Now().Add(time.Second))
	for _, want := range []string{"speculation cache", "actually running", "speculating", "Query(\"find the answer\")"} {
		if !strings.Contains(running, want) {
			t.Fatalf("running markdown missing %q:\n%s", want, running)
		}
	}

	state.apply(ptc.QueryTraceEvent{
		Kind: ptc.QueryTraceClaimHit, ID: "q1", Name: "Query", Preview: "\"find the answer\"",
		Speculative: true, Hit: true, HeadStart: 900 * time.Millisecond,
	})
	// Completion can race with the authoritative waiter after the claim. A late
	// ready event must not regress the claimed row back to "cached".
	state.apply(ptc.QueryTraceEvent{
		Kind: ptc.QueryTraceReady, ID: "q1", Name: "Query", Preview: "\"find the answer\"",
		Speculative: true, Duration: 1200 * time.Millisecond,
	})
	claimed := state.markdown(time.Now())
	if strings.Count(claimed, "cache hit") != 2 || !strings.Contains(claimed, "+0.9s head start") {
		t.Fatalf("claim must appear in cache and actually-running lanes:\n%s", claimed)
	}
	state.apply(ptc.QueryTraceEvent{
		Kind: ptc.QueryTraceDone, ID: "q1", Name: "Query", Preview: "\"find the answer\"",
		Speculative: true, Hit: true, Duration: 1200 * time.Millisecond,
		Wait: 300 * time.Millisecond, Saved: 900 * time.Millisecond,
	})
	hit := state.markdown(time.Now())
	for _, want := range []string{"cache hit", "took 1.2s", "waited 0.3s", "+0.9s saved"} {
		if !strings.Contains(hit, want) {
			t.Fatalf("hit markdown missing %q:\n%s", want, hit)
		}
	}

	state.apply(ptc.QueryTraceEvent{
		Kind: ptc.QueryTraceClaimMiss, ID: "serial-1", Name: "Query", Preview: "\"fallback\"",
	})
	miss := state.markdown(time.Now().Add(time.Second))
	if !strings.Contains(miss, "cache miss · running inline") {
		t.Fatalf("miss markdown:\n%s", miss)
	}
	state.apply(ptc.QueryTraceEvent{
		Kind: ptc.QueryTraceFailed, ID: "serial-1", Name: "Query", Preview: "\"fallback\"",
		Duration: time.Second, Wait: 1200 * time.Millisecond,
	})
	failed := state.markdown(time.Now())
	if !strings.Contains(failed, "failed · after 1.2s") || strings.Contains(failed, "fallback\") — inline") {
		t.Fatalf("failed authoritative call rendered as successful:\n%s", failed)
	}
}

func TestUITraceFinalizesRunningRows(t *testing.T) {
	state := uiTraceState{cacheByID: make(map[string]*uiCallRow), actualByID: make(map[string]*uiCallRow)}
	state.apply(ptc.QueryTraceEvent{Kind: ptc.QueryTraceDispatch, ID: "q1", Name: "Query"})
	state.apply(ptc.QueryTraceEvent{Kind: ptc.QueryTraceClaimHit, ID: "q1", Name: "Query", Hit: true})
	state.apply(ptc.QueryTraceEvent{Kind: ptc.QueryTraceClaimMiss, ID: "q2", Name: "Query"})
	state.finalizeRunning()
	if state.running() {
		t.Fatalf("final state still has running rows:\n%s", state.markdown(time.Now()))
	}
	rendered := state.markdown(time.Now())
	if strings.Count(rendered, "failed") < 2 {
		t.Fatalf("final state did not mark interrupted calls failed:\n%s", rendered)
	}
}

func TestUITraceFinalSnapshotSurvivesCancelledStreamContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	trace := &rlmUITrace{
		ctx:     ctx,
		binding: providerUIBinding{sessionID: "session", generation: 1, host: extension.UIHostTUI},
		state:   uiTraceState{cacheByID: make(map[string]*uiCallRow), actualByID: make(map[string]*uiCallRow)},
	}
	trace.state.apply(ptc.QueryTraceEvent{Kind: ptc.QueryTraceDispatch, ID: "q1", Name: "Query"})
	metrics := engine.Metrics{Cancelled: 1}
	trace.publishFinal(&metrics)
	if trace.state.running() || trace.state.metrics == nil || trace.state.metrics.Cancelled != 1 {
		t.Fatalf("cancelled final state = %+v", trace.state)
	}
}

func TestUITraceObserveCoalescesWithoutDroppingLifecycle(t *testing.T) {
	trace := &rlmUITrace{
		wake:  make(chan struct{}, 1),
		state: uiTraceState{cacheByID: make(map[string]*uiCallRow), actualByID: make(map[string]*uiCallRow)},
	}
	for i := 0; i < 600; i++ {
		id := fmt.Sprintf("q-%d", i)
		trace.observe(ptc.QueryTraceEvent{Kind: ptc.QueryTraceDispatch, ID: id, Name: "Query"})
		trace.observe(ptc.QueryTraceEvent{Kind: ptc.QueryTraceEvicted, ID: id, Name: "Query"})
	}
	trace.mu.Lock()
	defer trace.mu.Unlock()
	if len(trace.state.cache) != 600 || trace.state.running() {
		t.Fatalf("coalesced state: rows=%d running=%v", len(trace.state.cache), trace.state.running())
	}
}

func TestUITraceBoundsRenderedRows(t *testing.T) {
	state := uiTraceState{cacheByID: make(map[string]*uiCallRow), actualByID: make(map[string]*uiCallRow)}
	for i := 0; i < uiTraceRows+5; i++ {
		id := strings.Repeat("x", i+1)
		state.apply(ptc.QueryTraceEvent{Kind: ptc.QueryTraceDispatch, ID: id, Name: "Query", Preview: id})
	}
	rendered := state.markdown(time.Now())
	if !strings.Contains(rendered, "… 5 earlier calls") {
		t.Fatalf("bounded row marker missing:\n%s", rendered)
	}
}
