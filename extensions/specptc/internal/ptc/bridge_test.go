package ptc

import (
	"reflect"
	"sort"
	"testing"
)

type recordingSink struct {
	plans       map[string]CallPlan
	upserts     []string
	retractions []string
}

func newRecordingSink() *recordingSink {
	return &recordingSink{plans: make(map[string]CallPlan)}
}

func (s *recordingSink) Upsert(plan CallPlan) {
	s.plans[plan.ID] = plan
	s.upserts = append(s.upserts, plan.ID)
}

func (s *recordingSink) Retract(id string) {
	delete(s.plans, id)
	s.retractions = append(s.retractions, id)
}

func TestStreamBridgeCarriesShadowNamespaceAcrossBlocks(t *testing.T) {
	sink := newRecordingSink()
	handler := NewStreamBridge(queryPlanner(), nil, sink).Handler(nil)
	stream := "```go\nprefix := \"topic:\"\n```\ntext\n```go\nQuery(prefix + \"x\")\n"
	if err := handler(stream, false); err != nil {
		t.Fatal(err)
	}
	if len(sink.plans) != 1 {
		t.Fatalf("plans = %#v", sink.plans)
	}
	for _, plan := range sink.plans {
		if got := plan.Values[0]; got != "topic:x" {
			t.Fatalf("prompt = %#v", got)
		}
	}
}

func TestStreamBridgeResumesDependentPlanWhenResultArrives(t *testing.T) {
	sink := newRecordingSink()
	bridge := NewStreamBridge(queryPlanner(), nil, sink)
	handler := bridge.Handler(nil)
	stream := "```go\nfirst := Query(\"draft\")\njudge := Query(\"judge: \"+first)\n"
	if err := handler(stream, false); err != nil {
		t.Fatal(err)
	}
	if len(sink.plans) != 1 {
		t.Fatalf("initial plans = %#v", sink.plans)
	}
	var firstID string
	for firstID = range sink.plans {
	}
	bridge.Resolve(firstID, sink.plans[firstID].Arguments, "candidate")
	if len(sink.plans) != 2 {
		t.Fatalf("resumed plans = %#v", sink.plans)
	}
	var prompts []string
	for _, plan := range sink.plans {
		prompts = append(prompts, plan.Values[0].(string))
	}
	sort.Strings(prompts)
	if !reflect.DeepEqual(prompts, []string{"draft", "judge: candidate"}) {
		t.Fatalf("prompts = %#v", prompts)
	}
}

func TestStreamBridgeDispatchesLoopBeforeCodeBlockCloses(t *testing.T) {
	sink := newRecordingSink()
	bridge := NewStreamBridge(queryPlanner(), nil, sink)
	var forwarded []string
	handler := bridge.Handler(func(chunk string, _ bool) error {
		forwarded = append(forwarded, chunk)
		return nil
	})
	lines := []string{
		"```repl\n",
		"chunks := []string{\"a1\", \"b2\", \"c3\", \"d4\"}\n",
		"results := []string{}\n",
		"for _, chunk := range chunks {\n",
		" results = append(results, Query(\"sum: \"+chunk))\n",
	}
	for _, line := range lines {
		if err := handler(line, false); err != nil {
			t.Fatal(err)
		}
	}
	if len(sink.plans) != 4 {
		t.Fatalf("plans before block close = %d, want 4: %#v", len(sink.plans), sink.plans)
	}
	if !reflect.DeepEqual(forwarded, lines) {
		t.Fatalf("forwarded chunks = %#v, want %#v", forwarded, lines)
	}
}

func TestStreamBridgeRetractsLoopOvershootWhenBreakArrives(t *testing.T) {
	sink := newRecordingSink()
	handler := NewStreamBridge(queryPlanner(), nil, sink).Handler(nil)
	prefix := "```go\nchunks := []string{\"a\", \"b\", \"c\"}\nfor _, chunk := range chunks {\n Query(chunk)\n"
	if err := handler(prefix, false); err != nil {
		t.Fatal(err)
	}
	if len(sink.plans) != 3 {
		t.Fatalf("initial plans = %d, want 3", len(sink.plans))
	}
	if err := handler(" break\n", false); err != nil {
		t.Fatal(err)
	}
	if len(sink.plans) != 1 {
		t.Fatalf("plans after break = %d, want 1: %#v", len(sink.plans), sink.plans)
	}
	if len(sink.retractions) != 2 {
		t.Fatalf("retractions = %#v, want 2", sink.retractions)
	}
}

func TestStreamBridgePreservesBetsAcrossTemporarilyInvalidSuffix(t *testing.T) {
	sink := newRecordingSink()
	handler := NewStreamBridge(queryPlanner(), nil, sink).Handler(nil)
	if err := handler("```go\nQuery(\"stable\")\n", false); err != nil {
		t.Fatal(err)
	}
	if len(sink.plans) != 1 {
		t.Fatalf("initial plans = %#v", sink.plans)
	}
	if err := handler("if (\n", false); err != nil {
		t.Fatal(err)
	}
	if len(sink.plans) != 1 || len(sink.retractions) != 0 {
		t.Fatalf("temporary syntax retracted bets: plans=%#v retractions=%#v", sink.plans, sink.retractions)
	}
}

func TestStreamBridgeSeparatesMultipleCodeBlocks(t *testing.T) {
	sink := newRecordingSink()
	handler := NewStreamBridge(queryPlanner(), nil, sink).Handler(nil)
	stream := "```go\nQuery(\"one\")\n```\ntext\n```repl\nQuery(\"two\")\n```\n"
	if err := handler(stream, true); err != nil {
		t.Fatal(err)
	}
	if len(sink.plans) != 2 {
		t.Fatalf("plans = %#v, want two blocks", sink.plans)
	}
	var prompts []string
	for _, plan := range sink.plans {
		prompts = append(prompts, plan.Values[0].(string))
	}
	sort.Strings(prompts)
	if !reflect.DeepEqual(prompts, []string{"one", "two"}) {
		t.Fatalf("prompts = %#v", prompts)
	}
}
