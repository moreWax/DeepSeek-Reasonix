package ptc

import (
	"encoding/json"
	"reflect"
	"testing"
)

func queryPlanner() Planner {
	return Planner{Tools: map[string]ToolPolicy{
		"Query":        {},
		"QueryRaw":     {},
		"QueryBatched": {},
	}}
}

func TestPlannerStopsAfterUnresolvedOrLabeledControlFlow(t *testing.T) {
	planner := queryPlanner()
	unknown := planner.Plan(`
		if unknownCondition { return }
		Query("unreachable-or-uncertain")
	`, nil)
	if len(unknown) != 0 {
		t.Fatalf("unknown control-flow plans = %+v", unknown)
	}
	unknownRange := planner.Plan(`
		for _, item := range unknownItems { _ = item }
		Query("after-range")
	`, nil)
	if len(unknownRange) != 0 {
		t.Fatalf("unknown range plans = %+v", unknownRange)
	}
	unsupported := planner.Plan(`
		for condition { break }
		Query("after-for")
	`, nil)
	if len(unsupported) != 0 {
		t.Fatalf("unsupported flow plans = %+v", unsupported)
	}
	labeled := planner.Plan(`
	outer:
		for _, item := range []string{"a"} {
			if item == "a" { break outer }
		}
		Query("after-label")
	`, nil)
	if len(labeled) != 0 {
		t.Fatalf("labeled control-flow plans = %+v", labeled)
	}
}

func TestPlannerUsesSimultaneousAssignmentSemantics(t *testing.T) {
	plans := queryPlanner().Plan(`
		a, b := "one", "two"
		a, b = b, a
		Query(a + b)
	`, nil)
	assertPrompts(t, plans, "twoone")
}

func TestPlannerHonorsContinueAndReturnControlFlow(t *testing.T) {
	planner := queryPlanner()
	continued := planner.Plan(`
		for _, prompt := range []string{"skip", "keep"} {
			if prompt == "skip" { continue }
			Query(prompt)
		}
	`, nil)
	assertPrompts(t, continued, "keep")

	returned := planner.Plan(`
		for range []string{"one"} {
			return
			Query("inside")
		}
		Query("after")
	`, nil)
	if len(returned) != 0 {
		t.Fatalf("plans after return = %+v", returned)
	}
}

func TestPlannerRestoresLexicallyShadowedValues(t *testing.T) {
	plans, state, valid := queryPlanner().PlanCheckedState(`
		x := "outer"
		{
			x := "inner"
			Query(x)
		}
		if y := "condition"; true {
			y := "body"
			Query(y)
		}
		Query(x)
	`, nil)
	if !valid {
		t.Fatal("valid program was rejected")
	}
	assertPrompts(t, plans, "inner", "body", "outer")
	if !reflect.DeepEqual(state, map[string]any{"x": "outer"}) {
		t.Fatalf("shadow state = %#v", state)
	}
}

func TestPlannerResolvesLiteralAndLiveVariables(t *testing.T) {
	plans := queryPlanner().Plan(`
		topic := "gpu clusters"
		prefix := "Tell me about "
		first := Query(prefix + topic)
		second := Query(fmt.Sprintf("more on %s", topic))
	`, nil)
	assertPrompts(t, plans, "Tell me about gpu clusters", "more on gpu clusters")
}

func TestPlannerUnrollsOpenRangeLoop(t *testing.T) {
	tail := `
		for _, chunk := range chunks {
			results = append(results, Query("sum: "+chunk))
	`
	plans := queryPlanner().Plan(tail, map[string]any{
		"chunks":  []string{"a1", "b2", "c3", "d4"},
		"results": []string{},
	})
	assertPrompts(t, plans, "sum: a1", "sum: b2", "sum: c3", "sum: d4")
	ids := map[string]bool{}
	for _, plan := range plans {
		if ids[plan.ID] {
			t.Fatalf("duplicate plan id %q", plan.ID)
		}
		ids[plan.ID] = true
	}
}

func TestPlannerResolvesOnlyTakenBranch(t *testing.T) {
	source := `
		mode := "summarize"
		if mode == "summarize" {
			a := Query("taken: " + mode)
			_ = a
		} else {
			Query("not taken")
		}
	`
	plans := queryPlanner().Plan(source, nil)
	assertPrompts(t, plans, "taken: summarize")
}

func TestPlannerTaintSkipsDependencyAndRecoversIndependentCall(t *testing.T) {
	planner := Planner{Tools: map[string]ToolPolicy{"Query": {}}}
	source := `
		rows := FetchDB("top errors")
		a := Query("triage: " + doc)
		b := Query("explain: " + rows)
		c := Query("independent question")
		_, _, _ = a, b, c
	`
	plans := planner.Plan(source, map[string]any{"doc": "incident report"})
	assertPrompts(t, plans, "triage: incident report", "independent question")
}

func TestPlannerPreservesMultiplicityAndDeterministicPolicy(t *testing.T) {
	planner := Planner{Tools: map[string]ToolPolicy{"Query": {Deterministic: true}}}
	plans := planner.Plan(`
		for _, prompt := range []string{"same", "same", "same"} {
			Query(prompt)
		}
	`, nil)
	assertPrompts(t, plans, "same", "same", "same")
	for _, plan := range plans {
		if !plan.Deterministic {
			t.Fatalf("plan %#v lost deterministic policy", plan)
		}
	}
}

func TestPlannerExpandsBatchedCallsElementwise(t *testing.T) {
	plans := queryPlanner().Plan(`
		prompts := []string{"one", "two", "three"}
		answers := QueryBatched(prompts)
		_ = answers
	`, nil)
	assertPrompts(t, plans, "one", "two", "three")
	for _, plan := range plans {
		if plan.Name != "Query" {
			t.Fatalf("batch element name = %q, want Query", plan.Name)
		}
	}
}

func TestPlannerRejectsOversizedOrUnresolvableLoops(t *testing.T) {
	planner := queryPlanner()
	values := make([]string, defaultMaxUnroll+1)
	if plans := planner.Plan(`for _, value := range values { Query(value) }`, map[string]any{"values": values}); len(plans) != 0 {
		t.Fatalf("oversized loop plans = %#v", plans)
	}
	if plans := planner.Plan(`if unknown { Query("unsafe") }`, nil); len(plans) != 0 {
		t.Fatalf("unknown branch plans = %#v", plans)
	}
}

func TestCallPlanArgumentsAreCanonicalJSONShape(t *testing.T) {
	plans := queryPlanner().Plan(`Query("hello")`, nil)
	if len(plans) != 1 {
		t.Fatalf("plans = %#v", plans)
	}
	var payload struct {
		Args []any `json:"args"`
	}
	if err := json.Unmarshal(plans[0].Arguments, &payload); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(payload.Args, []any{"hello"}) {
		t.Fatalf("arguments = %#v", payload.Args)
	}
}

func assertPrompts(t *testing.T, plans []CallPlan, want ...string) {
	t.Helper()
	got := make([]string, len(plans))
	for i, plan := range plans {
		if len(plan.Values) != 1 {
			t.Fatalf("plan %d values = %#v", i, plan.Values)
		}
		prompt, ok := plan.Values[0].(string)
		if !ok {
			t.Fatalf("plan %d prompt = %#v", i, plan.Values[0])
		}
		got[i] = prompt
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("prompts = %#v, want %#v", got, want)
	}
}
