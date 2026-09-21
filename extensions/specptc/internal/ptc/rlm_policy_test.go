package ptc

import "testing"

func TestPlannerResumesDependentCallFromCompletedSpeculation(t *testing.T) {
	source := `
		first := QueryRaw("draft")
		judge := QueryRaw("judge: " + first)
		_, _ = first, judge
	`
	planner := Planner{Tools: RLMToolPolicies("")}
	initial := planner.Plan(source, nil)
	if len(initial) != 1 {
		t.Fatalf("initial plans = %+v, want only independent draft", initial)
	}
	planner.Results = map[string]any{initial[0].ID: "candidate"}
	resumed := planner.Plan(source, nil)
	assertPrompts(t, resumed, "draft", "judge: candidate")
}

func TestRLMToolPoliciesMatchRealPromptConstruction(t *testing.T) {
	planner := Planner{Tools: RLMToolPolicies("large context")}
	plans := planner.Plan(`
		a := Query("summarize")
		b := QueryRaw("raw prompt")
		c := QueryWith("slice", "focused")
		_, _, _ = a, b, c
	`, nil)
	assertPrompts(t, plans,
		RLMQueryPrompt("large context", "summarize"),
		"raw prompt",
		RLMQueryPrompt("slice", "focused"),
	)
	for _, plan := range plans {
		if plan.Name != "Query" {
			t.Fatalf("executor name = %q, want Query", plan.Name)
		}
	}
}

func TestRLMToolPoliciesExpandSyncAndAsyncBatches(t *testing.T) {
	planner := Planner{Tools: RLMToolPolicies("ctx")}
	plans := planner.Plan(`
		one := QueryBatchedRaw([]string{"a", "b"})
		two := QueryBatchedAsync([]string{"c", "d"})
		_, _ = one, two
	`, nil)
	assertPrompts(t, plans,
		"a", "b", RLMQueryPrompt("ctx", "c"), RLMQueryPrompt("ctx", "d"),
	)
}
