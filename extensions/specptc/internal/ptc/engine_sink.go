package ptc

import "github.com/esengine/DeepSeek-Reasonix/extensions/specptc/internal/engine"

// EngineSink connects streamed RLM call plans to the extension's bounded,
// turn-scoped speculation scheduler.
type EngineSink struct {
	Engine  *engine.Engine
	Backend *QueryBackend
	Scope   engine.Scope
}

// Upsert starts or replaces one predicted call. Saturation and stale turns fail
// open; the real REPL will execute the call normally on a later claim miss.
func (s EngineSink) Upsert(plan CallPlan) {
	if s.Engine == nil {
		return
	}
	s.Engine.Observe(s.Scope, engine.Call{
		CallID: plan.ID, Tool: plan.Name, Arguments: plan.Arguments,
		Deterministic: plan.Deterministic,
	})
}

// Retract cancels a prediction invalidated by later streamed code.
func (s EngineSink) Retract(id string) {
	if s.Backend != nil {
		s.Backend.SuppressCall(s.Scope, id)
	}
	if s.Engine != nil {
		s.Engine.Retract(s.Scope, id)
	}
}
